package com.example.orderdemo.infrastructure.fluss;

import com.example.orderdemo.domain.order.OrderEvent;
import com.example.orderdemo.domain.port.EventStore;
import com.example.orderdemo.domain.port.StoredEvent;
import com.example.orderdemo.infrastructure.trace.TraceContext;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * 基于 Fluss 日志表的事件存储。
 *
 * <p><b>这里没有实现乐观并发校验。</b> {@code expectedVersion} 只是被写进事件序号里，
 * 并没有「版本不匹配就拒绝」的语义——Fluss 没有 CAS 能力，也没有事务。
 * 两个并发命令读到同一个版本时会各自写出一份事件，日志里出现重复 seq。
 * 这是本方案的核心缺陷，由 {@code DemoController} 的并发探针复现。
 */
@Component
public class FlussEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FlussEventStore.class);
    private static final long SCAN_TIMEOUT_MILLIS = 30_000;

    private final Connection connection;
    private final Admin admin;
    private final EventCodec codec;

    public FlussEventStore(Connection connection, EventCodec codec) {
        this.connection = connection;
        this.admin = connection.getAdmin();
        this.codec = codec;
    }

    @Override
    public void append(String orderId, long expectedVersion, List<OrderEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        long seq = expectedVersion;
        try (Table table = connection.getTable(FlussTables.EVENT)) {
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<AppendResult>> futures = new ArrayList<>(events.size());
            for (OrderEvent event : events) {
                seq++;
                GenericRow row = new GenericRow(FlussTables.EV_COLUMNS);
                row.setField(FlussTables.EV_ORDER_ID, BinaryString.fromString(orderId));
                row.setField(FlussTables.EV_SEQ, seq);
                row.setField(FlussTables.EV_TYPE, BinaryString.fromString(codec.typeOf(event)));
                row.setField(FlussTables.EV_PAYLOAD, BinaryString.fromString(codec.encode(event)));
                row.setField(FlussTables.EV_OCCURRED_AT, System.currentTimeMillis());
                row.setField(FlussTables.EV_TRACE_ID, BinaryString.fromString(TraceContext.get()));
                futures.add(writer.append(row));
            }
            // flush 阻塞到服务端确认；allOf 再把每条记录各自的失败暴露出来。
            // 注意：这里没有原子性——中途失败会留下「半个聚合」。
            writer.flush();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            log.debug("appended {} events for {} (seq {}..{})", events.size(), orderId, expectedVersion + 1, seq);
        } catch (Exception e) {
            throw new IllegalStateException("追加事件失败: " + orderId, e);
        }
    }

    @Override
    public List<StoredEvent> readEvents(String orderId, long afterSeq) {
        List<StoredEvent> events = new ArrayList<>();
        scanBounded((bucket, record) -> {
            if (!orderId.equals(record.getRow().getString(FlussTables.EV_ORDER_ID).toString())) {
                return;
            }
            long seq = record.getRow().getLong(FlussTables.EV_SEQ);
            if (seq <= afterSeq) {
                return;
            }
            String type = record.getRow().getString(FlussTables.EV_TYPE).toString();
            String payload = record.getRow().getString(FlussTables.EV_PAYLOAD).toString();
            events.add(new StoredEvent(seq, codec.decodeEvent(type, payload)));
        });
        events.sort(Comparator.comparingLong(StoredEvent::seq));
        return events;
    }

    @Override
    public long totalEvents() {
        long[] count = {0};
        scanBounded((bucket, record) -> count[0]++);
        return count[0];
    }

    /**
     * 从每个桶的最早位点扫到扫描开始时的末尾位点。
     *
     * <p>这就是「按聚合读事件」的真实成本：Fluss 没有按 key 索引的日志读取，
     * 只能扫全表再按 order_id 过滤。聚合数量一大，这个读法就不可用了。
     */
    private void scanBounded(RecordConsumer consumer) {
        try (Table table = connection.getTable(FlussTables.EVENT)) {
            int buckets = table.getTableInfo().getNumBuckets();
            Map<Integer, Long> endOffsets = admin
                    .listOffsets(FlussTables.EVENT, IntStream.range(0, buckets).boxed().toList(),
                            new OffsetSpec.LatestSpec())
                    .all()
                    .get();

            LogScanner scanner = table.newScan().createLogScanner();
            try {
                long[] progress = new long[buckets];
                boolean[] finished = new boolean[buckets];
                for (int b = 0; b < buckets; b++) {
                    scanner.subscribe(b, 0L);
                    finished[b] = endOffsets.getOrDefault(b, 0L) == 0L;
                }

                long deadline = System.currentTimeMillis() + SCAN_TIMEOUT_MILLIS;
                while (!allFinished(finished) && System.currentTimeMillis() < deadline) {
                    ScanRecords records = scanner.poll(Duration.ofMillis(200));
                    for (TableBucket bucket : records.buckets()) {
                        for (ScanRecord record : records.records(bucket)) {
                            consumer.accept(bucket, record);
                            progress[bucket.getBucket()] = record.logOffset() + 1;
                        }
                    }
                    for (int b = 0; b < buckets; b++) {
                        if (!finished[b] && progress[b] >= endOffsets.getOrDefault(b, 0L)) {
                            finished[b] = true;
                        }
                    }
                }
                if (!allFinished(finished)) {
                    log.warn("扫描未在超时内完成，可能漏读部分事件: {}", FlussTables.EVENT);
                }
            } finally {
                scanner.close();
            }
        } catch (Exception e) {
            throw new IllegalStateException("扫描事件表失败", e);
        }
    }

    private static boolean allFinished(boolean[] finished) {
        for (boolean f : finished) {
            if (!f) {
                return false;
            }
        }
        return true;
    }

    @FunctionalInterface
    private interface RecordConsumer {
        void accept(TableBucket bucket, ScanRecord record);
    }
}
