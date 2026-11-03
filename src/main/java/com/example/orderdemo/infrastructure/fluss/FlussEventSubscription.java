package com.example.orderdemo.infrastructure.fluss;

import com.example.orderdemo.application.OrderViewProjector;
import com.example.orderdemo.domain.order.OrderEvent;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 领域事件订阅：独立线程消费事件表，增量维护读模型。
 *
 * <p>消费位点存在 Fluss 里，重启后从断点继续。注意「写读模型」和「提交位点」是两次独立写入，
 * 中间崩溃会导致重复消费——所以投影端必须幂等，这里靠事件序号去重。
 */
@Component
public class FlussEventSubscription implements SmartLifecycle {

    public static final String GROUP_ID = "order-view-v1";

    private static final Logger log = LoggerFactory.getLogger(FlussEventSubscription.class);

    private final Connection connection;
    private final EventCodec codec;
    private final OrderViewProjector projector;
    private final FlussOffsetStore offsetStore;

    private volatile boolean running;
    private Thread worker;
    private volatile LogScanner scanner;

    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();

    public FlussEventSubscription(
            Connection connection,
            EventCodec codec,
            OrderViewProjector projector,
            FlussOffsetStore offsetStore) {
        this.connection = connection;
        this.codec = codec;
        this.projector = projector;
        this.offsetStore = offsetStore;
    }

    @Override
    public void start() {
        running = true;
        worker = new Thread(this::loop, "order-view-subscription");
        worker.setDaemon(true);
        worker.start();
        log.info("事件订阅已启动, group={}", GROUP_ID);
    }

    @Override
    public void stop() {
        running = false;
        // 先关扫描器把阻塞中的 poll 唤醒，再等线程退出。
        // 否则连接被关闭时 LogFetcher 还在拉数据，会刷一屏 Disconnected 堆栈。
        LogScanner current = scanner;
        if (current != null) {
            try {
                current.close();
            } catch (Exception e) {
                log.debug("关闭扫描器异常", e);
            }
        }
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        try (Table table = connection.getTable(FlussTables.EVENT)) {
            int buckets = table.getTableInfo().getNumBuckets();
            LogScanner logScanner = table.newScan().createLogScanner();
            this.scanner = logScanner;
            for (int b = 0; b < buckets; b++) {
                long start = offsetStore.load(GROUP_ID, b);
                logScanner.subscribe(b, start);
                log.info("订阅 bucket {} 从位点 {}", b, start);
            }

            while (running) {
                ScanRecords records = logScanner.poll(Duration.ofMillis(300));
                Map<Integer, Long> progressed = new HashMap<>();
                for (TableBucket bucket : records.buckets()) {
                    for (ScanRecord record : records.records(bucket)) {
                        if (handle(record)) {
                            applied.incrementAndGet();
                        } else {
                            skipped.incrementAndGet();
                        }
                        progressed.put(bucket.getBucket(), record.logOffset() + 1);
                    }
                }
                // 先写读模型、后提交位点。宁可重复消费，不可丢事件。
                progressed.forEach((bucket, offset) -> offsetStore.commit(GROUP_ID, bucket, offset));
            }
        } catch (Exception e) {
            if (running) {
                log.error("事件订阅异常退出", e);
            }
        }
    }

    private boolean handle(ScanRecord record) {
        InternalRow row = record.getRow();
        long seq = row.getLong(FlussTables.EV_SEQ);
        String type = row.getString(FlussTables.EV_TYPE).toString();
        String payload = row.getString(FlussTables.EV_PAYLOAD).toString();
        OrderEvent event = codec.decodeEvent(type, payload);
        return projector.apply(seq, event);
    }

    public long appliedCount() {
        return applied.get();
    }

    public long skippedCount() {
        return skipped.get();
    }
}
