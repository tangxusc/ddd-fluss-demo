package com.example.orderdemo.application;

import com.example.orderdemo.domain.order.Order;
import com.example.orderdemo.domain.order.OrderEvent;
import com.example.orderdemo.domain.order.OrderLine;
import com.example.orderdemo.domain.order.OrderSnapshot;
import com.example.orderdemo.domain.port.EventStore;
import com.example.orderdemo.domain.port.SnapshotStore;
import com.example.orderdemo.domain.port.StoredEvent;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 缺陷探针。
 *
 * <p>这些方法存在的唯一目的，是让「这套方案哪里不行」变成可复现的实测数据，
 * 而不是一句「我觉得可能有并发问题」。
 */
@Service
public class ProbeService {

    private final OrderCommandService commandService;
    private final EventStore eventStore;
    private final SnapshotStore snapshotStore;

    public ProbeService(OrderCommandService commandService, EventStore eventStore, SnapshotStore snapshotStore) {
        this.commandService = commandService;
        this.eventStore = eventStore;
        this.snapshotStore = snapshotStore;
    }

    /**
     * 探针 A：N 个线程同时对同一订单执行「支付」。
     *
     * <p>命令语义上只有一次支付能成功。没有 CAS 的情况下，N 个线程都读到同样的版本，
     * 都会写出一个 seq 相同的 OrderPaid 事件。
     */
    public Map<String, Object> concurrentPay(String orderId, int threads) throws Exception {
        List<Supplier<String>> commands = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String paymentId = "pay-" + i;
            commands.add(() -> {
                commandService.pay(orderId, paymentId);
                return "OK:" + paymentId;
            });
        }
        List<String> results = runConcurrently(commands);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scenario", "N 个线程并发支付同一订单");
        response.put("threads", threads);
        response.put("results", results);
        response.put("succeeded", results.stream().filter(r -> r.startsWith("OK")).count());
        response.put("eventLog", seqReport(orderId));
        return response;
    }

    /**
     * 探针 B：发货和取消同时发起。
     *
     * <p>状态机里这两个命令互斥：发了货就不能取消。并发下两个命令都基于 PAID 状态判断，
     * 于是两个互斥事件都进了事件日志。
     */
    public Map<String, Object> shipCancelRace(String orderId) throws Exception {
        List<Supplier<String>> commands = List.of(
                () -> {
                    commandService.ship(orderId, "SF" + System.nanoTime());
                    return "OK:ship";
                },
                () -> {
                    commandService.cancel(orderId, "并发取消");
                    return "OK:cancel";
                });
        List<String> results = runConcurrently(commands);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scenario", "发货与取消并发");
        response.put("results", results);
        response.put("succeeded", results.stream().filter(r -> r.startsWith("OK")).count());
        response.put("eventLog", seqReport(orderId));
        return response;
    }

    /** 事件日志的序号健康度：有多少个 seq 被写了不止一次。 */
    public Map<String, Object> seqReport(String orderId) {
        List<StoredEvent> events = commandService.events(orderId);
        Map<Long, Integer> counts = new TreeMap<>();
        Map<String, Integer> byType = new TreeMap<>();
        for (StoredEvent event : events) {
            counts.merge(event.seq(), 1, Integer::sum);
            byType.merge(event.event().getClass().getSimpleName(), 1, Integer::sum);
        }
        List<Long> duplicated = counts.entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .toList();
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("totalEvents", events.size());
        report.put("distinctSeq", counts.size());
        report.put("duplicatedSeq", duplicated);
        report.put("duplicateCount", duplicated.size());
        report.put("eventsByType", byType);
        return report;
    }

    /**
     * 探针 D：快照 vs 全量重放。
     *
     * <p>正常情况下两者必须完全一致——这证明快照是可丢弃的派生数据。
     * 并发写坏之后两者会不一致，因为相同 seq 的事件之间没有确定的先后。
     */
    public Map<String, Object> rebuild(String orderId) {
        Order fromSnapshot = commandService.load(orderId);
        Order fromScratch = commandService.replayFromScratch(orderId);
        boolean same = fromSnapshot.toSnapshot().equals(fromScratch.toSnapshot());
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scenario", "快照恢复 vs 全量重放");
        response.put("identical", same);
        response.put("fromSnapshot", fromSnapshot.toSnapshot());
        response.put("fromScratch", fromScratch.toSnapshot());
        return response;
    }

    /**
     * 探针 C：事件已落库、快照未落库（模拟进程在两次写之间崩溃）。
     *
     * <p>Fluss 没有跨表事务，这两次写不可能原子。这里直接只写事件不写快照，
     * 观察快照表与事件日志的版本差。
     */
    public Map<String, Object> snapshotLag(String orderId) {
        Order order = commandService.load(orderId);
        long version = order.version();
        eventStore.append(orderId, version,
                List.of(new OrderEvent.ItemAdded(orderId, new OrderLine("SKU-CRASH", 1, 100))));

        long snapshotVersion = snapshotStore.load(orderId).map(OrderSnapshot::version).orElse(-1L);
        long maxSeq = commandService.events(orderId).stream()
                .mapToLong(StoredEvent::seq)
                .max()
                .orElse(0L);
        Order reloaded = commandService.load(orderId);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scenario", "只写事件、不写快照（模拟崩溃）");
        response.put("snapshotVersion", snapshotVersion);
        response.put("maxEventSeq", maxSeq);
        response.put("lagEvents", maxSeq - snapshotVersion);
        response.put("reloadedVersion", reloaded.version());
        response.put("recoveredFromLog", reloaded.version() == maxSeq);
        return response;
    }

    /**
     * 探针 D：按聚合回放的实际成本。
     *
     * <p>Fluss 没有按 key 索引的日志读取，读一个聚合必须扫全表再过滤。
     * 这里对比「这个聚合的事件数」和「全表事件数」。
     */
    public Map<String, Object> replayCost(String orderId) {
        long start = System.nanoTime();
        Order replay = commandService.replayFromScratch(orderId);
        long millis = (System.nanoTime() - start) / 1_000_000;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("scenario", "单聚合全量回放");
        response.put("eventsForOrder", replay.version());
        response.put("totalEventsInTable", eventStore.totalEvents());
        response.put("scanMillis", millis);
        return response;
    }

    private List<String> runConcurrently(List<Supplier<String>> commands) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(commands.size());
        ExecutorService pool = Executors.newFixedThreadPool(commands.size());
        List<Future<String>> futures = new ArrayList<>();
        try {
            for (Supplier<String> command : commands) {
                futures.add(pool.submit(() -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    try {
                        return command.get();
                    } catch (Exception e) {
                        return "FAIL:" + e.getClass().getSimpleName() + ":" + e.getMessage();
                    }
                }));
            }
            List<String> results = new ArrayList<>();
            for (Future<String> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
