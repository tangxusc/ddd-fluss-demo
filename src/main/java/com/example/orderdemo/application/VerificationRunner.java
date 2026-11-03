package com.example.orderdemo.application;

import com.example.orderdemo.domain.order.OrderLine;
import com.example.orderdemo.domain.order.OrderSnapshot;
import com.example.orderdemo.domain.order.PlaceOrder;
import com.example.orderdemo.infrastructure.fluss.FlussEventSubscription;
import com.example.orderdemo.infrastructure.fluss.FlussOrderViewStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 一键验证：跑完所有场景和缺陷探针，把实测结果打到 stdout，然后退出。
 *
 * <p>用 {@code --demo.verify=true --spring.main.web-application-type=none} 触发。
 * 之所以不做成 HTTP 探针，是因为结论要能一条命令复现，不依赖端口和客户端。
 */
@Component
@ConditionalOnProperty(name = "demo.verify", havingValue = "true")
public class VerificationRunner implements ApplicationRunner {

    private final OrderCommandService commandService;
    private final ProbeService probeService;
    private final FlussOrderViewStore viewStore;
    private final FlussEventSubscription subscription;
    private final ConfigurableApplicationContext context;

    public VerificationRunner(
            OrderCommandService commandService,
            ProbeService probeService,
            FlussOrderViewStore viewStore,
            FlussEventSubscription subscription,
            ConfigurableApplicationContext context) {
        this.commandService = commandService;
        this.probeService = probeService;
        this.viewStore = viewStore;
        this.subscription = subscription;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        String runId = String.valueOf(System.currentTimeMillis() % 100000);
        int exitCode = 0;
        try {
            scenarioLifecycle(runId);
            scenarioRebuild(runId);
            scenarioProjection(runId);
            probeConcurrentPay(runId);
            probeShipCancelRace(runId);
            probeSnapshotLag(runId);
            probeReplayCost(runId);
            System.out.println();
            System.out.println("VERIFY OK");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println();
            System.out.println("VERIFY FAILED");
            exitCode = 1;
        }
        // 先停订阅再关连接，否则 LogFetcher 会在连接关闭后刷一堆 Disconnected 堆栈。
        subscription.stop();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        context.close();
        System.exit(exitCode);
    }

    private void scenarioLifecycle(String runId) {
        String oid = "life-" + runId;
        System.out.println();
        System.out.println("[场景 1] 订单生命周期：命令 → 事件 → 快照");
        OrderSnapshot placed = commandService.place(new PlaceOrder(
                oid, "C-" + runId, List.of(new OrderLine("SKU-1", 2, 1500))));
        printSnapshot("place   ", placed);

        printSnapshot("addItem ", commandService.addItem(oid, new OrderLine("SKU-2", 1, 990)));
        printSnapshot("pay     ", commandService.pay(oid, "PAY-" + runId));
        printSnapshot("ship    ", commandService.ship(oid, "SF" + runId));

        System.out.println("  事件日志: " + describeEvents(oid));
    }

    private void scenarioRebuild(String runId) {
        String oid = "life-" + runId;
        Map<String, Object> result = probeService.rebuild(oid);
        System.out.println();
        System.out.println("[场景 2] 快照恢复 vs 全量重放");
        System.out.println("  identical = " + result.get("identical"));
        System.out.println("  说明: 快照是可丢弃的派生数据，重放事件能还原出同样的状态");
    }

    private void scenarioProjection(String runId) throws Exception {
        String oid = "life-" + runId;
        OrderView view = awaitView(oid, 4, 20_000);
        System.out.println();
        System.out.println("[场景 3] 领域事件订阅 → 读模型");
        System.out.println("  view = " + view);
        System.out.println("  订阅端 applied=" + subscription.appliedCount()
                + " skippedDuplicates=" + subscription.skippedCount());
    }

    private void probeConcurrentPay(String runId) throws Exception {
        String oid = "probe-pay-" + runId;
        commandService.place(new PlaceOrder(oid, "C-" + runId, List.of(new OrderLine("SKU-1", 1, 1000))));
        Map<String, Object> result = probeService.concurrentPay(oid, 8);
        System.out.println();
        System.out.println("[探针 A] 8 个线程并发支付同一订单");
        System.out.println("  结果 = " + result.get("results"));
        System.out.println("  成功数 = " + result.get("succeeded") + " / 8");
        System.out.println("  事件日志 = " + result.get("eventLog"));
    }

    private void probeShipCancelRace(String runId) throws Exception {
        String oid = "probe-race-" + runId;
        commandService.place(new PlaceOrder(oid, "C-" + runId, List.of(new OrderLine("SKU-1", 1, 1000))));
        commandService.pay(oid, "PAY-" + runId);
        Map<String, Object> result = probeService.shipCancelRace(oid);
        System.out.println();
        System.out.println("[探针 B] 发货与取消并发（两个命令互斥）");
        System.out.println("  结果 = " + result.get("results"));
        System.out.println("  事件日志 = " + result.get("eventLog"));
        System.out.println("  最终状态 = " + commandService.load(oid).status());
    }

    private void probeSnapshotLag(String runId) {
        String oid = "life-" + runId;
        Map<String, Object> result = probeService.snapshotLag(oid);
        System.out.println();
        System.out.println("[探针 C] 只写事件、不写快照（模拟两次写之间崩溃）");
        System.out.println("  快照版本 = " + result.get("snapshotVersion")
                + "，事件最大序号 = " + result.get("maxEventSeq")
                + "，落后 " + result.get("lagEvents") + " 个事件");
        System.out.println("  重新加载后版本 = " + result.get("reloadedVersion")
                + "，从日志恢复成功 = " + result.get("recoveredFromLog"));
    }

    private void probeReplayCost(String runId) {
        String oid = "life-" + runId;
        Map<String, Object> result = probeService.replayCost(oid);
        System.out.println();
        System.out.println("[探针 D] 单聚合全量回放成本");
        System.out.println("  该聚合事件数 = " + result.get("eventsForOrder")
                + "，全表事件数 = " + result.get("totalEventsInTable")
                + "，扫描耗时 = " + result.get("scanMillis") + " ms");
        System.out.println("  说明: 读一个聚合要扫全表，聚合多了这条路径不可用");
    }

    private OrderView awaitView(String orderId, long minVersion, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            OrderView view = viewStore.find(orderId).orElse(null);
            if (view != null && view.version() >= minVersion) {
                return view;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("等待读模型超时: " + orderId + " 期望版本 " + minVersion);
    }

    private void printSnapshot(String command, OrderSnapshot snapshot) {
        System.out.printf("  %s -> status=%-8s version=%d total=%d%n",
                command, snapshot.status(), snapshot.version(), snapshot.totalCents());
    }

    private String describeEvents(String orderId) {
        StringBuilder sb = new StringBuilder();
        commandService.events(orderId)
                .forEach(e -> sb.append(e.seq()).append("=").append(e.event().getClass().getSimpleName()).append(" "));
        return sb.toString().trim();
    }
}
