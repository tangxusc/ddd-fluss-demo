package com.example.orderdemo.application;

import com.example.orderdemo.domain.order.Order;
import com.example.orderdemo.domain.order.OrderEvent;
import com.example.orderdemo.domain.order.OrderLine;
import com.example.orderdemo.domain.order.OrderSnapshot;
import com.example.orderdemo.domain.order.PlaceOrder;
import com.example.orderdemo.domain.port.EventStore;
import com.example.orderdemo.domain.port.SnapshotStore;
import com.example.orderdemo.domain.port.StoredEvent;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 订单命令处理。
 *
 * <p>典型的事件溯源写路径：加载（快照 + 增量事件）→ 处理命令 → 追加事件 → 更新快照。
 *
 * <p><b>这条链路没有事务。</b> 事件追加和快照写入是两次独立写入，中间崩溃会让快照落后于事件，
 * 但不会丢事件——因为事件先写。反过来（先写快照）就会丢事件。
 */
@Service
public class OrderCommandService {

    private final EventStore eventStore;
    private final SnapshotStore snapshotStore;

    public OrderCommandService(EventStore eventStore, SnapshotStore snapshotStore) {
        this.eventStore = eventStore;
        this.snapshotStore = snapshotStore;
    }

    public OrderSnapshot place(PlaceOrder cmd) {
        Order order = Order.place(cmd);
        persist(order);
        return order.toSnapshot();
    }

    public OrderSnapshot addItem(String orderId, OrderLine line) {
        Order order = load(orderId);
        order.addItem(line);
        persist(order);
        return order.toSnapshot();
    }

    public OrderSnapshot pay(String orderId, String paymentId) {
        Order order = load(orderId);
        order.pay(paymentId);
        persist(order);
        return order.toSnapshot();
    }

    public OrderSnapshot ship(String orderId, String trackingNo) {
        Order order = load(orderId);
        order.ship(trackingNo);
        persist(order);
        return order.toSnapshot();
    }

    public OrderSnapshot cancel(String orderId, String reason) {
        Order order = load(orderId);
        order.cancel(reason);
        persist(order);
        return order.toSnapshot();
    }

    /** 快照 + 增量事件恢复聚合根。这是常规读路径。 */
    public Order load(String orderId) {
        OrderSnapshot snapshot = snapshotStore.load(orderId).orElse(null);
        long fromSeq = snapshot == null ? 0L : snapshot.version();
        List<OrderEvent> events = eventStore.readEvents(orderId, fromSeq).stream()
                .map(StoredEvent::event)
                .toList();
        return Order.rehydrate(orderId, snapshot, events);
    }

    /** 完全不用快照，从事件日志重放。用于验证快照是不是可丢弃的派生数据。 */
    public Order replayFromScratch(String orderId) {
        List<OrderEvent> events = eventStore.readEvents(orderId, 0L).stream()
                .map(StoredEvent::event)
                .toList();
        return Order.rehydrate(orderId, null, events);
    }

    public List<StoredEvent> events(String orderId) {
        return eventStore.readEvents(orderId, 0L);
    }

    private void persist(Order order) {
        List<OrderEvent> events = order.pendingEvents();
        long expectedVersion = order.version() - events.size();
        eventStore.append(order.orderId(), expectedVersion, events);
        order.markEventsCommitted();
        // 快照写在事件之后：崩在中间只会让快照落后，回放能修。
        snapshotStore.save(order.toSnapshot());
    }
}
