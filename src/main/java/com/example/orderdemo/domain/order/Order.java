package com.example.orderdemo.domain.order;

import java.util.ArrayList;
import java.util.List;

/**
 * 订单聚合根。
 *
 * <p>所有状态变更都必须经过「命令 → 事件 → apply」这条路径，没有任何 setter。
 * 这是事件溯源的基本约束：状态是事件的投影，不是可以直接改的字段。
 *
 * <p>{@code version} 等于已应用的事件数，也是乐观并发的版本号。
 *
 * <p>命令方法产生的新事件会进入 {@link #pendingEvents()}，由应用层负责落库后调用
 * {@link #markEventsCommitted()} 清空。重放历史事件走 {@link #apply}，不会进入待提交列表。
 */
public final class Order {

    private final String orderId;
    private String customerId;
    private OrderStatus status;
    private final List<OrderLine> lines = new ArrayList<>();
    private long version;
    private String paymentId;
    private String trackingNo;
    private String cancelReason;
    private final List<OrderEvent> pending = new ArrayList<>();

    private Order(String orderId) {
        this.orderId = orderId;
    }

    /** 下单。返回的聚合根已经应用了 {@link OrderEvent.OrderPlaced}，version = 1。 */
    public static Order place(PlaceOrder cmd) {
        if (cmd.orderId() == null || cmd.orderId().isBlank()) {
            throw new DomainException("orderId 不能为空");
        }
        if (cmd.customerId() == null || cmd.customerId().isBlank()) {
            throw new DomainException("customerId 不能为空");
        }
        if (cmd.lines() == null || cmd.lines().isEmpty()) {
            throw new DomainException("订单至少需要一个商品行");
        }
        Order order = new Order(cmd.orderId());
        order.raise(new OrderEvent.OrderPlaced(cmd.orderId(), cmd.customerId(), List.copyOf(cmd.lines())));
        return order;
    }

    /** 从快照恢复，再重放快照之后的事件。快照为 null 表示从头重放。 */
    public static Order rehydrate(String orderId, OrderSnapshot snapshot, List<OrderEvent> events) {
        Order order = new Order(orderId);
        if (snapshot != null) {
            order.customerId = snapshot.customerId();
            order.status = snapshot.status();
            order.lines.addAll(snapshot.lines());
            order.version = snapshot.version();
            order.paymentId = snapshot.paymentId();
            order.trackingNo = snapshot.trackingNo();
            order.cancelReason = snapshot.cancelReason();
        }
        for (OrderEvent event : events) {
            order.apply(event);
        }
        return order;
    }

    public List<OrderEvent> addItem(OrderLine line) {
        requireStatus(OrderStatus.CREATED, "只有 CREATED 状态的订单可以添加商品");
        return raise(new OrderEvent.ItemAdded(orderId, line));
    }

    public List<OrderEvent> pay(String paymentId) {
        requireStatus(OrderStatus.CREATED, "只有 CREATED 状态的订单可以支付");
        if (paymentId == null || paymentId.isBlank()) {
            throw new DomainException("paymentId 不能为空");
        }
        return raise(new OrderEvent.OrderPaid(orderId, paymentId));
    }

    public List<OrderEvent> ship(String trackingNo) {
        requireStatus(OrderStatus.PAID, "只有 PAID 状态的订单可以发货");
        if (trackingNo == null || trackingNo.isBlank()) {
            throw new DomainException("trackingNo 不能为空");
        }
        return raise(new OrderEvent.OrderShipped(orderId, trackingNo));
    }

    public List<OrderEvent> cancel(String reason) {
        if (status == null) {
            throw new DomainException("订单不存在: " + orderId);
        }
        if (status == OrderStatus.SHIPPED) {
            throw new DomainException("已发货的订单不能取消");
        }
        if (status == OrderStatus.CANCELLED) {
            throw new DomainException("订单已取消");
        }
        return raise(new OrderEvent.OrderCancelled(orderId, reason));
    }

    /** 重放历史事件。不进入待提交列表。 */
    public void apply(OrderEvent event) {
        mutate(event);
    }

    /** 产生新事件：先记录到待提交列表，再改变状态。 */
    private List<OrderEvent> raise(OrderEvent event) {
        pending.add(event);
        mutate(event);
        return List.of(event);
    }

    private void mutate(OrderEvent event) {
        switch (event) {
            case OrderEvent.OrderPlaced e -> {
                this.customerId = e.customerId();
                this.status = OrderStatus.CREATED;
                this.lines.clear();
                this.lines.addAll(e.lines());
            }
            case OrderEvent.ItemAdded e -> this.lines.add(e.line());
            case OrderEvent.OrderPaid e -> {
                this.status = OrderStatus.PAID;
                this.paymentId = e.paymentId();
            }
            case OrderEvent.OrderShipped e -> {
                this.status = OrderStatus.SHIPPED;
                this.trackingNo = e.trackingNo();
            }
            case OrderEvent.OrderCancelled e -> {
                this.status = OrderStatus.CANCELLED;
                this.cancelReason = e.reason();
            }
        }
        this.version++;
    }

    private void requireStatus(OrderStatus expected, String message) {
        if (status == null) {
            throw new DomainException("订单不存在: " + orderId);
        }
        if (status != expected) {
            throw new DomainException(message + "，当前状态 " + status);
        }
    }

    /** 尚未落库的事件。 */
    public List<OrderEvent> pendingEvents() {
        return List.copyOf(pending);
    }

    /** 事件已成功写入事件存储，清空待提交列表。 */
    public void markEventsCommitted() {
        pending.clear();
    }

    public String orderId() {
        return orderId;
    }

    public String customerId() {
        return customerId;
    }

    public OrderStatus status() {
        return status;
    }

    public List<OrderLine> lines() {
        return List.copyOf(lines);
    }

    public long totalCents() {
        return lines.stream().mapToLong(OrderLine::totalCents).sum();
    }

    public long version() {
        return version;
    }

    public String paymentId() {
        return paymentId;
    }

    public String trackingNo() {
        return trackingNo;
    }

    public String cancelReason() {
        return cancelReason;
    }

    public OrderSnapshot toSnapshot() {
        return new OrderSnapshot(
                orderId, customerId, status, lines(), totalCents(), version, paymentId, trackingNo, cancelReason);
    }
}
