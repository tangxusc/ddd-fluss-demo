package com.example.orderdemo.domain.order;

/**
 * 聚合根快照。
 *
 * <p>快照是「加速读」的派生数据，不是真相源。事件日志才是。任何时刻快照都可以被丢弃并从事件重建。
 */
public record OrderSnapshot(
        String orderId,
        String customerId,
        OrderStatus status,
        java.util.List<OrderLine> lines,
        long totalCents,
        long version,
        String paymentId,
        String trackingNo,
        String cancelReason) {}
