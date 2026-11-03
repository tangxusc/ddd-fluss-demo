package com.example.orderdemo.domain.order;

import java.util.List;

/**
 * 订单领域事件。
 *
 * <p>事件是「已经发生的事实」，只描述发生了什么，不包含发生时间等元数据——
 * 时间、序号、traceId 由 {@code EventEnvelope} 在落库时补上。
 *
 * <p>用 sealed interface + 嵌套 record，保证新增事件类型时编译器会提示所有 switch 需要更新。
 */
public sealed interface OrderEvent {

    String orderId();

    record OrderPlaced(String orderId, String customerId, List<OrderLine> lines)
            implements OrderEvent {}

    record ItemAdded(String orderId, OrderLine line) implements OrderEvent {}

    record OrderPaid(String orderId, String paymentId) implements OrderEvent {}

    record OrderShipped(String orderId, String trackingNo) implements OrderEvent {}

    record OrderCancelled(String orderId, String reason) implements OrderEvent {}
}
