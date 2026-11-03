package com.example.orderdemo.application;

import com.example.orderdemo.domain.order.OrderStatus;

/** 订单读模型。由事件订阅端持续维护，只用于查询。 */
public record OrderView(
        String orderId,
        String customerId,
        OrderStatus status,
        long totalCents,
        int itemCount,
        long version) {}
