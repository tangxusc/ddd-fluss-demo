package com.example.orderdemo.domain.order;

/** 订单状态机。状态只能沿 CREATED → PAID → SHIPPED 前进，或在 SHIPPED 之前取消。 */
public enum OrderStatus {
    CREATED,
    PAID,
    SHIPPED,
    CANCELLED
}
