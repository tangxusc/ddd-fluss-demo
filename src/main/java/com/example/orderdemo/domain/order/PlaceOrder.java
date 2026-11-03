package com.example.orderdemo.domain.order;

/** 下单命令。 */
public record PlaceOrder(String orderId, String customerId, java.util.List<OrderLine> lines) {}
