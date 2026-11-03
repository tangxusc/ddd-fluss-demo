package com.example.orderdemo.domain.port;

import com.example.orderdemo.domain.order.OrderEvent;

/** 带序号的事件。序号由聚合根版本决定，不依赖存储层分配的偏移量。 */
public record StoredEvent(long seq, OrderEvent event) {}
