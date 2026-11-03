package com.example.orderdemo.domain.port;

import com.example.orderdemo.domain.order.OrderEvent;

import java.util.List;

/**
 * 事件存储。
 *
 * <p>{@code expectedVersion} 表达的是「只有当前版本等于它才允许追加」的意图。
 * 具体实现能不能真的校验，取决于存储层能力——Fluss 做不到，这一点在实现类里明确写出来。
 */
public interface EventStore {

    void append(String orderId, long expectedVersion, List<OrderEvent> events);

    /** 读取序号大于 {@code afterSeq} 的事件，按序号升序。 */
    List<StoredEvent> readEvents(String orderId, long afterSeq);

    /** 事件表总行数。用于量化「按聚合回放要扫全表」的成本。 */
    long totalEvents();
}
