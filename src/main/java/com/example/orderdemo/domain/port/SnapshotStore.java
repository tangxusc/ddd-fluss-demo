package com.example.orderdemo.domain.port;

import com.example.orderdemo.domain.order.OrderSnapshot;

import java.util.Optional;

/** 聚合根快照存储。快照是派生数据，丢失可以从事件重建。 */
public interface SnapshotStore {

    Optional<OrderSnapshot> load(String orderId);

    void save(OrderSnapshot snapshot);
}
