package com.example.orderdemo.application;

import com.example.orderdemo.domain.order.OrderEvent;
import com.example.orderdemo.domain.order.OrderLine;
import com.example.orderdemo.domain.order.OrderStatus;
import com.example.orderdemo.infrastructure.fluss.FlussOrderViewStore;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * 把领域事件增量地投影成读模型。
 *
 * <p>幂等性靠版本号：事件序号不大于读模型当前版本时直接跳过。
 * 这样重复消费（至少一次语义下的必然结果）不会污染读模型。
 */
@Service
public class OrderViewProjector {

    private final FlussOrderViewStore viewStore;

    public OrderViewProjector(FlussOrderViewStore viewStore) {
        this.viewStore = viewStore;
    }

    /** @return true 表示真正应用了，false 表示重复事件被跳过 */
    public boolean apply(long seq, OrderEvent event) {
        Optional<OrderView> existing = viewStore.find(event.orderId());
        if (existing.isPresent() && seq <= existing.get().version()) {
            return false;
        }
        OrderView next = switch (event) {
            case OrderEvent.OrderPlaced e -> new OrderView(
                    e.orderId(),
                    e.customerId(),
                    OrderStatus.CREATED,
                    e.lines().stream().mapToLong(OrderLine::totalCents).sum(),
                    e.lines().size(),
                    seq);
            case OrderEvent.ItemAdded e -> {
                OrderView base = require(existing, e.orderId());
                yield new OrderView(base.orderId(), base.customerId(), base.status(),
                        base.totalCents() + e.line().totalCents(), base.itemCount() + 1, seq);
            }
            case OrderEvent.OrderPaid e -> {
                OrderView base = require(existing, e.orderId());
                yield new OrderView(base.orderId(), base.customerId(), OrderStatus.PAID,
                        base.totalCents(), base.itemCount(), seq);
            }
            case OrderEvent.OrderShipped e -> {
                OrderView base = require(existing, e.orderId());
                yield new OrderView(base.orderId(), base.customerId(), OrderStatus.SHIPPED,
                        base.totalCents(), base.itemCount(), seq);
            }
            case OrderEvent.OrderCancelled e -> {
                OrderView base = require(existing, e.orderId());
                yield new OrderView(base.orderId(), base.customerId(), OrderStatus.CANCELLED,
                        base.totalCents(), base.itemCount(), seq);
            }
        };
        viewStore.save(next);
        return true;
    }

    private OrderView require(Optional<OrderView> existing, String orderId) {
        return existing.orElseThrow(() ->
                new IllegalStateException("读模型缺少前置状态，事件乱序或丢事件: " + orderId));
    }
}
