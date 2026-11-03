package com.example.orderdemo.domain.order;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 聚合根的不变量。这些用例不碰 Fluss，只验证领域逻辑本身是对的。 */
class OrderTest {

    private static Order newOrder() {
        return Order.place(new PlaceOrder("O-1", "C-1",
                List.of(new OrderLine("SKU-1", 2, 1500))));
    }

    @Test
    void 下单后状态为已创建且版本为一() {
        Order order = newOrder();
        assertThat(order.status()).isEqualTo(OrderStatus.CREATED);
        assertThat(order.version()).isEqualTo(1);
        assertThat(order.totalCents()).isEqualTo(3000);
        assertThat(order.pendingEvents()).hasSize(1);
    }

    @Test
    void 已支付的订单不能加商品() {
        Order order = newOrder();
        order.pay("PAY-1");
        assertThatThrownBy(() -> order.addItem(new OrderLine("SKU-2", 1, 100)))
                .isInstanceOf(DomainException.class)
                .hasMessageContaining("只有 CREATED 状态");
    }

    @Test
    void 不能重复支付() {
        Order order = newOrder();
        order.pay("PAY-1");
        assertThatThrownBy(() -> order.pay("PAY-2"))
                .isInstanceOf(DomainException.class)
                .hasMessageContaining("只有 CREATED 状态");
    }

    @Test
    void 未支付不能发货() {
        Order order = newOrder();
        assertThatThrownBy(() -> order.ship("SF-1"))
                .isInstanceOf(DomainException.class)
                .hasMessageContaining("只有 PAID 状态");
    }

    @Test
    void 已发货不能取消() {
        Order order = newOrder();
        order.pay("PAY-1");
        order.ship("SF-1");
        assertThatThrownBy(() -> order.cancel("不想要了"))
                .isInstanceOf(DomainException.class)
                .hasMessageContaining("已发货");
    }

    @Test
    void 重放事件能还原出同样的状态() {
        Order order = newOrder();
        order.addItem(new OrderLine("SKU-2", 1, 990));
        order.pay("PAY-1");

        Order replayed = Order.rehydrate("O-1", null, List.of(
                new OrderEvent.OrderPlaced("O-1", "C-1", List.of(new OrderLine("SKU-1", 2, 1500))),
                new OrderEvent.ItemAdded("O-1", new OrderLine("SKU-2", 1, 990)),
                new OrderEvent.OrderPaid("O-1", "PAY-1")));

        assertThat(replayed.status()).isEqualTo(OrderStatus.PAID);
        assertThat(replayed.version()).isEqualTo(3);
        assertThat(replayed.totalCents()).isEqualTo(3990);
        assertThat(replayed.toSnapshot()).isEqualTo(order.toSnapshot());
    }

    @Test
    void 快照加增量事件与全量重放结果一致() {
        List<OrderEvent> all = List.of(
                new OrderEvent.OrderPlaced("O-1", "C-1", List.of(new OrderLine("SKU-1", 2, 1500))),
                new OrderEvent.ItemAdded("O-1", new OrderLine("SKU-2", 1, 990)),
                new OrderEvent.OrderPaid("O-1", "PAY-1"));

        Order fromScratch = Order.rehydrate("O-1", null, all);
        OrderSnapshot snapshotAtVersion2 = new OrderSnapshot(
                "O-1", "C-1", OrderStatus.CREATED,
                List.of(new OrderLine("SKU-1", 2, 1500), new OrderLine("SKU-2", 1, 990)),
                3990, 2, null, null, null);
        Order fromSnapshot = Order.rehydrate("O-1", snapshotAtVersion2, all.subList(2, 3));

        assertThat(fromSnapshot.toSnapshot()).isEqualTo(fromScratch.toSnapshot());
    }
}
