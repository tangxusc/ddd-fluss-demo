package com.example.orderdemo.infrastructure.fluss;

import com.example.orderdemo.domain.order.OrderEvent;
import com.example.orderdemo.domain.order.OrderSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 领域事件与 JSON 之间的编解码。
 *
 * <p>事件表是单表 + payload 列，所以事件类型要单独存一列，反序列化时靠它查注册表。
 * 新增事件类型只需要在这里加一行，不用改表结构。
 */
@Component
public class EventCodec {

    private static final Map<String, Class<? extends OrderEvent>> EVENTS = Map.of(
            "OrderPlaced", OrderEvent.OrderPlaced.class,
            "ItemAdded", OrderEvent.ItemAdded.class,
            "OrderPaid", OrderEvent.OrderPaid.class,
            "OrderShipped", OrderEvent.OrderShipped.class,
            "OrderCancelled", OrderEvent.OrderCancelled.class);

    private final ObjectMapper mapper;

    public EventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String typeOf(OrderEvent event) {
        return event.getClass().getSimpleName();
    }

    public String encode(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("序列化失败: " + value, e);
        }
    }

    public OrderEvent decodeEvent(String type, String payload) {
        Class<? extends OrderEvent> clazz = EVENTS.get(type);
        if (clazz == null) {
            throw new IllegalArgumentException("未知事件类型: " + type);
        }
        try {
            return mapper.readValue(payload, clazz);
        } catch (Exception e) {
            throw new IllegalStateException("反序列化失败: " + type + " / " + payload, e);
        }
    }

    public OrderSnapshot decodeSnapshot(String payload) {
        try {
            return mapper.readValue(payload, OrderSnapshot.class);
        } catch (Exception e) {
            throw new IllegalStateException("快照反序列化失败: " + payload, e);
        }
    }
}
