package com.example.orderdemo.api;

import com.example.orderdemo.application.OrderCommandService;
import com.example.orderdemo.application.OrderView;
import com.example.orderdemo.domain.order.OrderLine;
import com.example.orderdemo.domain.order.OrderSnapshot;
import com.example.orderdemo.domain.order.PlaceOrder;
import com.example.orderdemo.infrastructure.fluss.FlussOrderViewStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderCommandService commandService;
    private final FlussOrderViewStore viewStore;

    public OrderController(OrderCommandService commandService, FlussOrderViewStore viewStore) {
        this.commandService = commandService;
        this.viewStore = viewStore;
    }

    @PostMapping
    public OrderSnapshot place(@RequestBody PlaceRequest request) {
        return commandService.place(new PlaceOrder(request.orderId(), request.customerId(), request.lines()));
    }

    @PostMapping("/{orderId}/items")
    public OrderSnapshot addItem(@PathVariable String orderId, @RequestBody AddItemRequest request) {
        return commandService.addItem(orderId,
                new OrderLine(request.sku(), request.quantity(), request.unitPriceCents()));
    }

    @PostMapping("/{orderId}/pay")
    public OrderSnapshot pay(@PathVariable String orderId, @RequestBody PayRequest request) {
        return commandService.pay(orderId, request.paymentId());
    }

    @PostMapping("/{orderId}/ship")
    public OrderSnapshot ship(@PathVariable String orderId, @RequestBody ShipRequest request) {
        return commandService.ship(orderId, request.trackingNo());
    }

    @PostMapping("/{orderId}/cancel")
    public OrderSnapshot cancel(@PathVariable String orderId, @RequestBody CancelRequest request) {
        return commandService.cancel(orderId, request.reason());
    }

    /** 聚合根当前状态（快照 + 增量事件恢复后的结果）。 */
    @GetMapping("/{orderId}")
    public OrderSnapshot get(@PathVariable String orderId) {
        return commandService.load(orderId).toSnapshot();
    }

    @GetMapping("/{orderId}/events")
    public List<EventItem> events(@PathVariable String orderId) {
        return commandService.events(orderId).stream()
                .map(e -> new EventItem(e.seq(), e.event().getClass().getSimpleName()))
                .toList();
    }

    /** 读模型（订阅端投影出来的结果）。 */
    @GetMapping("/{orderId}/view")
    public Map<String, Object> view(@PathVariable String orderId) {
        OrderView view = viewStore.find(orderId).orElse(null);
        if (view == null) {
            return Map.of("orderId", orderId, "found", false);
        }
        return Map.of(
                "orderId", view.orderId(),
                "customerId", view.customerId(),
                "status", view.status().name(),
                "totalCents", view.totalCents(),
                "itemCount", view.itemCount(),
                "version", view.version(),
                "found", true);
    }

    public record PlaceRequest(String orderId, String customerId, List<OrderLine> lines) {}

    public record AddItemRequest(String sku, int quantity, long unitPriceCents) {}

    public record PayRequest(String paymentId) {}

    public record ShipRequest(String trackingNo) {}

    public record CancelRequest(String reason) {}

    public record EventItem(long seq, String type) {}
}
