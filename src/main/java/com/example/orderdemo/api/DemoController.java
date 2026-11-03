package com.example.orderdemo.api;

import com.example.orderdemo.application.ProbeService;
import com.example.orderdemo.infrastructure.fluss.FlussEventSubscription;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** 缺陷探针的 HTTP 入口。逻辑在 {@link ProbeService}。 */
@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final ProbeService probeService;
    private final FlussEventSubscription subscription;

    public DemoController(ProbeService probeService, FlussEventSubscription subscription) {
        this.probeService = probeService;
        this.subscription = subscription;
    }

    @PostMapping("/{orderId}/concurrent-pay")
    public Map<String, Object> concurrentPay(
            @PathVariable String orderId,
            @RequestParam(defaultValue = "8") int threads) throws Exception {
        return probeService.concurrentPay(orderId, threads);
    }

    @PostMapping("/{orderId}/ship-cancel-race")
    public Map<String, Object> shipCancelRace(@PathVariable String orderId) throws Exception {
        return probeService.shipCancelRace(orderId);
    }

    @GetMapping("/{orderId}/seq-report")
    public Map<String, Object> seqReport(@PathVariable String orderId) {
        return probeService.seqReport(orderId);
    }

    @PostMapping("/{orderId}/rebuild")
    public Map<String, Object> rebuild(@PathVariable String orderId) {
        return probeService.rebuild(orderId);
    }

    @GetMapping("/subscription")
    public Map<String, Object> subscription() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("applied", subscription.appliedCount());
        response.put("skippedDuplicates", subscription.skippedCount());
        return response;
    }
}
