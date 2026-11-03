package com.example.orderdemo.domain.order;

/**
 * 订单行。
 *
 * <p>金额统一用「分」的 long 表示，避免 BigDecimal 在 JSON 往返中丢精度，
 * 也避开 Fluss 的 DECIMAL 类型映射。
 */
public record OrderLine(String sku, int quantity, long unitPriceCents) {

    public OrderLine {
        if (sku == null || sku.isBlank()) {
            throw new DomainException("sku 不能为空");
        }
        if (quantity <= 0) {
            throw new DomainException("数量必须大于 0: " + quantity);
        }
        if (unitPriceCents < 0) {
            throw new DomainException("单价不能为负: " + unitPriceCents);
        }
    }

    public long totalCents() {
        return Math.multiplyExact((long) quantity, unitPriceCents);
    }
}
