package com.example.orderdemo.infrastructure.trace;

/** 请求级 traceId。事件信封里带上它，方便把一条命令产生的所有事件串起来。 */
public final class TraceContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TraceContext() {}

    public static void set(String traceId) {
        CURRENT.set(traceId);
    }

    public static String get() {
        String traceId = CURRENT.get();
        return traceId == null ? "-" : traceId;
    }

    public static void clear() {
        CURRENT.remove();
    }
}
