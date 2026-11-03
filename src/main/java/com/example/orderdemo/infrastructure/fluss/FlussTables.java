package com.example.orderdemo.infrastructure.fluss;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

/** Fluss 表定义集中在这里，避免列下标散落在各处。 */
public final class FlussTables {

    public static final String DATABASE = "order_ddd";

    public static final TablePath EVENT = TablePath.of(DATABASE, "order_event");
    public static final TablePath SNAPSHOT = TablePath.of(DATABASE, "order_snapshot");
    public static final TablePath VIEW = TablePath.of(DATABASE, "order_view");
    public static final TablePath OFFSET = TablePath.of(DATABASE, "consumer_offset");

    public static final int EVENT_BUCKETS = 3;

    /** 事件表列下标。 */
    public static final int EV_ORDER_ID = 0;
    public static final int EV_SEQ = 1;
    public static final int EV_TYPE = 2;
    public static final int EV_PAYLOAD = 3;
    public static final int EV_OCCURRED_AT = 4;
    public static final int EV_TRACE_ID = 5;
    public static final int EV_COLUMNS = 6;

    /** 快照表列下标。 */
    public static final int SN_ORDER_ID = 0;
    public static final int SN_STATUS = 1;
    public static final int SN_VERSION = 2;
    public static final int SN_PAYLOAD = 3;
    public static final int SN_UPDATED_AT = 4;
    public static final int SN_COLUMNS = 5;

    /** 读模型列下标。 */
    public static final int VW_ORDER_ID = 0;
    public static final int VW_CUSTOMER_ID = 1;
    public static final int VW_STATUS = 2;
    public static final int VW_TOTAL_CENTS = 3;
    public static final int VW_ITEM_COUNT = 4;
    public static final int VW_VERSION = 5;
    public static final int VW_UPDATED_AT = 6;
    public static final int VW_COLUMNS = 7;

    /** 偏移量表列下标。 */
    public static final int OF_GROUP_ID = 0;
    public static final int OF_BUCKET = 1;
    public static final int OF_OFFSET = 2;
    public static final int OF_UPDATED_AT = 3;
    public static final int OF_COLUMNS = 4;

    private FlussTables() {}

    /**
     * 事件表：追加型日志表，按 order_id 哈希分桶。
     *
     * <p>{@code bucket.key = order_id} 是整套设计成立的前提：同一个订单的所有事件落进同一个桶，
     * 桶内顺序由 Fluss 保证，聚合内的事件顺序才有意义。去掉这个设置，事件顺序就没保证了。
     */
    public static Schema eventSchema() {
        return Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("seq", DataTypes.BIGINT())
                .column("event_type", DataTypes.STRING())
                .column("payload", DataTypes.STRING())
                .column("occurred_at", DataTypes.BIGINT())
                .column("trace_id", DataTypes.STRING())
                .build();
    }

    /** 快照表：主键表，一个订单一行，只保留最新版本。 */
    public static Schema snapshotSchema() {
        return Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("status", DataTypes.STRING())
                .column("version", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING())
                .column("updated_at", DataTypes.BIGINT())
                .primaryKey("order_id")
                .build();
    }

    /** 读模型表：投影结果，被订阅端持续更新。 */
    public static Schema viewSchema() {
        return Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("customer_id", DataTypes.STRING())
                .column("status", DataTypes.STRING())
                .column("total_cents", DataTypes.BIGINT())
                .column("item_count", DataTypes.INT())
                .column("version", DataTypes.BIGINT())
                .column("updated_at", DataTypes.BIGINT())
                .primaryKey("order_id")
                .build();
    }

    /** 消费位点表：Fluss 没有 consumer group，位点得自己存。 */
    public static Schema offsetSchema() {
        return Schema.newBuilder()
                .column("group_id", DataTypes.STRING())
                .column("bucket", DataTypes.INT())
                .column("offset", DataTypes.BIGINT())
                .column("updated_at", DataTypes.BIGINT())
                .primaryKey("group_id", "bucket")
                .build();
    }
}
