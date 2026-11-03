package com.example.orderdemo.infrastructure.fluss;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.springframework.stereotype.Component;

/**
 * 消费位点存储。
 *
 * <p>Fluss 没有 Kafka 那样的 consumer group，位点必须由消费方自己维护。
 * 这里把位点也写进 Fluss，但这和「写读模型」是两次独立的写，不是原子的——
 * 所以投影只能做到至少一次，做不到恰好一次。
 */
@Component
public class FlussOffsetStore {

    private final Connection connection;

    public FlussOffsetStore(Connection connection) {
        this.connection = connection;
    }

    public long load(String groupId, int bucket) {
        try (Table table = connection.getTable(FlussTables.OFFSET)) {
            GenericRow key = new GenericRow(2);
            key.setField(0, BinaryString.fromString(groupId));
            key.setField(1, bucket);
            LookupResult result = table.newLookup().createLookuper().lookup(key).get();
            InternalRow row = result.getSingletonRow();
            return row == null ? 0L : row.getLong(FlussTables.OF_OFFSET);
        } catch (Exception e) {
            throw new IllegalStateException("读取位点失败: " + groupId + "/" + bucket, e);
        }
    }

    public void commit(String groupId, int bucket, long offset) {
        try (Table table = connection.getTable(FlussTables.OFFSET)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            GenericRow row = new GenericRow(FlussTables.OF_COLUMNS);
            row.setField(FlussTables.OF_GROUP_ID, BinaryString.fromString(groupId));
            row.setField(FlussTables.OF_BUCKET, bucket);
            row.setField(FlussTables.OF_OFFSET, offset);
            row.setField(FlussTables.OF_UPDATED_AT, System.currentTimeMillis());
            writer.upsert(row);
            writer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("提交位点失败: " + groupId + "/" + bucket, e);
        }
    }
}
