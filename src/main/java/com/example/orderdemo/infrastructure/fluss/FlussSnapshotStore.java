package com.example.orderdemo.infrastructure.fluss;

import com.example.orderdemo.domain.order.OrderSnapshot;
import com.example.orderdemo.domain.port.SnapshotStore;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 基于 Fluss 主键表的聚合根快照存储。
 *
 * <p>主键表天然适合做快照：按 order_id upsert 覆盖，按 order_id 点查，不用自己维护索引。
 */
@Component
public class FlussSnapshotStore implements SnapshotStore {

    private final Connection connection;
    private final EventCodec codec;

    public FlussSnapshotStore(Connection connection, EventCodec codec) {
        this.connection = connection;
        this.codec = codec;
    }

    @Override
    public Optional<OrderSnapshot> load(String orderId) {
        try (Table table = connection.getTable(FlussTables.SNAPSHOT)) {
            GenericRow key = new GenericRow(1);
            key.setField(0, BinaryString.fromString(orderId));
            LookupResult result = table.newLookup().createLookuper().lookup(key).get();
            InternalRow row = result.getSingletonRow();
            if (row == null) {
                return Optional.empty();
            }
            return Optional.of(codec.decodeSnapshot(row.getString(FlussTables.SN_PAYLOAD).toString()));
        } catch (Exception e) {
            throw new IllegalStateException("读取快照失败: " + orderId, e);
        }
    }

    @Override
    public void save(OrderSnapshot snapshot) {
        try (Table table = connection.getTable(FlussTables.SNAPSHOT)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            GenericRow row = new GenericRow(FlussTables.SN_COLUMNS);
            row.setField(FlussTables.SN_ORDER_ID, BinaryString.fromString(snapshot.orderId()));
            row.setField(FlussTables.SN_STATUS, BinaryString.fromString(String.valueOf(snapshot.status())));
            row.setField(FlussTables.SN_VERSION, snapshot.version());
            row.setField(FlussTables.SN_PAYLOAD, BinaryString.fromString(codec.encode(snapshot)));
            row.setField(FlussTables.SN_UPDATED_AT, System.currentTimeMillis());
            writer.upsert(row);
            writer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("写入快照失败: " + snapshot.orderId(), e);
        }
    }
}
