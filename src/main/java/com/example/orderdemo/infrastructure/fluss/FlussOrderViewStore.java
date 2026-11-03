package com.example.orderdemo.infrastructure.fluss;

import com.example.orderdemo.application.OrderView;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** 读模型在 Fluss 里的存取。 */
@Component
public class FlussOrderViewStore {

    private final Connection connection;

    public FlussOrderViewStore(Connection connection) {
        this.connection = connection;
    }

    public Optional<OrderView> find(String orderId) {
        try (Table table = connection.getTable(FlussTables.VIEW)) {
            GenericRow key = new GenericRow(1);
            key.setField(0, BinaryString.fromString(orderId));
            LookupResult result = table.newLookup().createLookuper().lookup(key).get();
            InternalRow row = result.getSingletonRow();
            if (row == null) {
                return Optional.empty();
            }
            return Optional.of(new OrderView(
                    row.getString(FlussTables.VW_ORDER_ID).toString(),
                    row.getString(FlussTables.VW_CUSTOMER_ID).toString(),
                    com.example.orderdemo.domain.order.OrderStatus.valueOf(
                            row.getString(FlussTables.VW_STATUS).toString()),
                    row.getLong(FlussTables.VW_TOTAL_CENTS),
                    row.getInt(FlussTables.VW_ITEM_COUNT),
                    row.getLong(FlussTables.VW_VERSION)));
        } catch (Exception e) {
            throw new IllegalStateException("读取读模型失败: " + orderId, e);
        }
    }

    public void save(OrderView view) {
        try (Table table = connection.getTable(FlussTables.VIEW)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            GenericRow row = new GenericRow(FlussTables.VW_COLUMNS);
            row.setField(FlussTables.VW_ORDER_ID, BinaryString.fromString(view.orderId()));
            row.setField(FlussTables.VW_CUSTOMER_ID, BinaryString.fromString(view.customerId()));
            row.setField(FlussTables.VW_STATUS, BinaryString.fromString(view.status().name()));
            row.setField(FlussTables.VW_TOTAL_CENTS, view.totalCents());
            row.setField(FlussTables.VW_ITEM_COUNT, view.itemCount());
            row.setField(FlussTables.VW_VERSION, view.version());
            row.setField(FlussTables.VW_UPDATED_AT, System.currentTimeMillis());
            writer.upsert(row);
            writer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("写入读模型失败: " + view.orderId(), e);
        }
    }
}
