package com.example.orderdemo;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * P0 冒烟检查：验证宿主机能连上本地 Fluss 集群并完成日志表/主键表的读写。
 *
 * <p>不依赖 Spring，直接跑：
 * {@code java --add-opens=java.base/java.nio=ALL-UNNAMED -cp ... com.example.orderdemo.SmokeCheck}
 */
public final class SmokeCheck {

    private static final String DB = "smoke_db";
    private static final TablePath LOG_PATH = TablePath.of(DB, "smoke_log");
    private static final TablePath KV_PATH = TablePath.of(DB, "smoke_kv");

    public static void main(String[] args) throws Exception {
        try {
            run();
            System.out.println("SMOKE OK");
            Thread.sleep(500);
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void run() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");

        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Admin admin = conn.getAdmin();
            System.out.println("[1] databases = " + admin.listDatabases().get());

            admin.createDatabase(DB, DatabaseDescriptor.EMPTY, true).get();
            System.out.println("[2] database ready: " + DB);

            // 每次重跑先清表，保证结果可复现
            admin.dropTable(LOG_PATH, true).get();
            admin.dropTable(KV_PATH, true).get();

            Schema logSchema = Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("seq", DataTypes.BIGINT())
                    .column("payload", DataTypes.STRING())
                    .build();
            TableDescriptor logDesc = TableDescriptor.builder()
                    .schema(logSchema)
                    .distributedBy(2, "id")
                    .build();
            admin.createTable(LOG_PATH, logDesc, true).get();
            TableInfo logInfo = admin.getTableInfo(LOG_PATH).get();
            System.out.println("[3] log table ready, buckets=" + logInfo.getNumBuckets());

            Schema kvSchema = Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("version", DataTypes.BIGINT())
                    .column("snapshot", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
            TableDescriptor kvDesc = TableDescriptor.builder()
                    .schema(kvSchema)
                    .distributedBy(2, "id")
                    .build();
            admin.createTable(KV_PATH, kvDesc, true).get();
            System.out.println("[4] kv table ready");

            // --- 写日志表：同一个 id 的 5 条记录必须落在同一个桶且保持顺序 ---
            try (Table table = conn.getTable(LOG_PATH)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < 5; i++) {
                    GenericRow row = new GenericRow(3);
                    row.setField(0, BinaryString.fromString("order-A"));
                    row.setField(1, (long) i);
                    row.setField(2, BinaryString.fromString("payload-" + i));
                    writer.append(row);
                }
                writer.flush();
            }
            System.out.println("[5] appended 5 rows");

            // --- 读回来 ---
            try (Table table = conn.getTable(LOG_PATH);
                 LogScanner scanner = table.newScan().createLogScanner()) {
                int buckets = table.getTableInfo().getNumBuckets();
                for (int b = 0; b < buckets; b++) {
                    scanner.subscribe(b, 0L);
                }
                List<String> seen = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 8000;
                while (seen.size() < 5 && System.currentTimeMillis() < deadline) {
                    ScanRecords records = scanner.poll(Duration.ofMillis(500));
                    for (TableBucket bucket : records.buckets()) {
                        for (ScanRecord record : records.records(bucket)) {
                            InternalRow row = record.getRow();
                            seen.add(row.getString(0).toString() + "/" + row.getLong(1)
                                    + "@bucket" + bucket.getBucket());
                        }
                    }
                }
                System.out.println("[6] scanned " + seen.size() + " rows: " + seen);
                if (seen.size() != 5) {
                    throw new IllegalStateException("期望读回 5 条，实际 " + seen.size());
                }
            }

            // --- 主键表 upsert + lookup ---
            try (Table table = conn.getTable(KV_PATH)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 0; i < 2; i++) {
                    GenericRow row = new GenericRow(3);
                    row.setField(0, BinaryString.fromString("order-A"));
                    row.setField(1, (long) i);
                    row.setField(2, BinaryString.fromString("snapshot-" + i));
                    writer.upsert(row);
                }
                writer.flush();
            }
            try (Table table = conn.getTable(KV_PATH)) {
                GenericRow key = new GenericRow(1);
                key.setField(0, BinaryString.fromString("order-A"));
                LookupResult result = table.newLookup().createLookuper().lookup(key).get();
                InternalRow row = result.getSingletonRow();
                System.out.println("[7] lookup -> " + (row == null ? "null"
                        : row.getString(0) + "/" + row.getLong(1) + "/" + row.getString(2)));
            }

            // --- 偏移量接口 ---
            Map<Integer, Long> latest = admin.listOffsets(
                    LOG_PATH, List.of(0, 1), new OffsetSpec.LatestSpec()).all().get();
            System.out.println("[8] latest offsets = " + latest);
        }
    }
}
