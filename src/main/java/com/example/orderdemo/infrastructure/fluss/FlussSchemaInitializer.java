package com.example.orderdemo.infrastructure.fluss;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/** 启动时建库建表，全部幂等。 */
@Component
public class FlussSchemaInitializer implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(FlussSchemaInitializer.class);

    private final Connection connection;

    public FlussSchemaInitializer(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Admin admin = connection.getAdmin();
        admin.createDatabase(FlussTables.DATABASE, DatabaseDescriptor.EMPTY, true).get();
        log.info("database ready: {}", FlussTables.DATABASE);

        createTable(admin, FlussTables.EVENT, TableDescriptor.builder()
                .schema(FlussTables.eventSchema())
                .distributedBy(FlussTables.EVENT_BUCKETS, "order_id")
                .build());

        createTable(admin, FlussTables.SNAPSHOT, TableDescriptor.builder()
                .schema(FlussTables.snapshotSchema())
                .distributedBy(3, "order_id")
                .build());

        createTable(admin, FlussTables.VIEW, TableDescriptor.builder()
                .schema(FlussTables.viewSchema())
                .distributedBy(3, "order_id")
                .build());

        createTable(admin, FlussTables.OFFSET, TableDescriptor.builder()
                .schema(FlussTables.offsetSchema())
                .distributedBy(1)
                .build());
    }

    private void createTable(Admin admin, TablePath path, TableDescriptor descriptor) throws Exception {
        boolean existed = admin.tableExists(path).get();
        admin.createTable(path, descriptor, true).get();
        log.info("table {} {}", path, existed ? "already exists" : "created");
    }
}
