package com.example.orderdemo.config;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlussConfig {

    /**
     * Connection 是线程安全的，全应用一个实例。
     *
     * <p>Table 和 Admin 不是线程安全的，所以不在这里暴露成 bean，由各个适配器按需创建。
     */
    @Bean(destroyMethod = "close")
    public Connection flussConnection(@Value("${demo.fluss.bootstrap-servers:localhost:9123}") String bootstrapServers) {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        return ConnectionFactory.createConnection(conf);
    }
}
