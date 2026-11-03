# DDD + Apache Fluss 事件溯源 Demo

用订单领域模型验证一件事：**能不能把 Fluss 当成 DDD 的事件存储和快照存储，由应用直接写入。**

答案是「能存，但不能保证一致性」。实测数据和缺陷清单在 [docs/verdict.md](docs/verdict.md)。

## 快速开始

```bash
./run.sh up        # 起本地 Fluss 集群（ZooKeeper + Coordinator:9123 + Tablet:9124）
./run.sh verify    # 跑完所有场景和缺陷探针，打印实测结果后退出
./run.sh down      # 停集群并删数据卷
```

其它命令：`./run.sh build` / `test` / `server`（Web 模式，REST 在 8080）/ `logs`。

环境要求：JDK 21、Maven、Docker（OrbStack 或 Docker Desktop 均可，镜像有 linux/arm64）。

## 这套东西在做什么

```
命令 ──► 聚合根 ──► 领域事件 ──► Fluss 日志表 order_event
                      │              （bucket.key = order_id，保证聚合内有序）
                      ├──► 聚合根快照 ──► Fluss 主键表 order_snapshot
                      └──► 订阅线程 ──► 读模型 ──► Fluss 主键表 order_view
                                          （位点存 consumer_offset）
```

- **领域事件**：`domain/order/OrderEvent.java`，sealed interface + record，单表 + JSON payload 存储。
- **聚合根快照**：`domain/order/OrderSnapshot.java`，主键表按 order_id upsert，点查恢复。
- **事件订阅**：`infrastructure/fluss/FlussEventSubscription.java`，独立线程从位点拉日志，增量投影读模型。
- **直接写入**：`infrastructure/fluss/FlussEventStore.java`，用 `fluss-client` 的 `AppendWriter` 写日志表，不经过 Flink。

## 验证了什么

`./run.sh verify` 会依次跑：

| 编号 | 内容 | 期望 |
|---|---|---|
| 场景 1 | 下单 → 加商品 → 支付 → 发货 | 版本 1→4，事件顺序正确 |
| 场景 2 | 快照恢复 vs 全量重放 | 两者完全一致，证明快照可丢弃 |
| 场景 3 | 事件订阅投影 | 读模型最终与聚合根一致 |
| 探针 A | 8 线程并发支付同一订单 | **多个 OrderPaid 写出同一个 seq** |
| 探针 B | 发货与取消并发 | **两个互斥事件同时落库** |
| 探针 C | 只写事件不写快照 | 快照落后但可从日志恢复 |
| 探针 D | 单聚合全量回放 | 必须扫全表，成本随全表增长 |

## 目录

```
src/main/java/com/example/orderdemo/
├── domain/order/        聚合根、事件、命令、快照（纯 Java，不依赖 Fluss）
├── domain/port/         EventStore / SnapshotStore 接口
├── infrastructure/fluss/ Fluss 适配层：建表、编解码、事件存储、快照、订阅
├── application/         命令服务、投影、探针、验证入口
└── api/                 REST 接口
```

## 踩过的坑

1. **`arrow-vector` 是 provided 作用域**。`fluss-client` 的 pom 里 Arrow 是 provided，不会传递到你的工程。日志表读写全靠它，不显式声明就是 `NoClassDefFoundError`。pom 里已加 `arrow-vector` + `arrow-memory-netty` 15.0.0。
2. **JDK 17+ 需要开放 `java.nio` 反射**。启动参数必须带 `--add-opens=java.base/java.nio=ALL-UNNAMED`，否则 Arrow 初始化失败。
3. **`fluss-client` 的 jar 里塞了 slf4j 1.7 且没有重定位**。它会和 Spring Boot 的 slf4j 2.0 + logback 抢类，结果整个应用日志退化成 NOP。pom 里用 shade 的 filter 把 `org/slf4j/**` 从 fluss 的 jar 里剔掉才正常。
4. **官方 compose 用了 tmpfs 卷**，macOS 上不生效，改成普通命名卷。
5. **首次 lookup 会报 `NotLeaderOrFollowerException`**。建表后元数据还没传播完，客户端会自动重试并恢复，属于启动期噪音。
6. **`AppendWriter` 没有 `close()`**，只有 `flush()`。每次追加新建 writer 是安全的，但高频写入下建议按线程复用。

## 结论

Fluss 适合做「事件归档 + 快照缓存 + 实时投影源」，不适合做「聚合根的并发真相源」。

具体哪里不行、怎么绕、什么时候该换方案，见 [docs/verdict.md](docs/verdict.md)。
