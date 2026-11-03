# DDD + Apache Fluss 可行性验证报告

验证时间：2026-11-03
Fluss 版本：1.0.0（2026-09-21 发布，已从 Apache 孵化器毕业为顶级项目）
运行环境：macOS arm64 / OrbStack / JDK 21.0.12 / Spring Boot 3.5.16
复现命令：`./run.sh up && ./run.sh verify`

---

## 一、结论

**能存，不能保证一致性。**

Fluss 可以承担事件溯源里「存储」这一层：领域事件写日志表、聚合根快照写主键表、订阅端拉日志做投影，全部由应用直接调用 `fluss-client` 完成，不需要 Flink 参与。这三条在实测中全部跑通。

但 Fluss 不是事务型存储。它没有多记录原子写、没有 CAS、没有跨表事务、没有 consumer group。后果是：**同一聚合的并发命令会产生重复序号的事件，聚合根的状态机被绕过，而且这个过程完全静默——写入不报错，读出来看着也正常。**

所以定位要摆正：

| 用法 | 是否可行 |
|---|---|
| 事件归档、审计日志、CDC 源 | 可行 |
| 聚合根快照缓存 | 可行 |
| 下游实时投影的数据源 | 可行（至少一次） |
| **聚合根的并发真相源** | **不可行** |

---

## 二、跑通的部分（实测）

### 场景 1：命令 → 事件 → 快照

```
place    -> status=CREATED  version=1 total=3000
addItem  -> status=CREATED  version=2 total=3990
pay      -> status=PAID     version=3 total=3990
ship     -> status=SHIPPED  version=4 total=3990
事件日志: 1=OrderPlaced 2=ItemAdded 3=OrderPaid 4=OrderShipped
```

四个事件顺序正确。这是因为日志表建表时指定了 `bucket.key = order_id`：同一个订单的所有事件哈希到同一个桶，桶内顺序由 Fluss 保证。**去掉这个设置，聚合内的事件顺序就没有任何保证**，事件溯源不成立。

### 场景 2：快照是可丢弃的派生数据

```
identical = true   （快照+增量恢复 与 全量重放 的结果完全一致）
```

快照丢失可以从事件重建，符合事件溯源的基本要求。

### 场景 3：领域事件订阅

```
view = OrderView[orderId=life-10283, status=SHIPPED, totalCents=3990, itemCount=2, version=4]
订阅端 applied=4 skippedDuplicates=0
```

独立线程从 `order_event` 拉日志，增量更新 `order_view` 主键表。位点存在 Fluss 自己的 `consumer_offset` 表里，进程重启后从断点续消费（实测第二次启动从 offset 5 继续，不是从 0）。

---

## 三、跑不通的部分（实测）

以下数据来自 `docker compose down -v` 之后的一次完整运行，数据集是干净的（全表 14 条事件）。

### 探针 A：8 个线程并发支付同一订单

```
结果 = [OK:pay-0, FAIL:当前状态 PAID, FAIL:当前状态 PAID, OK:pay-3,
        FAIL:当前状态 PAID, OK:pay-5, OK:pay-6, FAIL:当前状态 PAID]
成功数 = 4 / 8
事件日志 = {totalEvents=5, distinctSeq=2, duplicatedSeq=[2],
           eventsByType={OrderPaid=4, OrderPlaced=1}}
```

**4 条 `OrderPaid` 事件全部写在了 seq=2。** 命令语义上「支付」只能成功一次，事件日志里却记了四次支付。

多次运行的统计：成功数在 4~7 之间浮动（取决于有多少线程在第一次写入完成前读完了状态），但**重复 seq 每次都会出现**。这是必然而非偶然——`expectedVersion` 只是一个被写进序号的整数，Fluss 没有任何机制校验它。

最要命的是它不报错。四个线程都收到了写入成功，聚合根读出来是 `PAID`，看起来一切正常。只有把事件日志拉出来按 seq 分组，才能看到它已经被写坏了。

### 探针 B：发货与取消并发

```
结果 = [OK:ship, OK:cancel]
事件日志 = {totalEvents=4, distinctSeq=3, duplicatedSeq=[3],
           eventsByType={OrderCancelled=1, OrderPaid=1, OrderPlaced=1, OrderShipped=1}}
最终状态 = CANCELLED
```

状态机规定「已发货不能取消」，但两个命令都基于 `PAID` 状态做了判断，于是 `OrderShipped` 和 `OrderCancelled` 同时落在 seq=3。最终状态是 `CANCELLED`——一个已经发货的订单被取消了。这个状态在领域模型里永远不可能出现。

### 探针 C：事件与快照不在一个事务里

```
快照版本 = 4，事件最大序号 = 5，落后 1 个事件
重新加载后版本 = 5，从日志恢复成功 = true
```

事件追加和快照写入是两次独立的 Fluss 写，没有事务。崩在中间会让快照落后于事件。这个缺陷**可以恢复**：因为事件先写、快照后写，重放日志就能补上。反过来（先写快照）就会丢事件。所以顺序不能调换。

### 探针 D：按聚合读事件要扫全表

```
该聚合事件数 = 5，全表事件数 = 14，扫描耗时 = 20 ms
```

Fluss 的日志表只能按桶顺序读，没有按 key 的索引。读一个聚合的事件必须扫完所有桶再按 `order_id` 过滤。事件总量线性增长，这条路径的成本也线性增长。生产环境聚合上百万之后，这条路径不可用——快照从「可选优化」变成「必须存在」。

---

## 四、缺陷清单

### P0 · 阻断级

**1. 没有乐观并发控制（CAS）**

`EventStore.append(orderId, expectedVersion, events)` 的 `expectedVersion` 参数在 Fluss 实现里只是被用来算序号，没有校验语义。两个并发命令读到同一个版本时，各自都会写出事件。

影响：聚合根不变量在并发下失效，且无任何报错。
现有 API 里没有任何补救手段——没有条件写、没有唯一约束、没有事务。

**2. 没有多事件原子追加**

一条命令产生 N 个事件时，Fluss 会当成 N 条独立记录写入。中途失败会留下半个聚合，无法回滚。`flush()` 只保证「已发送的都被确认或抛错」，不保证全有或全无。

### P1 · 严重

**3. 事件表无唯一约束**

即使应用层想用 `(order_id, seq)` 做幂等，Fluss 日志表也不支持唯一约束或条件插入。重复 seq 拦不住，只能在读取端做去重，而读取端去重无法还原「哪一条才是对的」。

**4. 没有 consumer group**

位点必须由消费方自己维护。本项目把位点写进 `consumer_offset` 表，但「写读模型」和「提交位点」是两次独立写，中间崩溃会重复消费。只能做到至少一次，投影端必须幂等。这比 Kafka 少了整套消费组协调，也少了重平衡、位点重置、监控等配套。

**5. 按聚合回放不可用**

见探针 D。事件溯源的核心能力「从事件重建单个聚合」在 Fluss 上是 O(全表)。这决定了快照不是优化项而是必需品，也决定了「修复单个聚合」这类运维操作不可行。

### P2 · 一般

**6. `append()` 拿不到 offset**

`AppendWriter.append()` 返回 `CompletableFuture<AppendResult>`，而 `AppendResult` 在 1.0.0 里是个空类（源码注释写着 "to have offset, timestamp, etc. in the future"）。应用无法知道事件落在日志的哪个位置，也没法用它做幂等键。只能自己维护 `seq`。

**7. 客户端工程问题**

- `arrow-vector` / `arrow-memory-netty` 在 `fluss-client` 的 pom 里是 `provided`，不传递。不显式声明就在第一次读写日志表时 `NoClassDefFoundError`。
- `fluss-client` 的 jar 里打进了 slf4j 1.7 的类且没有重定位，会和 Spring Boot 的 slf4j 2.0 + logback 抢类路径，导致整个应用日志退化成 NOP。必须用 shade filter 剔掉。
- 建表后首次 lookup 会抛 `NotLeaderOrFollowerException`（元数据未传播完），客户端自动重试恢复，但日志里会留 ERROR。

**8. 生态成熟度**

1.0.0 发布于 2026-09-21，是毕业后的第一个大版本。多语言客户端（Java / Rust / Python / C++ / HTTP Gateway）已经齐了，但运维工具、监控模板、社区问答和公开的生产案例还在积累期，遇到问题能查到的资料远少于 Kafka。

---

## 五、怎么用

### 可以直接用

- 事件归档：命令侧已经在别处保证了正确性，Fluss 只负责长期保存和下游分发。
- 快照缓存：把 Fluss 主键表当 Redis 用，按聚合点查。
- 实时投影源：用 Flink 或自建消费者读 `order_event` 做读模型、指标、风控。

这些场景 Fluss 的优势是真实的：写入端默认 `acks=all` + 幂等（`client.writer.enable-idempotence` 默认开启），重试不会产生重复；日志表用 Arrow 列式存储并支持分层到 S3/HDFS，长期留存的设计成本低于纯行式日志；一套系统同时提供日志和 KV 两种访问路径。

### 需要补东西才能用

如果一定要用它做聚合根的真相源，必须自己补上串行化：

1. **单写者 per 聚合**：按 `order_id` 做路由，同一聚合的命令永远落到同一个进程/线程。这是最省事的做法，代价是扩缩容时要迁移所有权，且进程崩溃后有短暂的不可用窗口。
2. **外部 CAS**：在 PG / Redis 里存 `(aggregate_id, version)`，写事件前先原子地占版本，占到了才写 Fluss。Fluss 退回纯存储角色。
3. **读取端校验**：写完立刻回读并按 seq 去重，发现冲突后补偿。这只是「事后发现」，不是「事前阻止」，无法保证不变量。

### 应该换方案

需要严格的并发正确性、需要跨聚合事务、需要按聚合快速回放时，用天生支持的存储：

- **PostgreSQL 事件表** + `UNIQUE(aggregate_id, seq)` + `INSERT ... WHERE version = expected`。最朴素，也最可靠。
- **EventStoreDB**：专为事件溯源设计，原生支持乐观并发、按 stream 读取、订阅。

Fluss 在这些维度上不是「差一点」，是「没有这个能力」。

---

## 六、一句话总结

Fluss 是一个很好的**流存储**，不是一个**事件存储**。把它当事件存储用，能跑起来，能演示，能上线，直到第一次并发冲突把聚合根写坏——而且它不会告诉你。
