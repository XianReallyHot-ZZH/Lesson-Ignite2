# 课程地图 v1 —— Lesson-Ignite2（Apache Ignite 2.18.0 复刻课程总课表）

> 本文档是 wayfinder 地图（[issue #1](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/1)）的汇总产出，由 15 张已关闭决策票（ marshaller 线 + 13 章弧 + 事件弧）与 9 份验收调研（docs/research/01–16）拼装而成，2026-10-04 定稿。
> 每课详情（概念簇/tracer/验收来源）以对应决策票的 resolution 为准——本文是索引与依赖图，不重述细节。
> 编号重排说明：事件与消息弧插为章 9（原 fog 毕业票，位置在 H2 SQL 之后、calcite 之前）；原"章 9 calcite"→章 10、"章 10 计算与服务"→章 11、"章 11 部署"→章 12、"章 12 收尾"→章 13。

## 一、总课表（97 主课 + 1 选做）

| 章 | 课 | 标题 | tracer / 里程碑 |
|---|---|---|---|
| **0 构建骨架与 Ignition 生命周期** | 0.1 | 5 模块 reactor 骨架（provided+shade） | mvn install 全绿 + 跨模块冒烟 |
| | 0.2 | 入口与注册表（Ignition/grids/nodeId，Spring 重载显式抛） | 注册表语义测试 |
| | 0.3 | kernal 容器（gateway/ctx/GridComponent；最小闭包 Pool+Timeout+marshaller 三件套） | **`Ignition.start()` 返回且 STARTED** + JdkMarshaller 往返 |
| | 0.4 | 生命周期收口（stop 逆序/shutdown hook/LifecycleBean） | start→stop→同名 restart |
| **1 单节点缓存** | 1.1 | cache 创建与代理层 | cache 生命周期 |
| | 1.2 | entry 层与并发容器（VersionManager） | 并发 put 正确性 |
| | 1.3 | 写路径贯通（ATOMIC 退化，no-op 原则） | **里程碑：put/get** |
| | 1.4 | 批量、扫描与过期 | **里程碑：put/get/scan** |
| **2 SPI + Discovery（全保真）** | 2.1 | SPI 框架（GridManagerAdapter 五步，Resource 最小版） | fake SPI 五步 |
| | 2.2 | 单节点 discovery + 拓扑视图变换 | start() 含本地 join |
| | 2.3 | join 与环建立（拒绝族/checkAttributes） | **里程碑：2 节点互发现** |
| | 2.4 | ring 消息总线（双轨序列化/ensured/Discard） | ensured 广播清空断言 |
| | 2.5 | 故障检测与 ring 愈合（心跳双圈/previousNodeId 反查） | kill -9 收敛；慢节点不误杀 |
| | 2.6 | 多播 IP finder（侧支） | 零预配互发现 |
| | 2.7 | custom 通道与 finder 生命周期（segmentation） | custom 事件 + cleaner 清除 |
| **3 通信（全保真 NIO）** | 3.1 | Message 基础设施（手写 codec） | 序列化往返 |
| | 3.2 | GridIoManager（topic 分发/本机短路） | 本机消息往返 |
| | 3.3 | NIO 服务器骨架（selector/session） | echo + 线程模型可见 |
| | 3.4 | filter 链与编解码（2B-LE framing） | 半包续读 |
| | 3.5 | 写队列与背压（四层限流之一二） | 慢消费者实验 |
| | 3.6 | SPI 装配与出站连接 | **里程碑：跨节点消息往返** |
| | 3.7 | 握手协议（四消息/order 消解） | 时序+错误码+双连唯一 |
| | 3.8 | 恢复与 ack（RecoveryDescriptor at-least-once） | 断网不丢不重 |
| | 3.9 | 稳态治理（idle/重连/slowClient/inverse） | 空闲关连+反连 |
| | 3.10 | binary v1 · 任意对象过线（真格式子集） | 自定义对象跨节点 |
| | 3.11 | （选做）paired connections 与 channel | paired 双连 ack |
| **4 Affinity 与 rebalance** | 4.1 | Rendezvous function | 分配稳定单测 |
| | 4.2 | assignment cache | ready future 顺序 |
| | 4.3 | exchange 骨架（coordinator 收齐-广播） | kill 一节点分区图一致 |
| | 4.4 | exchange 高级（merge/BECOME_CRD/exchangeFreeSwitch） | 交换中杀 coordinator |
| | 4.5 | 五态状态机 + late affinity（diffFromAffinity） | put 路由随 rebalance 变化 |
| | 4.6 | rebalance（Demand/Supply/RebalanceFuture 链） | **里程碑：分区正确落位含备份** |
| | 4.7 | REPLICATED 与 cache group | 同组双 cache 一次 rebalance |
| | 4.8 | client cache 语义（CLIENT 型 exchange/转发） | fat client join+put+断连重连 |
| **5 near cache** | 5.1 | 结构与读加速（near-first 三段式） | 命中率>0+消息数下降 |
| | 5.2 | reader 登记、捎带回写与淘汰（版本门） | 零额外消息更新 |
| **6 事务（全保真）** | 6.1 | 隐式微事务与一阶段提交 | 单 key put 跨节点提交 |
| | 6.2 | 锁系统（candidate 队列/死锁检测） | 死锁被检测抛异常 |
| | 6.3 | 悲观 2PC（finish 消息对/回滚=commit=false） | 多 primary 提交+回滚一致 |
| | 6.4 | 乐观事务与隔离矩阵（3×2） | 写冲突回滚 |
| | 6.5 | SERIALIZABLE + 拓扑×事务（remap/PME latch） | kill 节点 in-flight remap |
| | 6.6 | 崩溃恢复（recovery 共识） | kill 发起节点共识裁决 |
| | 6.7 | CacheStore×tx（two-phase + write-behind） | 会话回调序+断电窗口 |
| | 6.8 | 事务×near + 系统事务（utility cache 通道） | near 事务失效+系统事务 |
| **7 持久化（全保真，最大章）** | 7.1 | PageMemory（锁+tag） | 页池单测 |
| | 7.2 | PageStore 文件层 | 重启页文件可复读 |
| | 7.3 | BPlusTree+FreeList（**接缝替换**，树可第二用户复用） | 纯内存 put/get 走真树 |
| | 7.4 | WAL（帧格式/手写 codec/delta 挂点） | .wal 逐帧可解析 |
| | 7.5 | checkpoint（标记双写/反压） | kill 后已 checkpoint 页在盘 |
| | 7.6 | 恢复编排（位置防重放/uncommitted tx） | **里程碑：重启数据存活**（6 kill 注入矩阵） |
| | 7.7 | baseline/activation 状态机 | activate→deactivate→数据在 |
| | 7.7b | historical rebalance 补课（WAL 增量供应） | 增量模式 join |
| | 7.8 | TTL 后台清理 + eviction/region 治理 | 过期清除+页淘汰不丢 |
| | 7.9 | snapshot（full，借 checkpoint 拷贝） | 快照→恢复集群 |
| | 7.10 | WAL compaction（SegmentAware） | 归档压缩后可恢复 |
| | 7.11 | 增量 snapshot | 基快照+增量恢复 |
| **8 H2 SQL 课弧** | 8.1 | 表映射（QueryEntity+注解双入口/事件源） | 本地 SELECT |
| | 8.2 | SQL 索引落盘（InlineIndexTree 复用 7.3 树） | 索引页重启存活 |
| | 8.3 | 查询切分与两阶段 | **里程碑：跨节点 SELECT** |
| | 8.4 | DDL 链（动态 cache SQL 皮肤/第二 discovery 协议） | 纯 SQL 建删表多节点一致 |
| | 8.5 | 分布式 join（map 侧远程索引查找） | colocated vs distributed 双跑 |
| | 8.6 | 查询进阶（裁剪代数树/EXPLAIN/lazy） | EXPLAIN 见裁剪 |
| | 8.7 | DML（SELECT 改写+CAS+重跑） | **跨节点 UPDATE/DELETE** |
| | 8.8 | 查询治理 + Lucene（offheap Directory） | KILL 释放资源 |
| **9 事件与消息（fog 毕业弧）** | 9.1 | routine 基础设施（V1/V2 双协议/批量/ack） | 空壳 handler 全周期+join 自动获得 |
| | 9.2 | cache 事件桥（双 fire/primary-backup 分叉/ack 回路） | put→断 backup→ack 清理 |
| | 9.3 | 投递保证（PartitionRecovery 三场景） | **kill primary：不丢不重** |
| | 9.4 | events + messaging 消费者（remoteQuery RPC/TOPIC_COMM_USER） | 事件全链+remoteListen |
| **10 calcite 课弧（库为黑盒）** | 10.1 | 引擎接入与路由（三级选择） | 双引擎注册+hint 路由 |
| | 10.2 | schema 映射与单节点 SELECT（VolcanoUtils 补丁） | 与 H2 结果一致 |
| | 10.3 | fragment 切分与跨节点派发 | **里程碑：双引擎一致+hint 切换** |
| | 10.4 | exchange 与流控（256/4/ack） | 分页+背压可观测 |
| | 10.5 | 算子全集与分区裁剪 | 三表 join+裁剪 |
| | 10.6 | 查询治理（Registry/KILL/内存配额/条带池） | KILL 释放 fragment |
| **11 计算与服务（四线）** | 11.1 | ComputeTask 最小闭环 | 手写 task 跨节点 |
| | 11.2 | 闭包门面（IgniteComputeHandler 保留） | 全闭包 API 面 |
| | 11.3 | 资源注入全家桶 + LB 三 SPI | 注入生效 |
| | 11.4 | failover 与取消 | **里程碑：MR 作业；kill worker 重跑** |
| | 11.5 | session 与 checkpoint | 属性同步+cp 恢复 |
| | 11.6 | collision 与执行治理 | 自定义 SPI 排队 |
| | 11.7 | services 控制面（二阶段不降级） | singleton 部署+故障重分配 |
| | 11.8 | services 本地与 proxy（sticky） | proxy 调用+断 master 重试 |
| | 11.9 | DataStreamer（双路径汇合） | 百万条流式加载 |
| | 11.10 | clear 双广播 + 流式 INSERT 收尾 | clear 集群语义+SQL 流式 |
| | 11.11 | DataStructures · atomic 族（系统事务） | AtomicLong 跨节点递增 |
| | 11.12 | DataStructures · queue/set 族 | 跨节点队列 |
| **12 部署子系统（全保真）** | 12.1 | 部署骨架与 GridDeployment 状态机 | 退化路径+状态机单测 |
| | 12.2 | p2p 管道（双 store/防环链） | **里程碑：p2p 派单类到达远端** |
| | 12.3 | marshaller 映射协议（含磁盘持久化） | 全网收敛+重启映射在 |
| | 12.4 | UriDeploymentSpi（file+http/签名校验） | gar 包零显式部署 |
| | 12.5 | 清理链与 undeploy | 类缓存全清 |
| **13 客户端形态与收尾** | 13.1 | binary 格式与 schema（compactFooter 双分支） | **字节逐位对照 vendor** |
| | 13.2 | metadata 集群交换（三通道） | A 写新类型→B keepBinary 读 |
| | 13.3 | BinaryObject API（Builder/withKeepBinary） | 改字段重建+e2e |
| | 13.4 | thick client：ClientImpl（换 UUID 重生） | kill router→重连恢复 |
| | 13.5 | thin 握手与 cache op 双端（partition awareness） | **里程碑：官方 thin client 连复刻节点 put/get** |
| | 13.6 | thin 查询游标（ReliableChannel） | 官方 client scan+SQL 翻页 |
| | 13.7 | JSR-107 + metrics 收口 + COPY 推包 | 标准入口+CSV 入集群 |

**课数核对**：章 0（4）+1（4）+2（7）+3（10+1 选做）+4（8）+5（2）+6（8）+7（12，含 7.7b）+8（8）+9（4）+10（6）+11（12）+12（5）+13（7）= **97 主课 + 1 选做**。课号连续性 ✓（各章内部 1..n 连续，7.7b 为补课插入位）。

## 二、章间依赖 DAG（blocking = 先修）

```
0 → 1 → 2 → 3 → 4 ─┬→ 5(near) ─→ 6(tx) ─→ 7(persistence) ─→ 8(H2 SQL) ─→ 9(events) ─→ 10(calcite)
                   │                  │ 5↔6：6.8 消费 5.x                      8 ─────────────┐
                   └→ 11(compute) ────┼──────────────────────────────────────────────────────┤
                          │            └（6 的 WAL TxRecord 被 7.6 消费）                    │
                          └→ 12(deploy)：11 消费部署；12.3 依赖 2/3 的两条总线              │
                                                       13(client)：9(ring 半边)+3(binary v1)+8(SQL)+12.2(metadata)
```

要点：4 是最大分叉点（5/6/11 三线）；7 依赖 6（WAL 事务记录）与 1（接缝替换）；8 依赖 7（InlineIndex 用 PageMemory）；10 依赖 8（共享 SchemaManager 事件源与 InlineIndex，零新建）；13 是全课程总收口（thin client 验收 = 官方客户端互操作）。

## 三、kernal 终态对齐（C-11 验收：vendor 启动序列全覆盖核对）

vendor 41 processor + 12 manager（research 01 §5/§4）逐项处置——有课位或显式排除，无沉默缺失：

| 组件 | 处置 |
|---|---|
| Pool/Timeout/Resource/Cluster/InternalSubscription | 0.3 / 0.3 / 2.1→11.3 / 2.1+8.1 |
| Closure/Task/Job/TaskSession | 11.1–11.5 |
| Checkpoint(manager)/Deployment/LB/Failover/Collision | 11.5 / 12 章 / 11.3 / 11.4 / 11.6 |
| Discovery/Io/EventStorage/Metric | 2.2 / 3.2 / 9.4 / 13.7（Q13 最小收口） |
| CacheObjectBinary / MarshallerMapping | 3.10+13.1–13.3 / 12.3 |
| CacheProcessor/IndexProcessor/QueryProcessor/QueryEngine | 1 章 / 8.2 / 8 章 / 10 章 |
| ClusterState/PdsConsistentId/Segmentation | 7.7 / 7.2–7.7 / 2.7 |
| ClientListener/Service/DataStream/Continuous/DataStructures | 13.5–13.6 / 11.7–11.8 / 11.9 / 9 章 / 11.11–11.12 |
| Affinity(processor) | 4.1–4.2 |
| Tracing/Encryption/Security/Compression/Platform/REST/Schedule | **显式排除**（ADR 0001：外围/独立子系统；附录记接缝） |
| RollingUpgrade | **排除**（同构集群无版本混部） |
| Diagnostic/Failure/Plugin/Port/JobMetrics/PerformanceStatistics/Maintenance/MetaStorage/DistributedConfiguration/DurableBackgroundTasks/CacheObjectTransformer/SystemView/Tracing/Encryption/EncryptionSpi(Indexing 自定义) | **附录清单**（诊断/FailureHandler 通道/plugin 框架/端口登记/废弃指标替代/perf 文件/维护模式/metastore 深处/分布式配置扩展/后台任务/转换扩展点/system view 导出/IndexingSpi 插件接口）——其中 MetaStorage 以持久化 metastore 形态随 7.2/7.9 实现，DistributedCalciteConfiguration 消费点在 10.1 |

**终态断言**（写入最后一课验收）：kernal 启动序列 == vendor research 01 §6 时间线的子序列，且上表"排除/附录"项之外无缺口；资源注入注解清单对齐 vendor 全集（11.3 验收）。

## 四、附录（不设课，记接缝与扩展路线）

- **A. 外围 17 模块接缝清单**：spring（IgniteSpringHelper 接口/core 留口）、web/urideploy(已入课)/json、rest-http、jta（CacheJtaManager 接缝）、log4j2/slf4j/jcl、zookeeper/kubernetes（IP finder 插件）、opencensus、control-utility（命令本体在 core `internal/management`，随课弧生长）、direct-io/numa、dev-utils、extdata/*、codegen（旧）。依据 research 02 §3/§5。
- **B. 深挖 calcite 扩展路线**（课后可选开发，ADR 0001）：索引下推全链深化、colocation/MPP 调度推导、算子性能变体、背压进阶、查询治理深挖、VolcanoUtils 式库补丁实践。依据 research 06 §5.3。
- **C. 永久排除汇总**：platforms（.NET/C++）、MVCC（2.18 不存在）、实验性安全/加密、字节级带宽限流（不存在——真身是四层消息级限流，已全做）、schedule、RollingUpgrade、真 Ignite 集群互操作（thin client 协议除外）。
- **D. 可简化项沉淀**（历次裁决中保留但标注"低频"的部分）：MulticastIpFinder 已入主线（2.6）；显式锁 API（IgniteCache.lock）显式抛；geospatial 仅钩子；metrics/JMX 最小实现（Q13）。

## 五、教学总纪律（贯穿）

1. 认知负担优先：一课一簇、2–4h、宁多切不双簇（ROADMAP §3.2）。
2. 接口忠实、实现渐进（ADR 0004）：no-op 原则、占位与真身落点见各章票。
3. 全保真（ADR 0005）：线协议与线程模型不简化；历次"可简化"建议均已逐条裁决并记录在对应票。
4. 验收 = TDD 红绿切片 + 改写 vendor 测试（消息序列断言优先）+ 分章 kill 注入矩阵（7.6/9.3 等）。
5. 每课完成产出配图充分的学习者讲义 `lessons/XX.YY-name/explainer.md`（mermaid），git tag 逐课打点。
