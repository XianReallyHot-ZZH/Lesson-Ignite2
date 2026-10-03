# 06 · ignite-calcite 集成层结构：库当黑盒，Ignite 侧怎么接

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。前置结论直接沿用 `docs/research/04-persistence-sql.md` §2.3：calcite 与 H2 双引擎并存、按配置/hint 选择。本文回答五个问题：模块内部分层、`CalciteQueryProcessor` 生命周期、cache→schema/表映射、分布式执行路径、以及"集成层复刻"的最小闭包。正文中的短路径（如 `prepare/...`、`.../calcite/exec/...`）是 `modules/calcite/src/main/java/org/apache/ignite/internal/processors/query/calcite/` 下的缩写，仅为排版省行；**文末"引用文件清单"给出全部完整可定位路径**（均已逐一打开验证），行号以当前 submodule 内容为准。

---

## 0. 一句话总览

ignite-calcite 模块 = **calcite 库之上的三个薄适配**（parser/validator 配置、convention+rel+rule+metadata 的计划器扩展、type factory）+ **一套自有的分布式执行框架**（fragment 切分、mailbox 数据交换、消息协议）+ **几个对 core 的钩子**（`QueryEngine` 接口实现、`SchemaChangeListener` 订阅 schema 变更、复用 core 的 `InlineIndex` B+树索引与 `GridDhtLocalPartition` 分区扫描）。与 H2 引擎最大的结构差异：**不再把 SQL 切成 map/reduce 两次 SQL 执行，而是把物理计划切成 fragment 树、用统一的火山式执行器在数据节点上流式跑、按 batch 回传**。

---

## 1. 模块内部结构（434 个 main .java 的分层）

`find modules/calcite/src/main/java -name '*.java' | wc -l` = 434（另有 test 源码 178 个，不计）。按职责分三层：

### 1.1 层 A：对 core 的集成（约 90 文件）

| 包 | 文件数 | 职责 |
|---|---|---|
| 根包 `calcite/` | 9 | `CalciteQueryProcessor`（processor 本体）+ 查询运行时对象（`Query`/`RootQuery`/`QueryRegistry(Impl)`/`QueryState`/`RunningFragment`/`RemoteFragmentKey`）+ `DistributedCalciteConfiguration`（运行时可变配置，如 `sql.calcite.disabledRules`，`DistributedCalciteConfiguration.java:38-52`） |
| `calcite/schema/` | 21 | cache→表映射：`SchemaHolderImpl`（订阅 schema 变更）、`CacheTableImpl`/`CacheTableDescriptorImpl`（表与行类型）、`CacheIndexImpl`（索引包装）、`SystemView*`（系统视图表）、`ViewTableImpl` |
| `calcite/message/` | 18 | 自有消息协议：`MessageType`（类型码 300+）、`QueryStartRequest/Response`、`QueryBatchMessage`、`QueryBatchAcknowledgeMessage`、`QueryCloseMessage`、`InboxCloseMessage`、`CalciteErrorMessage`、`MessageServiceImpl`（挂到 `GridTopic.TOPIC_QUERY`）、`CalciteMessageFactory` |
| `org/apache/ignite/calcite/` | 2 | 公共配置 API `CalciteQueryEngineConfiguration`（`ENGINE_NAME="calcite"`，`CalciteQueryEngineConfiguration.java:29`） |

### 1.2 层 B：对 calcite 库的适配（约 210 文件）

| 包 | 文件数 | 职责 |
|---|---|---|
| `calcite/rel/`（含 set/agg/logical） | 59 | Ignite 物理 `RelNode`（`IgniteTableScan`/`IgniteIndexScan`/`IgniteHashJoin`/`IgniteMergeJoin`/`IgniteExchange`/`IgniteSender`/`IgniteReceiver`/…）+ `IgniteConvention` |
| `calcite/prepare/`（含 ddl/bounds） | 58 | 双重身份：既是对 calcite 的适配（`IgnitePlanner`/`IgniteSqlValidator`/`IgniteConvertletTable`/`PlannerPhase` 规则分阶段编排），也是集成核心（`PrepareServiceImpl`/`Splitter`/`Fragment`/`QueryTemplate`/`QueryPlanCache`） |
| `calcite/rule/`（含 logical） | 32 | converter 规则（logical→Ignite convention）与启发式规则（`ExposeIndexRule`/`FilterScanMergeRule`/join 顺序等） |
| `calcite/metadata/`（含 cost） | 26 | `RelMetadataProvider` 扩展：行数/选择率/代价/**fragment 映射**（`IgniteMdFragmentMapping`）、`IgniteCostFactory`，外加 `AffinityService`/`MappingService`（对 core affinity 的薄封装，见 §3） |
| `calcite/sql/`（含 generated/stat/kill/fun） | 43 | 自有 parser（`IgniteSqlParserImpl`，ffmpeg 式生成代码在 `generated/`）与 DDL AST（`IgniteSqlCreateTable` 等）、`SET`/`KILL` 语句 |
| `calcite/trait/` | 17 | 物理 trait：`DistributionTrait(Diff)`/`RewindabilityTrait(Diff)`/`CorrelationTrait(Diff)`、`IgniteDistribution`（affinity/any/single/broadcast 语义） |
| `calcite/type/`、`calcite/hint/`、`calcite/externalize/` | 4+4+4 | `IgniteTypeFactory`/`IgniteTypeSystem`；hint 方言；`RelJson*`（fragment 的 JSON 序列化，跨节点传计划用） |
| `org/apache/calcite/plan/volcano/` | 1 | 直接给 calcite 库同名包打补丁：`VolcanoUtils.bestCost` 读取 `RelSubset.bestCost` 私有字段（`VolcanoUtils.java:23`）——库黑盒化的第一个例外点 |

### 1.3 层 C：分布式执行自有框架（约 124 文件）

| 包 | 文件数 | 职责 |
|---|---|---|
| `calcite/exec/` | 33 | 执行服务（`ExecutionServiceImpl`/`ExchangeServiceImpl`/`MailboxRegistry(Impl)`）、`LogicalRelImplementor`（Rel→执行 Node）、数据访问（`TableScan`/`IndexScan`/`IndexCountScan`/`IndexFirstLastScan`）、`PartitionExtractor`（分区裁剪） |
| `calcite/exec/rel/` | 33 | 火山式算子：`ScanNode`/`FilterNode`/`ProjectNode`/`HashJoinNode`/`MergeJoinNode`/`SortNode`/`AggregateNode`/`*SpoolNode`/**`Inbox`/`Outbox`**（mailbox）等 |
| `calcite/exec/exp/`（含 agg） | 31 | 表达式求值：`RexToLixTranslator`/`RexImpTable`/`ExpressionFactory`（基于 janino 编译）、`RangeIterable`（索引扫描区间） |
| `calcite/exec/tracker/` | 11 | 查询/算子两级内存配额追踪 |
| `calcite/exec/task/` | 6 | 查询任务线程池：`StripedQueryTaskExecutor`（默认，按 queryId 分条带防死锁）与 `QueryBlockingTaskExecutor`（UDF 安全模式，开关见 `CalciteQueryProcessor.java:164-171`） |
| `calcite/exec/partition/` | 8 | `PartitionNode` 表达式树：运行时按参数算出目标分区（分区裁剪） |
| `calcite/util/` | 12 | 杂项（`Commons`、`AbstractService`、`Service`/`LifecycleAware` 生命周期约定等） |

补充一个量化事实：模块内 **222 个不同的非 calcite 包 `org.apache.ignite` import**，其中最高频的是工具类（`typedef.F` 91 次、`typedef.internal.U` 39 次）与 `GridKernalContext`（24 次），真正的数据面依赖集中在 `GridCacheContextInfo`/`GridCacheContext`/`CacheDataRow`/`GridCursor`/`AffinityTopologyVersion` 各 5-10 次——集成面比 H2 引擎窄得多（H2 侧需要 `GridH2Table` 整套自建）。

---

## 2. CalciteQueryProcessor 生命周期与引擎装配

### 2.1 组件装配：`IgniteComponentType.QUERY_ENGINE`

与 indexing 模块同款机制：`IgniteComponentType.QUERY_ENGINE` 声明 noOp 类 `NoOpQueryEngine`、实现类 `org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor`、模块名 `ignite-calcite`，**并把 `CalciteMessageFactory` 注册为消息工厂**（`modules/core/src/main/java/org/apache/ignite/internal/IgniteComponentType.java:116-121`）。即 calcite 在 classpath 上 → 真实 processor 组件化；不在 → no-op。

### 2.2 start/stop：13 个内部 service 的统一生命周期

`CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine`（`CalciteQueryProcessor.java:135`）。构造函数一次性 new 出 13 个协作者（`CalciteQueryProcessor.java:282-318`）：

`SchemaHolderImpl`、`QueryPlanCacheImpl`、`MailboxRegistryImpl`、`QueryTaskExecutor`（Striped 或 Blocking，`L293-295`）、`ExecutionServiceImpl`、`AffinityServiceImpl`、`MessageServiceImpl`、`MappingServiceImpl`、`ExchangeServiceImpl`、`PrepareServiceImpl`、`TimeoutServiceImpl`、`QueryRegistryImpl`、`InjectResourcesService`。

- `onKernalStart`（`L399-416`）逐个调用 `onStart(ctx)`（`L834-839`）；`onKernalStop`（`L419-438`）反向 `onStop()`。各 service 通过 `util/Service.java` 与 `LifecycleAware` 约定接入。
- 启动期的实际注册发生在各 service 的 `init()`：例如 `MessageServiceImpl.init()` 往 `GridIoManager` 挂 `GridTopic.TOPIC_QUERY` 监听（`message/MessageServiceImpl.java:138-140`）；`ExecutionServiceImpl.init()` 订阅 `QUERY_START_REQUEST/RESPONSE/ERROR` 三种消息并监听节点离开事件（`exec/ExecutionServiceImpl.java:472-480`）；`ExchangeServiceImpl.init()` 订阅 `QUERY_BATCH/ACKNOWLEDGE/CLOSE/INBOX_CANCEL` 四种（`exec/ExchangeServiceImpl.java:190-194`）。
- 还有两个静态块级别的"库适配"：关掉 volcano planner 的 dump 输出、对 calcite `SqlToRelConverter`/`Correlate` 类关闭断言以绕过 CALCITE-7009/5421/7034 三个上游 bug（`CalciteQueryProcessor.java:136-150`）——集成层要为库缺陷打补丁的实例。
- calcite 侧的 `FrameworkConfig`（parser/validator/convertlet/operatorTable/traitDefs/costFactory，`CalciteQueryProcessor.java:174-214`）允许插件整体替换（`ctx.plugins().createComponent(FrameworkConfig.class)`，`L285-286`）。

### 2.3 与 GridQueryProcessor 的 QueryEngine 契约

core 侧接口 `QueryEngine extends GridProcessor`（`modules/core/.../query/QueryEngine.java:28`），除 GridProcessor 生命周期外只有四个查询入口：`query`/`parameterMetaData`/`resultSetMetaData`/`queryBatched`（`QueryEngine.java:37-82`）。装配与选择全部在 `GridQueryProcessor`：

1. **`initQueryEngines()`**（`GridQueryProcessor.java:574-654`）：读 `SqlConfiguration.getQueryEnginesConfiguration()`；**没配任何引擎 → 默认 H2**（indexing，`L583-597`）；配了则逐个校验"每个引擎类至多一个实例"（`L611-624`），非 `IndexingQueryEngine` 的引擎**从已注册组件里按类查找**（calcite processor 正是这样被找到的）；默认引擎只允许一个（`L635-642`）。
2. **`engineForQuery()`**（`GridQueryProcessor.java:3236-3268`）：SQL hint（`QUERY_ENGINE('calcite')`）→ client context 的 `queryEngine()` → 默认引擎。
3. **调用点**：`querySqlFields` 流程内 `engineForQuery(cliCtx, qry)` 后统一走 `qryEngine.query(qryCtx, schemaName, sql, params)` 或 `qryEngine.queryBatched(...)`（`GridQueryProcessor.java:3087-3126`）。也就是说 **H2 与 calcite 在 core 眼里只是同一个接口的两个实现**，SqlFieldsQuery 的解析入口、游标封装、取消机制全在 core 侧共享。

### 2.4 与 H2（IndexingQueryEngine）并存

- H2 引擎是 core 侧标记接口 `IndexingQueryEngine`（`modules/core/.../query/IndexingQueryEngine.java:23`）的唯一实现方向（`IndexingQueryEngineConfiguration`，ENGINE_NAME="h2"）；calcite 的配置类直接实现公共的 `QueryEngineConfiguration`（`engineClass()` 返回 `CalciteQueryProcessor.class`）。
- 模块 README 给出并存用法：两个配置并列、`default` 二选一；JDBC URL 参数 `?queryEngine=calcite`、ODBC `QUERY_ENGINE=` 或 SQL hint 逐查询切换（`modules/calcite/README.txt`）。
- **注意运行时耦合**：`modules/calcite/pom.xml` 依赖 `ignite-indexing`（`pom.xml:57-71`），README 明言"some logic from ignite-indexing module is reused"。但主源码里对 `processors.query.h2` 零 import，仅用到 core 的 `org.apache.ignite.spi.indexing.IndexingQueryFilter(Impl)`（backup 过滤谓词）——复刻时 indexing 可作为纯运行时依赖保留而不抄其代码。

---

## 3. cache → schema/表/行类型/索引映射

### 3.1 事件驱动：SchemaChangeListener（对照 H2 的 onSqlTypeCreated 回调）

calcite 侧**不走** `GridQueryProcessor` 的直接回调，而是订阅 core 的事件接口：`SchemaHolderImpl extends AbstractService implements SchemaChangeListener`（`schema/SchemaHolderImpl.java:68`），在构造时 `subscriptionProcessor.registerSchemaChangeListener(this)`（`L157-170`，经 `modules/core/.../subscription/GridInternalSubscriptionProcessor.java:85` 注册）。事件源是 core 的 `SchemaManager`——每个 SQL 类型注册完成时 `lsnr.onSqlTypeCreated(schemaName, type, cacheInfo)`（`modules/core/.../query/schema/management/SchemaManager.java:381`），同一分发点也通知 H2 侧监听器。覆盖的事件还有：schema 建/删、列增/删（`L199-250`）、类型删、**索引建/删/重建**（`L297-359`）、UDF/表函数、系统视图、视图（`L362-446`）。

`onSqlTypeCreated` 的处理就三行：`new CacheTableDescriptorImpl(cacheInfo, typeDesc, affinityIdentity)` 包进 `CacheTableImpl`，`publishTable` 后整体 `rebuild()`（`SchemaHolderImpl.java:190-196, 253-261, 264-274`）。`rebuild()` 重建 calcite 根 schema（`Frameworks.createRootSchema`），把所有 `IgniteSchema` 挂为 subSchema（`L464-475`）——**每次变更整个 schema 树换新引用（volatile 写），无锁读**。

### 3.2 表/行类型：QueryEntity → calcite RelDataType

- `IgniteTable extends TranslatableTable`（`schema/IgniteTable.java:36`），对 calcite 暴露的核心扩展是三个方法：`getRowType(typeFactory, requiredColumns)`（列裁剪，`L53`）、`distribution()`（声明数据分布 trait，`L85`）、`colocationGroup(ctx)`（声明"哪些节点持有哪些分区"，`L80`）。
- `CacheTableDescriptorImpl` 持有 core 的 `GridCacheContextInfo` + `GridQueryTypeDescriptor`（`schema/CacheTableDescriptorImpl.java:88, 118`）：`rowType()` 把 QueryTypeDescriptor 的字段列表转成 `IgniteTypeFactory` 的行类型（`L486`）；`distribution()` 三分支——REPLICATED → `broadcast()`、无 affinity 键 → `random()`、否则 `affinity(affFields, cacheId, affinityIdentity)`（`L236-244`；`affinityIdentity` = affinity 函数类+backups+分区数+nodeFilter 的相等性包装，`SchemaHolderImpl.java:85-144`，用于跨 cache 同构共置判断）。
- `colocationGroup()` 直接问 core：`cctx.affinity().assignments(topVer)` 逐分区取节点列表（PRIMARY_SYNC 时只留 primary），REPLICATED 用 `cacheGroupAffinityNodes`（`CacheTableDescriptorImpl.java:517-585`）。这就是规划期"数据在哪"的唯一事实来源。

### 3.3 数据访问：分区扫描与索引

- **全表扫描**：`CacheTableImpl.scan()` 返回 `exec/TableScan`（`schema/CacheTableImpl.java:93-104`），后者按 `GridDhtLocalPartition` 逐分区 reserve，用 core 的 `GridCursor<? extends CacheDataRow>` 迭代行、`desc.toRow` 转成计算行（`exec/TableScan.java:44` 起）——**直接扫 cache 存储层，不经过任何 H2 或 SQL 中间层**。
- **索引**：calcite 引擎**复用 core 的 `InlineIndex`（与 H2 引擎同一套 PageMemory B+树索引，由 core 的 `IndexProcessor` 建维护）**。`onIndexCreated` 只是把 core 的 `IndexDescriptor` 包成 `CacheIndexImpl`：记录 `RelCollation`（供规划器做序/范围推理）+ 持有 `Index` 引用（`SchemaHolderImpl.java:297-333`）。执行期 `CacheIndexImpl.scan()` 调 `idx.unwrap(InlineIndex.class)` 构造 `exec/IndexScan`（`schema/CacheIndexImpl.java:93-103`），`IndexScan` 直接 import core 的 `InlineIndex/InlineIndexTree/BPlusTree/SortedSegmentedIndexCursor` 做范围查找（`exec/IndexScan.java:59` 及其 import 区）。规划期由 `ExposeIndexRule`+`prepare/bounds/` 的 `SearchBounds` 决定能否用索引、下推什么区间。

**结论**：索引的存储与维护完全不在 calcite 模块内——集成层只需"声明 collation + 打开 InlineIndex 游标"。这与 H2 侧 `H2IndexFactory`→`InlineIndexImpl` 的关系一致（见 04 报告 §2.2），双引擎共享同一份物理索引。

---

## 4. 分布式执行路径：一条 SqlFieldsQuery 在 calcite 引擎下的旅程

### 4.1 入口与解析（发起节点）

`GridQueryProcessor.querySqlFields` → `engineForQuery` 选中 calcite → `CalciteQueryProcessor.query()`（`CalciteQueryProcessor.java:441-448`）→ `parseAndProcessQuery`（`L536-599`）：查 `QueryPlanCache`（CacheKey=schema+SQL+context+params，`L552`）→ miss 则 `Commons.parse`（多语句拆分）→ 逐条 `processQuery`（构造 `RootQuery`、注册 `QueryRegistry`、记 `EVT_SQL_QUERY_EXECUTION` 事件，`L729-806`）→ `prepareSvc.prepareSingle` → `executionSvc.executePlan`。

### 4.2 规划与切分（prepare 期，与拓扑无关）

`PrepareServiceImpl.prepareSingle` 按 SqlKind 分派到 `prepareQuery`（`prepare/PrepareServiceImpl.java:83-138`），后者是全链核心（`L173-193`）：

```
validateAndGetTypeMetadata（IgniteSqlValidator）
→ optimize：按 PlannerPhase 依次跑 5 个 program（HEP_DECORRELATE → HEP_FILTER_PUSH_DOWN
   → HEP_PROJECT_PUSH_DOWN → HEP_OPTIMIZE_JOIN_ORDER → OPTIMIZATION(CBO)，
   prepare/PlannerPhase.java:85-170）
→ new Splitter().go(igniteRel)   // 切 fragment
→ new QueryTemplate(fragments)   // 可跨查询复用的"计划模板"
→ MultiStepQueryPlan(plan 文本、模板、字段元数据、参数元数据)
```

`Splitter` 是一个 `IgniteRelShuttle`：自根向下走，**遇到 `IgniteExchange` 就切一刀**——原地把 exchange 替换成 `IgniteReceiver`（留在消费方 fragment），输入侧新起一个以 `IgniteSender` 为根的 fragment 压栈继续切（`prepare/Splitter.java:39-95`）。fragment 的根以 JSON 序列化（`RelJsonWriter`，`prepare/Fragment.java:78-84`）——**跨节点传的是 JSON 化的物理计划，不是 SQL**。

### 4.3 映射（执行期，绑定拓扑）

`executePlan` → `mapAndExecutePlan`（`exec/ExecutionServiceImpl.java:578-794`）先 `plan.init(mappingSvc, partSvc, mapCtx)`：`AbstractMultiStepPlan.init` → `QueryTemplate.map`（`prepare/QueryTemplate.java:64-88`）——把模板 fragment `attach` 到当前 cluster，逐个 `Fragment.map` 用 `IgniteMdFragmentMapping` 元数据自根向下推导 `FragmentMapping`（每个 fragment 落在哪些节点，`prepare/Fragment.java:141-192`；colocation 不满足时在切点重切/兜底，`QueryTemplate.java:99-115`），同时 `PartitionExtractor` 生成分区裁剪表达式树。产出 `ExecutionPlan`（fragments + 各自 mapping + PartitionNodes，`prepare/ExecutionPlan.java:37-48`）。

### 4.4 派发与执行（多阶段，但阶段数由计划决定）

- **本地 fragment 先跑**：第一个 fragment（根 fragment，含 `RootNode` 消费端）用 `LogicalRelImplementor` 把 Rel 树解释成执行 Node 树（`exec/ExecutionServiceImpl.java:646-649`），`RootQuery.run` 注册并等待所有远端 fragment（`RootQuery.java:215-243`）。
- **其余 fragment 逐节点派发**：对 mapping 中每个节点发一条 `QueryStartRequest`（queryId、schema、**fragment 的 JSON**、拓扑版本、`FragmentDescription`、参数、超时、事务条目，`ExecutionServiceImpl.java:657-701, 675-690`）。
- **数据节点**：`onMessage(QueryStartRequest)` 注册远端 `Query` → `fragmentPlanCache` 缓存 JSON→Rel 反序列化（`L868-907`，`RelJsonReader.fromJson`）→ `executeFragment`：`LogicalRelImplementor.go` 造出**以 `Outbox` 为根**的 Node 树并 `init()`，回 `QueryStartResponse`（`L839-865`）。发起端收齐所有 response 才放行游标（`RootQuery.onResponse`，`RootQuery.java:382-393`；节点离开/错误走 `CalciteErrorMessage` 与 `onNodeLeft`，`ExecutionServiceImpl.java:943-969`）。

### 4.5 数据回传：mailbox 与 batch 流控

- 每个跨节点 exchange = 源端 `Outbox` + 宿端 `Inbox`，由 `MailboxRegistry` 按 (queryId, fragmentId, exchangeId) 登记。`ExchangeServiceImpl.sendBatch` 把行打成 `QueryBatchMessage`（`exec/ExchangeServiceImpl.java:143-158`）；宿端 `Inbox.onBatchReceived` 喂数据（`exec/rel/Inbox.java:169`）；**流控**：批大小 `IO_BATCH_SIZE=256` 行、在途批数高水位 `IO_BATCH_CNT=4`，宿端回 `QueryBatchAcknowledgeMessage` 推进低水位（`exec/rel/AbstractNode.java:47, 50`；`exec/rel/Outbox.java:293-345`）。查询关闭/取消发 `QueryCloseMessage`/`InboxCloseMessage`。
- 消息层全部复用 core IO：`MessageServiceImpl.send` → `GridIoManager.sendToGridTopic(nodeId, GridTopic.TOPIC_QUERY, msg, CALLER_THREAD)`，本机则直接回调短路（`message/MessageServiceImpl.java:149-157`）；消息类型码 300-307（8 个 QUERY_* 消息）与 350-353（4 个内嵌结构：`FragmentMapping`/`ColocationGroup`/`FragmentDescription`/`QueryTxEntry`）由 `CalciteMessageFactory` 注册（`message/MessageType.java:43-76`、`message/CalciteMessageFactory.java:30-40`）。

### 4.6 与 H2 map-reduce 的结构差异

| 维度 | H2（GridMap/ReduceQueryExecutor） | calcite |
|---|---|---|
| 切分单位 | 两条 SQL 文本（map 查询 + reduce 查询，`GridSqlQuerySplitter` 拆 AST） | 物理计划 fragment 树（`Splitter` 按 `IgniteExchange` 切，任意层数） |
| 数据节点执行 | 再解析 map SQL、在本地 H2 上跑 `GridH2Table` | 反序列化 fragment JSON、`LogicalRelImplementor` 直接解释执行算子树 |
| 中间结果 | 写入 reduce 端 H2 merge table，排序归并 | `Inbox` 内存队列接 `Outbox` 流式批（256 行/批、4 批窗口） |
| 聚合位置 | reduce 查询（一条 SQL）完成最终聚合排序 | 根 fragment 的算子（HashAggregate/Sort）流式消费 |
| 消息通道 | 同一个 `GridTopic.TOPIC_QUERY`（`GridMapQueryExecutor.java:818`、`GridReduceQueryExecutor.java:285`），QUERY_POOL 策略 | 同一个 `TOPIC_QUERY`（`MessageServiceImpl.java:139, 155`），CALLER_THREAD 策略，独立消息类型码 |
| 阶段模型 | 固定两阶段 | 多阶段（每个 exchange 一跳），但仍然"先全部启动、按需拉数" |

---

## 5. 分级结论：集成层复刻的最小闭包

模仿 02 报告 §5 的分级。"集成层复刻"= **Apache Calcite 当黑盒依赖（Maven 引 calcite-core），只复刻 Ignite 侧代码**。

### 5.1 P0 最小闭包（缺一跑不通跨节点 SELECT）

| # | 组件 | 锚点 | 为什么不可缺 |
|---|---|---|---|
| 1 | `QueryEngine` 接口 + 引擎注册/选择 | `QueryEngine.java:28`；`IgniteComponentType.java:116-121`；`GridQueryProcessor.java:574-654, 3236-3268` | core 侧多引擎契约，`query()` 是唯一入口 |
| 2 | `CalciteQueryProcessor` 骨架 + service 组装/生命周期 | `CalciteQueryProcessor.java:282-318, 399-438` | 复刻课自己的 processor；13 个 service 可先瘦身（见 5.2） |
| 3 | `FrameworkConfig`（parser/validator/traitDefs/typeFactory） | `CalciteQueryProcessor.java:174-214` | calcite 库的全部接线，约 40 行但决定方言 |
| 4 | schema 映射：`SchemaHolder(Impl)` + `CacheTable(Impl)` + `CacheTableDescriptor(Impl)` + `colocationGroup`/`distribution` | `SchemaHolderImpl.java:68-196, 464-475`；`CacheTableDescriptorImpl.java:236-244, 486-585` | cache 变表、规划器要懂分布；复刻课可直接订阅自己课程里的 schema 事件 |
| 5 | 扫描数据访问：`TableScan`（分区 reserve + `CacheDataRow` 游标） | `exec/TableScan.java:44`；`CacheTableImpl.java:93-104` | SELECT 的数据来源 |
| 6 | 计划链：`PrepareServiceImpl.prepareSingle` → validate/optimize → `MultiStepPlan` | `PrepareServiceImpl.java:83-193` | SQL → 可执行计划 |
| 7 | fragment 切分与序列化：`Splitter` + `Fragment` + `RelJson*` | `Splitter.java:39-95`；`Fragment.java:51-146`；`externalize/RelJson*.java` | 跨节点传计划的前提 |
| 8 | fragment 映射：`Fragment.map` + `IgniteMdFragmentMapping` + `MappingService`/`AffinityService` | `Fragment.java:141-192`；`metadata/IgniteMdFragmentMapping.java` | "这个 fragment 去哪些节点跑" |
| 9 | 执行解释器 + 算子最小集：`LogicalRelImplementor` + Scan/Filter/Project/Sort/Aggregate/Join 各 1 种 + `RootNode` | `exec/LogicalRelImplementor.java:134-695`；`exec/rel/*` | 数据节点真跑起来 |
| 10 | exchange：`IgniteSender`/`IgniteReceiver`（rel）+ `Outbox`/`Inbox` + `MailboxRegistry` + `ExchangeServiceImpl` | `exec/rel/Outbox.java:41`、`Inbox.java:43`；`ExchangeServiceImpl.java:50-194` | 跨节点数据面 |
| 11 | 消息协议：`MessageServiceImpl`（TOPIC_QUERY）+ `MessageType` 全部 12 类 + `CalciteMessageFactory` | `MessageServiceImpl.java:138-157`；`MessageType.java:43-76` | 控制面与数据面共用通道 |
| 12 | 派发协议：`mapAndExecutePlan`/`executeFragment`/`onMessage(QueryStartRequest)` + `RootQuery`/`QueryRegistry` | `ExecutionServiceImpl.java:578-927`；`RootQuery.java:66-490` | 发起端↔数据节点的完整握手 |

（配套的 core 侧改动即 04 报告 §2.3 已列的 `initQueryEngines`/`engineForQuery`；复刻课若已有自己的 `GridQueryProcessor` 等价物，接入点相同。）

### 5.2 可简化项（依赖 calcite 默认行为，不必逐个复刻）

| 项 | 官方实现 | 简化路径 |
|---|---|---|
| planner 阶段与规则编排 | `PlannerPhase` 5 阶段 + rule 包 32 文件 | 先只保留 `OPTIMIZATION` 一阶段 + 最少 converter 规则（Scan/Filter/Project/Send-Receive）；HEP 各阶段与 join 顺序优化直接用 calcite 默认 HepProgram |
| cost/model metadata | `metadata/` 24 文件（行数/选择率/代价） | 只保留 `IgniteMdFragmentMapping`（映射必需）与粗粒度 rowCount；代价模型用 calcite 默认 `VolcanoCost` |
| trait 体系 | `trait/` 17 文件（Distribution/Rewindability/Correlation） | 最小只需 `DistributionTraitDef` + `IgniteDistributions.single/any`；Rewindability/Correlation trait 可退化为常量 |
| 表达式编译 | `exec/exp/` 31 文件（RexToLix/janino） | 用解释求值或 calcite 自带 `RexExecutor`（`RexExecutorImpl` 本来就是接 calcite 的 `DataContext`） |
| DDL/EXPLAIN/batch/hint/kill/statements | `prepare/ddl/`、`exec/ddl/`、`sql/` 43 文件、`hint/` | 课程首版只支持 SELECT/INSERT；DDL 走课程自己的 cache 配置 API |
| 索引扫描路径 | `IndexScan`/`bounds`/`ExposeIndexRule` | 首版全表扫描即可跑通；索引访问列为深挖 |
| 内存配额/超时/性能统计 | `exec/tracker/`、`TimeoutService`、perfStat 分支 | No-op 实现；接口留着 |
| striped executor 并发模型 | `exec/task/` | 单线程直接执行（`QueryBlockingTaskExecutor` 的退化版） |
| fragment 重切/TrimExchange/分区裁剪 | `QueryTemplate.java:99-115`、`IgniteTrimExchange`、`exec/partition/` | 首版"colocation 失败即报错"，不做运行时重切 |

### 5.3 深挖复刻（课后可选扩展）

1. **索引下推全链**：`ExposeIndexRule` + `prepare/bounds/`（SearchBounds 推导）+ `exec/IndexScan`（InlineIndex 范围游标、`IndexCountScan`/`IndexFirstLastScan` 的 MIN/MAX 优化）。
2. **colocation 与 MPP 调度**：`IgniteMdFragmentMapping` 的共置推导、`IgniteTrimExchange`（裁掉冗余交换）、`FragmentSplitter` 运行时重切、`exec/partition/` 分区裁剪。
3. **执行算子全家庭**：Sort/Hash 两种 aggregate、Merge/Hash/NL/Correlated 四种 join、三种 spool（Table/SortedIndex/HashIndex）、set-op、`exec/exp/agg` 的动态聚合 accumulator。
4. **流控与背压**：`Outbox` 高低水位、`Inbox` 多源对齐（`Aligner`）、批 ack 协议的乱序容忍（`Outbox.java:293-360`）。
5. **查询治理**：`QueryRegistry`/KILL、`TimeoutService`、两级内存配额（`QueryMemoryTracker`/`GlobalMemoryTracker`）、`StripedQueryTaskExecutor` 的防死锁条带。
6. **对库打补丁的姿势**：`org.apache.calcite.plan.volcano.VolcanoUtils`（同包名注入读私有字段）与 `CalciteQueryProcessor.java:136-150` 的断言关闭——集成层应对上游缺陷的两个真实样本。

---

## 引用文件清单（相对 `vendors/ignite/`，均已打开核验）

calcite 模块（前缀 `modules/calcite/src/main/java/`，`...` = `org/apache/ignite/internal/processors/query/calcite`）：

1. `.../CalciteQueryProcessor.java`（L135 类声明；L136-150 静态补丁；L164-171 executor 开关；L174-214 FRAMEWORK_CONFIG；L282-318 构造；L399-438 start/stop；L441-448 query；L536-599 parseAndProcessQuery；L729-806 processQuery；L834-847 onStart/onStop）
2. `org/apache/ignite/calcite/CalciteQueryEngineConfiguration.java`（L29 ENGINE_NAME；L51-55 engineClass）
3. `.../DistributedCalciteConfiguration.java`（L38-52 disabledRules）
4. `.../RootQuery.java`（L66 类；L215-243 run；L350 iterator；L365-393 onNodeLeft/onResponse）
5. `.../schema/SchemaHolderImpl.java`（L68 类；L169 注册监听；L178-196 schema/type 事件；L199-250 列增删；L253-261 createTable；L297-333 索引事件；L449-451 schema 查询；L464-475 rebuild）
6. `.../schema/CacheTableImpl.java`（L46 类；L69-71 getRowType；L93-104 scan；L107-114 distribution/colocationGroup；L117-134 indexes）
7. `.../schema/CacheTableDescriptorImpl.java`（L88/L118 字段与构造；L236-244 distribution；L486 rowType；L517-585 colocationGroup/partitionedGroup）
8. `.../schema/IgniteTable.java`（L36 接口；L53/L69-85 扩展方法）
9. `.../schema/IgniteIndex.java`、`.../schema/CacheIndexImpl.java`（L51 类；L93-103 scan→IndexScan+InlineIndex）
10. `.../prepare/PrepareServiceImpl.java`（L83-138 prepareSingle；L173-193 prepareQuery）
11. `.../prepare/PlannerPhase.java`（L85 枚举；L87-170 四个 HEP 阶段）
12. `.../prepare/Splitter.java`（L39 类；L47-72 go；L80-95 visit(IgniteExchange)）
13. `.../prepare/Fragment.java`（L51 类；L78-84 JSON 序列化；L122-124 rootFragment；L141-192 map）
14. `.../prepare/QueryTemplate.java`（L55-60 构造；L64-88 map）
15. `.../prepare/AbstractMultiStepPlan.java`（L79 起 init）
16. `.../prepare/MultiStepPlan.java`、`.../prepare/ExecutionPlan.java`（L37-48；L84-96 remotes）
17. `.../exec/ExecutionServiceImpl.java`（L129 类；L472-480 init；L510-540 executePlan；L578-794 mapAndExecutePlan；L646-649 本地执行；L657-701/L675-690 QueryStartRequest；L839-865 executeFragment；L868-927 远端 onRequest；L930-969 response/error/nodeLeft）
18. `.../exec/ExchangeServiceImpl.java`（L50 类；L143-168 sendBatch/acknowledge/closeInbox；L190-194 init 注册）
19. `.../exec/LogicalRelImplementor.java`（L134 类；L176-695 各 visit）
20. `.../exec/TableScan.java`（L44 类；分区 reserve 与 CacheDataRow 迭代）
21. `.../exec/IndexScan.java`（L59 类；import 区含 InlineIndex/InlineIndexTree/BPlusTree）
22. `.../exec/rel/AbstractNode.java`（L47 IO_BATCH_SIZE=256；L50 IO_BATCH_CNT=4）
23. `.../exec/rel/Outbox.java`（L41 类；L293-345 高低水位流控）、`.../exec/rel/Inbox.java`（L43 类；L169 onBatchReceived）
24. `.../message/MessageServiceImpl.java`（L45 类；L138-140 TOPIC_QUERY 监听；L149-157 send）
25. `.../message/MessageType.java`（L43-76 全部 12 个类型码）、`.../message/CalciteMessageFactory.java`（L30-40 registerAll）
26. `.../metadata/MappingServiceImpl.java`（L63-76 executionNodes）、`.../metadata/AffinityServiceImpl.java`（L51）
27. `modules/calcite/src/main/java/org/apache/calcite/plan/volcano/VolcanoUtils.java`（L23）
28. `modules/calcite/pom.xml`（L57-71 依赖）、`modules/calcite/README.txt`

core 模块（前缀 `modules/core/src/main/java/org/apache/ignite/`）：

29. `internal/IgniteComponentType.java`（L116-121 QUERY_ENGINE）
30. `internal/processors/query/QueryEngine.java`（L28 接口；L37-82 四入口）
31. `internal/processors/query/GridQueryProcessor.java`（L574-654 initQueryEngines；L3087-3126 引擎调用点；L3236-3268 engineForQuery）
32. `internal/processors/query/IndexingQueryEngine.java`（L23 标记接口）、`internal/processors/query/NoOpQueryEngine.java`
33. `internal/processors/query/schema/SchemaChangeListener.java`（L31 接口）
34. `internal/processors/query/schema/management/SchemaManager.java`（L381 onSqlTypeCreated 分发）
35. `internal/processors/subscription/GridInternalSubscriptionProcessor.java`（L85 registerSchemaChangeListener）
36. `spi/indexing/IndexingQueryFilterImpl.java`（calcite 主源码唯一复用的 indexing SPI）

H2 对照（前缀 `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/`）：

37. `twostep/GridMapQueryExecutor.java`（L818 TOPIC_QUERY）
38. `twostep/GridReduceQueryExecutor.java`（L285 TOPIC_QUERY）

包文件计数来自 `find modules/calcite/src/main/java -name '*.java'` 按目录 `uniq -c`；非 calcite ignite import 计数（222）来自全模块 import 语句去重统计。

---

## 对复刻课的启示

1. **"集成层"有清晰的三明治结构**：底层对 core 的钩子（QueryEngine/SchemaChangeListener/TOPIC_QUERY/InlineIndex）、顶层对 calcite 的接线（FrameworkConfig/convention/rules/metadata）、中间是自有的分布式执行内核（fragment/mailbox/算子）。复刻课可以按这三层排 lesson：先接线跑通单节点 SELECT，再加 fragment 派发跑通跨节点，最后补优化器细节。
2. **双引擎共享的东西比想象多**：TOPIC_QUERY 通道、InlineIndex 物理索引、SchemaManager 事件源、QueryUtils 类型描述。复刻课做 calcite 集成时，这些 core 资产应当已经在前面的持久化/索引课程里就位——集成层课不新建基础设施，只新增消费者。
3. **传计划而非传 SQL** 是 calcite 路线与 H2 路线的本质分水岭：数据节点收到的是 JSON 序列化的物理计划 + 参数，因此 `RelJson*` 序列化兼容性（calcite 版本升级）是集成层的长期负担，也是课程里值得单独一讲的设计权衡。
4. **流控协议很小但关键**：256 行/批、4 批在途窗口 + ack，三常量撑起背压。课程实现可以先做成"无窗口全量 push"，再把窗口作为独立的进阶 lesson，性能对比直观。
5. **库不是完美黑盒**：`VolcanoUtils` 同包注入读私有字段、关闭上游类断言，都是集成现实工程里"绕过库缺陷"的样本；复刻课遇到类似问题时这两招可直接引用。
