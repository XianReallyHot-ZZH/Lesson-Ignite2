# SQL 进阶：DML/DDL 全链、分布式 join 与查询治理（Apache Ignite 2.18.0 源码调研 · 14 号）

> Primary source：本仓库 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码 submodule，只读）。
> 文中所有源码路径均相对 `vendors/ignite/`，全部经实际打开/检索验证存在；每条 `文件:行号` 引用均现场核验。类名、方法名以 2.18 源码为准。
> 边界：04 号报告已验收的结论（H2 装配、schema/表/索引映射、查询切分主算法 splitSelect、两阶段执行主流程）直接引用不重查；06 号 §3（SchemaManager 事件源）、13 号 §1.6（索引树 vs 数据树）同样按已验收引用。

---

## 0. 范围与两个先决纠偏

本报告覆盖 04/06/13 未覆盖的 indexing 内部：DML 全链（dml 包）、DDL 全链（CommandProcessor / core 的 SqlCommandProcessor）、分布式 join 两条路径与查询进阶边界、查询治理与周边（cancel/timeout/metrics/注解/Lucene/geospatial），最后给出章 8 切课建议。

**先决纠偏（grep 全库核验）**：

1. **`LazyQueryList` 不存在**——2.18 中 lazy 模式不是独立结果集类，而是 map/reduce 两侧的流式协议标志（`GridH2QueryRequest.FLAG_LAZY`，见 §3.7）。
2. **geospatial 实现类不在源码树里**——`org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex` 与 `GeoSpatialUtils` 通过反射按名加载（`H2Utils.java:141-146`），但 grep 全仓库零命中：它们在独立的可选 artifact 中（见 §4.7）。

`modules/indexing/.../query/h2/dml/` 包共 16 个类 + package-info，合计 3833 行（`wc -l` 实测），最大三类是 `UpdatePlanBuilder`（932）、`DmlAstUtils`（635）、`DmlUtils`（584）。

---

## 1. DML 全链

一张图看全链（编号对应下文小节）：

```
SqlFieldsQuery(INSERT/MERGE/UPDATE/DELETE)
  → QueryParser.prepareDmlStatement          (§1.1 解析, H2 AST → GridSqlStatement)
  → UpdatePlanBuilder.planForStatement       (§1.1 计划, DmlAstUtils 改写成 SELECT)
  → IgniteH2Indexing.executeDml              (§1.2 三支路: fast / distributed / select+写回)
      ├─ FastUpdate.execute → cache.replace/remove           (§1.1 快路径)
      ├─ rdcQryExec.update → GridH2DmlRequest ──TOPIC_QUERY──► map 节点本地执行 (§1.4)
      └─ 两阶段 SELECT 游标 → DmlUtils.processSelectResult   (§1.3 写回)
           ├─ put / putIfAbsent / putAll                    (INSERT 单行 / MERGE)
           └─ DmlBatchSender → invokeAll(Insert|Modifying EntryProcessor)  (§1.3 汇合点)
```

### 1.1 解析与计划：QueryParser → UpdatePlanBuilder → DmlAstUtils

1. `QueryParser.parse` 用 `GridSqlQueryParser.isDml(prepared)` 识别 DML（`QueryParser.java:430-431`），调 `prepareDmlStatement`（`QueryParser.java:557-602`）：先把 H2 `Prepared` 转成 Ignite AST（`GridSqlStatement`），对语句涉及的每个 `GridH2Table` 做 `H2Utils.checkAndStartNotStartedCache`（client 节点上惰性拉起 cache）；若 INSERT 可流式（`isStreamableInsertStatement`），额外记下 `streamTbl`（`QueryParser.java:575-580`）。
2. `UpdatePlanBuilder.planForStatement`（`UpdatePlanBuilder.java:102-116`）按语句类型分派：`GridSqlMerge`/`GridSqlInsert` → `planForInsert`（`:128`），`GridSqlUpdate`/`GridSqlDelete` → `planForUpdate`（`:387`）。产物是 **`UpdatePlan`**——携带 `UpdateMode`（枚举 MERGE/INSERT/UPDATE/DELETE/BULK_LOAD，`UpdateMode.java:23-37`）、目标 `GridH2Table`、派生 SELECT SQL、`KeyValueSupplier`（把 SELECT 行组装回 key/value 对）、`FastUpdate`（快路径参数）、`DmlDistributedPlanInfo`。
3. **DML → SELECT 改写是核心技巧**，全部在 `DmlAstUtils`：
   - `selectForDelete`：生成 `SELECT _KEY, _VAL FROM tbl WHERE ... [LIMIT n]`（`DmlAstUtils.java:146-182`）；
   - `selectForUpdate`：生成 `SELECT _KEY, _VAL, "_upd_c1", "_upd_c2"... FROM tbl WHERE ...`——每个被 SET 的列变成别名 `_upd_列名` 的额外投影列（`DmlAstUtils.java:350-361`），新值表达式由 map 查询求值；
   - `selectForInsertOrMerge`：把 `INSERT INTO t(...) SELECT ...` 的子查询补齐缺列后原样返回（`DmlAstUtils.java:91`）。
4. **快路径（无查询 DML）**：`WHERE _key = ?`（可再 AND `_val = ?`）且 SET 单列直值/参数时，`getFastUpdateArgs`/`getFastDeleteArgs`（`DmlAstUtils.java:189-225`，判定逻辑 `findKeyValueEqualityCondition` `:233-272`）产出 `FastUpdate`，执行时直接调 `cache.replace(key, oldVal, newVal)` / `cache.remove(key[, val])`（`FastUpdate.java:77-103`）——完全绕过 SELECT。
5. **`KeyValueSupplier`** 决定"SELECT 行 → 新 value 对象"的组装方式：更新整行 `_VAL` 时是 `PlainValueSupplier`（直接取行内指定列，`UpdatePlanBuilder.java:914+` 私有实现）；只更新部分字段时走 `createSupplier` 的属性合并分支（`UpdatePlanBuilder.java:474-475` 构造点），即"读旧值、克隆、覆盖被改属性"。

### 1.2 执行总入口：executeDml 的三条支路

`IgniteH2Indexing.queryDistributed... → executeDml`（`IgniteH2Indexing.java:1041-1128`）注册 running query 后：

- **事务门卫**：`if (!updateInTxAllowed && ctx.cache().context().tm().inUserTx()) throw`（`IgniteH2Indexing.java:1056-1060`）——默认（`IGNITE_ALLOW_DML_INSIDE_TRANSACTION` 未设，`:176-177`）**显式用户事务内禁止一切 SQL DML**，报错文案点名 TRANSACTIONAL 原子模式。事务外执行时，ATOMIC cache 的每个 cache 操作自含微事务（章 1/12 已建立的隐式 tx），DML 本身不再包一层（见 §1.5）。
- 之后 `executeUpdateDistributed`（`:1953`）处理 JDBC batch（多组参数逐条或 `plan.createRows` 批量），单条落到 `executeUpdate`（`:2080`）：**失败键重跑循环，默认 4 次**（`DFLT_UPDATE_RERUN_ATTEMPTS = 4`，`:173`；循环体 `:2097-2124`）。
- `executeUpdate0`（`:2149-2295`）按序尝试三条支路：
  1. `plan.processFast(args)` 快路径命中即返回（`:2163-2169`）；
  2. **分布式 DML**：`plan.distributedPlan() != null` 且非本地时，走 `rdcQryExec.update(...)`（`:2171-2197`，详见 §1.4）；返回 null（集群有 <2.3 节点）则回退；
  3. **SELECT + 写回**：构造派生 `SqlFieldsQuery(plan.selectQuery())`（flags 透传 distributedJoins/enforceJoinOrder/timeout/pageSize；**lazy 受限**：UPDATE 改写列出现在 WHERE 时强制非 lazy，防止索引扫描下同条目被更新多次——注释原文即 `UPDATE test SET val = val + 1 WHERE val >= ?` 例子，`IgniteH2Indexing.java:2211-2215`，判定源 `DmlAstUtils.selectForUpdate` 的 `canBeLazy`，`DmlAstUtils.java:365-380`）。非本地且非本地子查询 → `executeSelectForDml` 走 04 §2.2 的两阶段查询；`plan.hasRows()`（字面量 VALUES 行）→ `plan.createRows` 不执行任何 SELECT；否则本地执行。最终把游标喂给 `DmlUtils.processSelectResult`（`:2277`）。

### 1.3 与 cache 写路径的汇合点（逐模式核验）

`DmlUtils.processSelectResult`（`DmlUtils.java:166-185`）按 `UpdateMode` 分派，汇合点各不相同（均经 `QueryUtils.cacheForDML` 包装——read-through 开启时加 `withSkipValues`/skip 语义避免 DML 触发 store 读，`QueryUtils.java:1796-1801`）：

| 模式 | 单行快路径 | 批量路径 |
|---|---|---|
| INSERT | `cache.putIfAbsent(key, val)`（`DmlUtils.java:209`），重复键抛 DUPLICATE_KEY | `DmlBatchSender.add(key, new InsertEntryProcessor(val), 0)`（`:222`） |
| MERGE | `cache.put(key, val)`（`:319`） | 按 `updateBatchSize` 分页 `cache.putAll(rows)`（`:327-349`，落点 `:342`） |
| UPDATE | 无（除非 FastUpdate 的 `replace`） | `DmlBatchSender.add(key, new ModifyingEntryProcessor(oldVal, new EntryValueUpdater(newVal)), 0)`（`:260-271`） |
| DELETE | 无（除非 FastUpdate 的 `remove`） | `DmlBatchSender.add(key, new ModifyingEntryProcessor(val, RMV), 0)`（`:368-380`） |

**`DmlStatementsProcessor`（indexing 包，127 行）就是 EntryProcessor 的家**（类注释明言"Contains entry processors for DML"，`DmlStatementsProcessor.java:26-31`）：

- `InsertEntryProcessor`：`entry.exists()` 则返回 false（记为失败键），否则 `entry.setValue(val)`（`:33-51`）——INSERT 语义 = putIfAbsent 的 EntryProcessor 版；
- `ModifyingEntryProcessor`：**先比较后改**——`!Objects.equals(entryVal, val)`（map-reduce 期间值被别人改过）返回 false，否则应用 `entryModifier`（`:56-90`，比较点 `:83`）。这不是行级锁，是**乐观 CAS 语义**：比较基准是 SELECT 阶段读到的 `_VAL`；
- 修饰闭包两个：`EntryValueUpdater`（`e.setValue(newVal)`，`:106-125`）与 `RMV`（`e.remove()`，`:93-101`）。

**`DmlBatchSender`（454 行）是批量写回器**：`add` 按 `cctx.affinity().primaryByKey(key, NONE)` 把键分桶到主节点（`DmlBatchSender.java:105-145`），攒够 `updateBatchSize` 或遇重复键即 `sendBatch` → `processPage` → **`cacheForDML(cctx.cache()).invokeAll(batch.rowProcessors())`**（`:237-241`，invokeAll 落点 `:241`）——这就是 DML 与 cache 写路径的最终汇合：**invokeAll(EntryProcessor)**。返回的 `EntryProcessorResult` 里 false 即"并发修改/重复键"，收集进 `failedKeys`（`:244-259`+），由 §1.2 的 4 次重跑循环消费，重跑仍失败则抛 CONCURRENT_UPDATE 异常（`DmlUtils.java:279-292`）。

### 1.4 分布式 DML（skipReducerOnUpdate 优化）与消息

默认 DML 走"两阶段 SELECT 拉回发起节点 + 本地 invokeAll"。`SqlFieldsQueryEx.setSkipReducerOnUpdate(true)`（JDBC thin 专用开关，`SqlFieldsQueryEx.java:37,149-151`）启用**服务端 DML**：`UpdatePlanBuilder.checkPlanCanBeDistributed`（`UpdatePlanBuilder.java:854-909`）把派生 SELECT 再过一遍 `GridSqlQuerySplitter.split`，仅当 **非本地切分 + 有真实 cache + skipMergeTable + 恰一个 map 查询且无子查询**（`:889-893`）才产出 `DmlDistributedPlanInfo(replicatedOnly, cacheIds)`（`:900`，类定义 `DmlDistributedPlanInfo.java:25-56`）。

执行链与消息（全部核验）：

1. 发起节点 `GridReduceQueryExecutor.update`（`GridReduceQueryExecutor.java:912-1013`）：`mapper.nodesForPartitions` 选节点（replicatedOnly 时收敛到本节点或任一数据节点，`:935-942`；任一节点版本 <2.3.0 则整体回退返回 null，`:944-951`），组装 **`GridH2DmlRequest`**（requestId/topologyVersion/caches/schema/query/参数/timeout/flags，`:962-972`；flags 含 `FLAG_ENFORCE_JOIN_ORDER` 与 replicatedOnly 的 `FLAG_REPLICATED`，`:957-960`）经 `GridTopic.TOPIC_QUERY` 发出；取消时对参与节点补发 `GridQueryCancelRequest`（`:987-991`）；
2. 数据节点 `GridMapQueryExecutor.onDmlRequest`（`GridMapQueryExecutor.java:674-781`）：`reservePartitions` 预约分区（`:693`）→ 组装 local `SqlFieldsQuery`（queryParallelism>1 时开 distributedJoins，`:732-737`）→ **`h2.executeUpdateOnDataNode(...)`**（`:739`；实现即重新 parse + 本地 `executeUpdate`，`IgniteH2Indexing.java:1471-1495`——也就是说 map 节点在本地把"SELECT + EntryProcessor 写回"整段做掉）；
3. 应答 **`GridH2DmlResponse`**（updateCounter/errorKeys/error）回发起节点，`onDmlResponse`（`GridReduceQueryExecutor.java:1021-1040`）喂给 `DmlDistributedUpdateRun`（`DmlDistributedUpdateRun.java:34-119`）：聚合各节点计数与失败键，收齐 `nodeCount` 份后完成 future；失败键同样交回 §1.2 的重跑循环（此时重跑走完整路径而非分布式路径）。

### 1.5 并发与事务语义小结

- DML **不是**一个分布式事务：每个 `invokeAll` 页/每次 `putAll` 页独立原子；页间失败会留下部分效果，错误以"失败键集合 + 异常链"上报（`BatchUpdateException` 用于 JDBC batch，`DmlUtils.java:500-505`）。
- UPDATE/DELETE 的正确性靠 `ModifyingEntryProcessor` 的值比较 + 发起端 4 次重跑，而不是锁。
- 显式事务内默认直接拒绝（§1.2）；`IGNITE_ALLOW_DML_INSIDE_TRANSACTION=true` 放开后，ATOMIC cache 仍无跨语句原子性。

### 1.6 BULK LOAD（COPY 命令）

`COPY` 是 native 语法（非 H2），`CommandProcessor.processBulkLoadCommand`（indexing `CommandProcessor.java:633-663`）：`UpdatePlanBuilder.planForBulkLoad(cmd, tbl)` 造计划（列/类型来自 CSV 列定义，`UpdatePlanBuilder.java:547+`）→ `DmlBulkLoadDataConverter`（复用 `plan.processRow` 做 行→k/v 转换，`DmlBulkLoadDataConverter.java:28-51`）→ **`ctx.grid().dataStreamer(tbl.cacheName())`** + `BulkLoadStreamerWriter`（`:651-653`）→ CSV 解析与流水线在 core 的 `internal/processors/bulkload/`（`BulkLoadProcessor`/`BulkLoadCsvParser`/pipeline blocks，共约 1270 行）。注意 `unregister=false`（命令游标保持打开等客户端分批 ack，`CommandProcessor.java:153`）——**COPY 的文件内容来自客户端逐包推送**（`BulkLoadAckClientParameters` 协议）。流式 INSERT（JDBC `SET STREAMING`/`SqlSetStreamingCommand`）另有一条 `isStreamableInsertStatement` 判定链（`IgniteH2Indexing.java:1498-1502`）。

---

## 2. DDL 全链

### 2.1 处理器分工（三层）

- **indexing `CommandProcessor extends SqlCommandProcessor`**（"responsible for execution of all non-SELECT and non-DML commands"，`CommandProcessor.java:94-97`）：`runCommand`（`:132-166`）先把可转换的 H2 命令转 native（CREATE/DROP INDEX → `SqlCreateIndexCommand`/`SqlDropIndexCommand`，`:204-231`），native 命令优先走父类，不认识才落到自己的 `runCommandH2`（H2 AST 路径）。
- **core `SqlCommandProcessor`**（`modules/core/.../internal/sql/SqlCommandProcessor.java`）：native 命令总分发 `runCommand`（`:104-131`）——DDL 走 `runCommandNativeDdl`（`:310+`），其余是 KILL 家族/统计命令（`:109-128`）。`isCommandSupported` 列出支持面（`:136-153`）：CREATE/DROP INDEX、ALTER TABLE、CREATE/ALTER/DROP USER、KILL×6、统计三命令、CREATE/DROP VIEW。
- H2 AST 侧 `CommandProcessor.isCommand`（indexing `CommandProcessor.java:570-573`）：仅 `CreateIndex|DropIndex|CreateTable|DropTable|AlterTableAlterColumn`。

### 2.2 CREATE TABLE：SQL → QueryEntity → 动态 cache

`runCommandH2` 的 `GridSqlCreateTable` 分支（indexing `CommandProcessor.java:243-298`）：

1. `toQueryEntity(cmd)`（`:449-564`）把 SQL 表定义翻译成 **`QueryEntityEx`**：列类型（含 DECIMAL 精度/scale、VARCHAR 长度→`fieldsPrecision/Scale`，`:461-494`）、NOT NULL 集合、默认值、key/value 类型名（`QueryUtils.createTableValueTypeName(schema, table)` 合成二进制类型名，`:505-506`）、`WRAP_KEY/WRAP_VALUE` 选项决定 keyFieldName/keyFields（`:517-528`）、`fillAbsentPKsWithDefaults(true)` 与主键/亲和列 inline size（`:555-561`）。解析侧约束：默认模板 `TEMPLATE_PARTITIONED`（`GridSqlQueryParser.java:1098`）、必须有且仅有 PRIMARY KEY 约束（`:1107-1131`）、**`CREATE TABLE ... AS SELECT` 显式不支持**（`:1100-1105`）。
2. 目标 cache 已存在（`WITH "cache=..."`）→ `dynamicAddQueryEntity`（`:271-279`）；否则 **`dynamicTableCreate`**（`:280-296`）。后者在 `GridQueryProcessor.dynamicTableCreate`（`GridQueryProcessor.java:2237-2326`）：从模板取 `CacheConfiguration`（PARTITIONED/REPLICATED 模板或用户模板，`:2256-2267`），逐项落 WITH 参数（cacheGroup/dataRegion/affinityKey→`CacheKeyConfiguration`/atomicityMode/writeSync/backups/encrypted/parallelism，`:2281-2310`），最后 **`ctx.grid().getOrCreateCache0(ccfg, true)`**（`:2315`）。
3. `getOrCreateCache0` 内部最终 `ctx.discovery().sendCustomEvent(new DynamicCacheChangeBatch(sndReqs))`（`GridCacheProcessor.java:4128`）——**CREATE TABLE 就是动态 cache 创建**，走 PME/exchange 全流程（章 4 已建概念）；各节点 `GridCacheProcessor.onCustomEvent → ctx.query().onCacheChangeRequested`（`GridCacheProcessor.java:4255-4258`）触发 SchemaManager 的类型注册（06 §3 的事件源），H2 侧 `H2SchemaManager.onSqlTypeCreated` 建表（`H2SchemaManager.java:135-151`，04 §2.1 已验收）。
4. **DROP TABLE = 销毁 cache**：`dynamicTableDrop` → `ctx.grid().destroyCache0(cacheName, true)`（`GridQueryProcessor.java:2336-2348`，且禁止在目标 cache 自身上执行 DROP，`:2339-2342`）。cache 名与表名 1:1（`QueryUtils.createTableCacheName`，`:2277`）。

### 2.3 CREATE/DROP INDEX 与 ALTER TABLE ADD/DROP COLUMN

- CREATE INDEX（native 或 H2 转换而来）→ core `runCommandNativeDdl` 组 `QueryIndex`（spatial 参数映射 `QueryIndexType.GEOSPATIAL`，列升降序、inlineSize，`SqlCommandProcessor.java:316-349`）→ **`ctx.query().dynamicIndexCreate(cacheName, schema, tbl, idx, ifNotExists, parallel)`**（`GridQueryProcessor.java:3462-3468`）。DROP INDEX 同理 `dynamicIndexDrop`（core `SqlCommandProcessor.java:350-369`；系统索引 PK/affinity/proxy 禁删，`:356`）。DROP INDEX 对不存在索引 + ifExists → 空完成 future。
- ALTER TABLE ADD/DROP COLUMN（H2 AST 路径）：indexing `CommandProcessor.java:317-371 / 372-425` 做列校验（`WRAP_VALUE=false` 的 SQL 类型表禁改列，`:330-332`；NOT NULL 新列要求 cache 配置允许，`:364-365`）→ `dynamicColumnAdd` / `dynamicColumnRemove`（`GridQueryProcessor.java:3495-3502 / 3513-3520`）。
- 四个 dynamic* 的公共出口是 **`startIndexOperationDistributed`**：构造 `SchemaOperationClientFuture` 并 **`ctx.discovery().sendCustomEvent(new SchemaProposeDiscoveryMessage(op))`**（`GridQueryProcessor.java:3564-3601`，发送点 `:3572`）——这是与 DynamicCacheChangeBatch 平行的第二条 discovery 协议（propose → 各节点执行 → finish 消息完成 future；类型注册/索引重建的事件分发即 06 §3 的 `SchemaManager` 监听器面，包括 `onIndexCreated/Rebuilt` 等）。`dynamicAddQueryEntity`（`GridQueryProcessor.java:3531-3556`）也走同一协议（`SchemaAddQueryEntityOperation`）。
- 其余 ALTER 面：`SqlAlterTableCommand`（native 路径）实际只服务 logging 相关 ALTER（core `SqlCommandProcessor.java:370-374` 起的分支）；`ALTER TABLE ... RENAME`、`TRUNCATE` 等不在支持面（isCommandSupported 清单为准）。

### 2.4 ALTER 语句支持面汇总

| 语句 | 路径 | 终点 |
|---|---|---|
| CREATE TABLE | H2 AST | `dynamicTableCreate` → `getOrCreateCache0` → `DynamicCacheChangeBatch` → PME |
| DROP TABLE | H2 AST | `destroyCache0` → 同上 |
| CREATE/DROP INDEX | native（H2 可转换） | `dynamicIndexCreate/Drop` → `SchemaProposeDiscoveryMessage` |
| ALTER TABLE ADD/DROP COLUMN | H2 AST | `dynamicColumnAdd/Remove` → 同上 |
| CREATE/DROP/ALTER USER、CREATE/DROP VIEW、KILL×6、统计命令 | native（core parser） | `SqlCommandProcessor.runCommand` 各分支 |
| CREATE TABLE ... AS SELECT | — | 解析期拒绝（`GridSqlQueryParser.java:1102-1105`） |

---

## 3. 分布式 join 与查询进阶

### 3.1 colocated vs non-colocated：两条路径的判定与执行

**判定端（优化器钩子）**：H2 代价优化时对每个 `GridH2IndexBase` 调 `getDistributedMultiplier`（`GridH2IndexBase.java:112-116`）→ `CollocationModel.distributedMultiplier`（`CollocationModel.java:635-651`）：仅当查询开启 distributedJoins（`SplitterContext.distributedJoins()`）且非 prepare 阶段才建 **`CollocationModel`**（join 序列上每表一个节点，类型 `CollocationModelType` PARTITIONED/REPLICATED/COLLOCATED，乘数 `CollocationModelMultiplier` COLLOCATED/AFFINITY/REPLICATED_NOT_LAST）。表序列按"affinity 条件是否与前置表对齐"逐表降级：完全对齐 → COLLOCATED（map 本地 join，reduce 直取）；含 affinity 等值条件 → 单键路由；否则 AFFINITY（走分布式查找）。**replicated 表约束**：distributed join 中 replicated cache 必须排在 join 序列末尾，否则报错（`isCollocated` 校验，`CollocationModel.java:723-733`）。

**执行端（map 内拉取）**：non-colocated join 不在 reduce 侧重组，而在 **map 节点执行本地 join 时按批远程查对方索引**：`H2TreeIndex` 挂 `DistributedLookupBatch implements IndexLookupBatch`（`DistributedLookupBatch.java:52-90`，构造参数 `ucast`/`affColId`）。`addSearchRows` 攒一批 join 查找区间，`getAffinityKey` 尝试从区间上下界提取亲和键（可从显式 affinity 列或从 `_KEY` 经 `cctx.affinity().affinityKey(pk)` 推导，`:102-131`）：

- **提取得出 → unicast**：`rangeSegment(affKey)` 定位单个 (node, segment)，只发一个节点（`:136-141` 附近逻辑）；
- **提取不出 → broadcast**：`broadcastSegments()` 对全部节点×segment 发请求。

请求消息 **`GridH2IndexRangeRequest`**（bounds 内含序列化的上下界 SearchRow），对端 `H2TreeIndex.onIndexRangeRequest`（消息经索引自身监听的 TOPIC_QUERY 子通道进 `onMessage0`，`H2TreeIndex.java:445-463`，处理 `:469+`）：在本节点索引上跑 `RangeSource`，把命中行序列化成 `GridH2IndexRangeResponse`（分页 `GridH2RowRange`）回发；发起端 `onIndexRangeResponse`（`:579`）喂 `RangeStream`，游标由 `UnicastCursor`/`BroadcastCursor`（`opt/join/` 包）包装进本地 join。**reduce 侧不需要知道 join 的存在**——reduce 只见 merge table（04 §2.2 主流程不变）。这就是"affinity key 对齐 vs 广播"的真实含义：对齐时 unicast、不对齐时 map 侧逐批广播， replicated 表则因每个 map 节点都有全量数据而永远"本地"。

### 3.2 replicatedOnly 优化

切分阶段统计 `replicatedOnly = mapSqlQrys.stream().noneMatch(isPartitioned)`（`GridSqlQuerySplitter.java:319`，透传到 `GridCacheTwoStepQuery` 字段 `:53`）。reduce 端 `createMapping`：`qry.explain() || qry.isReplicatedOnly()` 时把参与节点收敛为**单个**（优先本节点，否则任一数据节点，`GridReduceQueryExecutor.java:878-891`）——replicated-only 查询退化为"挑一个节点本地跑"。分布式 DML 走同款收敛（§1.4 的 `update` 重复实现了一份，`GridReduceQueryExecutor.java:926-942`）。两个例外让 replicated 表被当作 partitioned 处理：外连接混合 cache 模式、replicated 表与带子查询的 partitioned 表同现（`treatReplicatedAsPartitioned`，`GridSqlQuerySplitter.java:1285-1287`；map 端对应 `FLAG_REPLICATED_AS_PARTITIONED`，`GridMapQueryExecutor.java:218`）。

### 3.3 分区裁剪（04 未覆盖的 derivePartitions 链）

`splitMapQuery` 末尾：`if (map.isPartitioned() && canExtractPartitions) map.derivedPartitions(extractor.extract(mapQry))`（`GridSqlQuerySplitter.java:1289-1290`）。**`PartitionExtractor`**（`affinity/PartitionExtractor.java`）：只处理单 SELECT（**UNION 不支持裁剪**，`:103-105`），对 WHERE 建分区代数树（`PartitionNode`，AND/OR/composite，`:107-117`），优化后若是 `PartitionAllNode`（全分区）则放弃。多个 map 查询的分区树用 `mergeMapQueries` OR 合并，亲和组不兼容则整体放弃回退广播（`:131-190`）。结果交给 `QueryMapper.nodesForPartitions`（`GridReduceQueryExecutor.createMapping:870-871` 引用）生成 node→partition 映射，随 `GridH2QueryRequest.partitions` 下发（`:437`），map 端据此只预约/扫描归属分区。

### 3.4 UNION / 子查询 / CTE

- **UNION**：`splitQuery` 先把整个查询（含 union）建成 `SplitterQueryModel` 树，需要拆子查询时对 UNION 每个分支递归 `pushDownQueryModel`（`GridSqlQuerySplitter.java:395-409`）——即 **union 的每个分支独立切分成自己的 map 查询，共享一个 reduce**；分区信息按 §3.3 合并。
- **子查询**：切分前 `GridSubqueryJoinOptimizer.pullOutSubQueries(selectStmt)`（`QueryParser.java:481`）把可上拉的子查询改写成 join，让 CollocationModel 能统一分析（类即 `GridSubqueryJoinOptimizer.java:58`）。
- **CTE**：非递归 WITH 由 H2 内联成 `TableView` → 解析为 `GridSqlSubquery`（`GridSqlQueryParser.java:685-694`）；**递归 CTE 显式抛错不支持**（`:686-688`）。

### 3.5 ORDER BY / LIMIT / OFFSET / DISTINCT 的 map/reduce 分界（splitSelect 边界场景，04 主算法之外）

全部在 `GridSqlQuerySplitter.splitSelect` 后半（`GridSqlQuerySplitter.java:1228-1262`）：

- **ORDER BY**：reduce 恒保留排序列；map 侧仅在**无聚合**时保留排序（有聚合时 `mapQry.clearSort()`，`:1228-1236`）——map 排序服务于归并顺序，聚合场景下归并由 reduce 做。join 子查询被强制按 join 条件排序另算（`setupMergeJoinSorting`，`:369`）。
- **LIMIT**：reduce 恒有；map 侧仅无聚合时保留（`:1238-1246`）；collocatedGrpBy=true 时 map 保留（同段注释）。
- **OFFSET**：reduce 恒有；map 的 limit 改写为 `offset + limit`（`:1248-1254`）。
- **DISTINCT**：reduce 恒 distinct；map 侧只在**无聚合且无 GROUP BY 且无 HAVING** 时保留 distinct（`:1258-1262`）。
- HAVING：无 collocatedGrpBy 时下推到 map 的部分聚合列、reduce 重挂（`:1213-1226`）；含 distinct 聚合时 map 侧连 GROUP BY 都要撤掉（`:1204-1210`）——因为 `COUNT(DISTINCT x)` 无法两段式合并，分组必须整体推迟到 reduce。
- 递归拆分上限：`splitId > 99` 即抛 "Too complex query to process."（`:1152-1153`）——多层子查询嵌套时每层各占一个 merge table 编号。

### 3.6 EXPLAIN 输出

reduce 端 `explainPlan`（`GridReduceQueryExecutor.java:1141-1179`）：对每个 map 查询 `SELECT PLAN FROM mergeTableIdentifier(i)` 取该 merge 表内缓存的 map 计划（map 节点执行 EXPLAIN 请求时把本地 EXPLAIN 计划写进 merge table），再本地 `EXPLAIN <reduce query>`（`:1169-1174`），返回 = 每个 map 查询一行 + reduce 一行。EXPLAIN 查询本身在 `createMapping` 也收敛到单节点（§3.2）。`qry.explain()` 由切分入口提取并传入 two-step 查询（`GridSqlQuerySplitter.java:264-266,332`）。

### 3.7 lazy 查询模式（LazyQueryList 不存在的真相）

`SqlFieldsQuery.lazy` 默认 **true**（`DFLT_LAZY = true`，2.8 起，`SqlFieldsQuery.java:62,85-90`）。flag 经 `queryFlags(...)` 打进 `GridH2QueryRequest.FLAG_LAZY`（`GridH2QueryRequest.java:83,457-477`；组装点 `GridReduceQueryExecutor.java:440`）。语义在两侧：

- **map 侧**：查询执行后只准备**第一页**发回（`prepareNextPage` + `sendNextPage`，`GridMapQueryExecutor.java:540-556`）；`!lazy` 时发完首页即 `qryResults.releaseQueryContext()`（释放 H2 会话与分区预约，`:582-583`）——因为非 lazy 下 reducer 会立刻把剩余页全部拉完；lazy 时上下文保留到全部结果页取完才释放（`prepareNextPage` 内 `isAllClosed() && isLazy()` 才 release，`GridMapQueryExecutor.java` prepareNextPage 段）。行按页从本地 H2 `ResultSet` 流式读（`MapQueryResult.fetchNextPage`，`MapQueryResult.java:181-220`）。
- **reduce 侧**：`awaitAllReplies` 只等每个 map 查询的**首页**（`onFirstPage` 计数，`GridReduceQueryExecutor.java:307-308`），随后即返回游标 `H2FieldsIterator`（`:560-568`）；后续页由本地 merge table 读缺页时经 `ReduceResultPage.fetchNextPage` 回发 `GridQueryNextPageRequest` 拉取（`:279-289`）。DML 内 lazy 的限制见 §1.2。

### 3.8 SqlFieldsQuery 参数化与 flags 汇总

参数化：H2 `PreparedStatement` 参数索引由 `setupParameters` 收集（map/reduce 各自的 `parameterIndexes`，`GridSqlQuerySplitter.java:1300-1313`）。flags 落点（字段声明 `SqlFieldsQuery.java:72-90`）：

| flag | 默认 | 作用点 |
|---|---|---|
| `collocated` | false | `split` 的 `collocatedGrpBy`：跳过聚合/HAVING 拆分（§3.5） |
| `distributedJoins` | false | CollocationModel 降级与 DistributedLookupBatch 挂载（§3.1） |
| `enforceJoinOrder` | false | H2 侧禁 join 顺序优化；随 request flag 下发（`GridReduceQueryExecutor.java:957,440`） |
| `replicatedOnly` | false | 手动声明 replicated-only（与 §3.2 自动判定叠加） |
| `lazy` | true | §3.7 |
| `skipReducerOnUpdate`（仅 `SqlFieldsQueryEx`） | false | §1.4 服务端 DML |
| `parts`/`timeout`/`pageSize`/`maxRows`/`dataPageScanEnabled` | — | §3.3/§4.2/分页 |

---

## 4. 查询治理与周边

### 4.1 取消链（KILL QUERY → 执行点）

1. `KILL QUERY 'nodeId qryId'` 由 native parser 产 `SqlKillQueryCommand` → core `processKillQueryCommand` → **`ctx.query().runningQueryManager().cancelQuery(qryId, nodeId, async)`**（core `SqlCommandProcessor.java:121-122,175-177`）。
2. `RunningQueryManager.cancelQuery` 组 **`GridQueryKillRequest`**，目标为本节点直接回调、远端经 `TOPIC_QUERY` + MANAGEMENT_POOL 发送（`RunningQueryManager.java:616-680`，发送点 `:635,657`）；future 等 `GridQueryKillResponse`。
3. 目标节点 `onQueryKillRequest`：查 `runs` 注册表，`runningQryInfo.cancel()`（`RunningQueryManager.java:751-784`，取消点 `:770`）。registered query 的 cancel 即触发 `GridQueryCancel`（core `GridQueryCancel.java:28-52`，cancelActions 列表 + canceled 标志）。
4. 两阶段取消扩散：reducer 侧 `cancel.add(() -> send(nodes, new GridQueryCancelRequest(reqId), ...))`（`GridReduceQueryExecutor.java:428`；DML 同型 `:987-991`）；map 端 `GridMapQueryExecutor.onCancel` 清理 `MapNodeResults` 并 cancel 各结果集（`GridMapQueryExecutor.java:164-180`）。本地 H2 语句级取消挂 `cancelStatement(stmt)`（`IgniteH2Indexing.java:698`）。消息统一入口 `IgniteH2Indexing.onMessage`（`:1628-1669`）按类型分发到两个 executor。

### 4.2 timeout

每个本地 H2 执行经 `executeSqlQueryWithTimer` → `executeSqlQuery`：`session(conn).setQueryTimeout(timeoutMillis)`（负值时取 `distrCfg.defaultQueryTimeout()`），并把语句取消挂到 `GridQueryCancel`（`IgniteH2Indexing.java:695-710`，timeout 落点 `:703-705`；包装层 `:790-823` 含 tracing 与 heavy query 跟踪）。请求级 timeout 随 `GridH2QueryRequest.timeout + explicitTimeout` 下发，map 端为 0 且非显式时用集群默认（`GridMapQueryExecutor.java:239-241`）。

### 4.3 QueryMetrics 打点位置

打点不在 indexing，而在 core 的包装器：`GridQueryProcessor.executeQuery(qryType, qry, cctx, clo, complete)` 记 startTime，finally 里 `cctx.queries().collectMetrics(...)`（`GridQueryProcessor.java:3855-3905`，打点 `:3899`）→ `GridCacheQueryManager.collectMetrics`：`metrics.update(duration, failed)` 汇入 cache 级 `GridCacheQueryMetricsAdapter`，并按 SQL 文本聚 top-N detail metrics（EXPLAIN 不计）（`GridCacheQueryManager.java:1399-1419`，构造 `:275`）。TextQuery/ScanQuery 迭代期另有 `metrics.onRead/addGetTimeNanos`（`:891-897`）。

### 4.4 schema 空间：PUBLIC 语义

默认 schema 常量 `DFLT_SCHEMA = "PUBLIC"`（`QueryUtils.java:101`，启动时在该 schema 注册 `QUERY_ENGINE` UDF，`H2SchemaManager.java:104-106`）。命名规则 `normalizeSchemaName(cacheName, sqlSchema)`：未显式指定 `sqlSchema` 时 **schema 名 = cache 名（强制引号转义）**；显式指定则按普通标识符归一（`QueryUtils.java:399-424`）。schema 的建/删事件由 `H2SchemaManager.onSchemaCreated/onSchemaDropped` 映射成 H2 `CREATE/DROP SCHEMA IF NOT EXISTS`（`H2SchemaManager.java:109-132`）。系统 schema `SYS`（或 `IGNITE`，`QueryUtils.java:110`）禁 DML（`QueryParser.java:558-560`）。SQL 建表固定 `setSqlSchema("\"schema\"") + setSqlEscapeAll(true)`（`GridQueryProcessor.java:2302-2303`）。

### 4.5 @QuerySqlField → QueryEntity → GridQueryTypeDescriptor

两条入口汇到同一描述符：

- **注解路径**（无 QueryEntity 时 `QueryEntity` 由类反射合成）：`QueryEntity.processAnnotationsInClass` 扫 key/value 类的字段注解（`QueryEntity.java:800-871`）；`processAnnotation` 展开 `@QuerySqlField` 全部属性——`index=true` 建单列索引（geometry 字段自动 GEOSPATIAL 类型，`:893-903`）、`notNull/precision/scale`、**`groups`/`orderedGroups`（组索引列序+升降序）**；组索引字段上设 inlineSize 直接抛错（inline size 只属于单列索引，`:914-918`）；类级 `@QueryTextField` 开值全文索引（`:833-836`）。
- **QueryEntity 路径**：`QueryUtils.typeForQueryEntity`（`QueryUtils.java:470-631`）构造 **`QueryTypeDescriptorImpl implements GridQueryTypeDescriptor`**：表名/别名/notNull/precision/scale（`processBinaryMeta` `:641+`）、affinity 字段（从 binary affinity mapper 或 `GridCacheDefaultAffinityKeyMapper` 推导，`:562-588,595-607`）、主键与亲和列 inline size（`QueryEntityEx` 专属，`:617-626`）。索引来自 `qryEntity.getIndexes()`：`processIndex` 按 `QueryIndexType` 分派——SORTED/GEOSPATIAL 走 `createIndexDescriptor`（inlineSize 随 `QueryIndex` 携带，`:810-834`），**FULLTEXT → `d.addFieldToTextIndex`**（`:851-860`，喂给 §4.6 的 Lucene 索引）。DDL 动态索引复用同一入口 `processDynamicIndexChange`（`:794-800`）。

### 4.6 Lucene 文本索引的真实使用面

indexing pom 共 3 个 lucene artifact（lucene-core / lucene-analyzers-common / lucene-queryparser，`modules/indexing/pom.xml:50-65`，grep 计 9 行匹配；版本 `lucene.version=8.11.2`，`parent/pom.xml:96`）。对应 **6 个类、1545 行**，全在 `opt/` 包：

- **`GridLuceneIndex`（432 行）**：真 Lucene 引擎——`IndexWriter`/`IndexSearcher`/`MultiFieldQueryParser`/`StandardAnalyzer`（import 见 `GridLuceneIndex.java:39-57`，类 `:68`，writer 字段 `:85`）；
- **`GridLuceneDirectory`（268）+ File/InputStream/OutputStream/LockFactory（共 1113 行）**：自定义 Lucene `BaseDirectory`，底层是 `GridUnsafeMemory` 堆外内存（`GridLuceneDirectory.java:34,49`）——把 Lucene 索引文件放进 Ignate 管理的 offheap。

**启用条件**（`H2TableDescriptor.createTextIndex`，`H2TableDescriptor.java:209-230`）：(a) value 类型为 `String.class` 且未设 `disableCreateLuceneIndexForStringValueType`（即 `TextQuery` 查 String 值 cache 开箱可用）；或 (b) 类型描述符有 textIndex（来自 `@QueryTextField` 或 `QueryIndexType.FULLTEXT`）。**使用面是 `TextQuery` API，不是 SQL**：`IgniteCache.query(TextQuery)` → `GridCacheQueryManager` TEXT 分支 → `GridQueryProcessor.queryText`（`GridCacheQueryManager.java:551-570`，`queryText` `:568`；`GridQueryProcessor.java:3720-3743`）→ **`IgniteH2Indexing.queryLocalText`**（`:345`）查本节点 `GridLuceneIndex`。SQL 引擎（H2）完全不经过 Lucene——不存在 "H2 FULLTEXT 定制"，也不存在 `IgniteH2TextIndex` 这个类。写路径：cache 更新时 H2 表描述符同步写 lucene 文档（`H2TableDescriptor.onDrop` 关闭索引，`:235-239`）。

### 4.7 geospatial 存在性核实

2.18 源码树**没有** `modules/geospatial`（`ls modules/` 核验，39 模块无此项），`GridH2SpatialIndex`/`GeoSpatialUtils` 全库 grep 零命中——它们是外部可选 artifact（ignite-geospatial 扩展），运行时反射加载：`H2Utils.createSpatialIndex` 反射调 `GeoSpatialUtils.createIndex`（`H2Utils.java:273-284`），类名常量 `:141-146`；启动探测 `checkSpatialIndexEnabled`（`:289-298`，`IgniteH2Indexing.java:1536-1538` 打日志）。in-tree 的只有：`QueryIndexType.GEOSPATIAL` 枚举与 DDL/注解映射（§2.3、§4.5）、客户端代理 **`GridH2ProxySpatialIndex`**、`H2IndexFactory` 的 GEOSPATIAL 分支（`H2IndexFactory.java:98-108`，proxy 分支 `:99-105`）。结论：**对"仓库内保真"而言 geospatial 的全部义务就是这套钩子**。

---

## 5. 章 8 切课建议（分级结论）

按切课纪律（一课一个新概念簇、单课 2–4h、宁多切不一课双簇；ROADMAP §3.2/§4），章 8"SQL 之 H2 课弧"建议 **8 课**。依赖图（`→` 硬前置）：

```
8.1 H2 内嵌与表映射 → 8.2 SQL 索引落盘(复用7.3树) → 8.3 切分与两阶段 → 8.5 分布式join → 8.6 查询进阶(裁剪/排序分页/EXPLAIN/lazy)
                                     ↘ 8.4 DDL 链(动态cache+SchemaManager事件源) ↗
8.3 + 8.4 → 8.7 DML 全链 → 8.8 查询治理(+TextQuery/Lucene，可后置)
```

| 课 | 概念簇 | tracer（验收里程碑） | 关键 vendor 锚点 | 约束落位 |
|---|---|---|---|---|
| 8.1 | H2 内嵌引擎 + schema/表映射：IgniteH2Indexing 装配、`CREATE TABLE ... engine`、GridH2Table/_KEY/_VAL、QueryEntity↔typeDescriptor（§4.5） | 配 QueryEntity 的 cache put 后本地 `SELECT *` 可查 | 04 §2.1 + §4.5 本报告（`QueryUtils.java:470-631`） | **②SchemaManager 事件源**在此成形（onSqlTypeCreated 建表链；calcite 弧只做消费者） |
| 8.2 | SQL 索引落盘：H2IndexFactory/H2TreeIndex→InlineIndexImpl、pk hash/affinity 系统索引、inline size | `CREATE INDEX` 后索引页出现在 page memory（重启存活） | 04 §2.1、13 §1.6、`H2IndexFactory.java:77-96` | **①禁止平行实现**：InlineIndexTree 只准包 7.3 的 BPlusTree |
| 8.3 | 查询切分与两阶段：QueryParser、splitSelect 主算法、GridH2QueryRequest/分页回传、reduce merge table | **跨节点 SELECT**（章 tracer）：两节点数据单查询聚合正确 | 04 §2.2（已验收，不重查） | — |
| 8.4 | DDL 链：CommandProcessor 分层、CREATE TABLE→QueryEntity→getOrCreateCache0→DynamicCacheChangeBatch→PE、SchemaProposeDiscoveryMessage 协议、CREATE/DROP INDEX | 纯 SQL 建表→查表→建索引→删表，多节点 schema 一致 | §2 全部（`GridQueryProcessor.java:2237-2326,3564-3601`） | 复用章 4 PME/exchange 概念（螺旋重访点） |
| 8.5 | 分布式 join：CollocationModel 判定、DistributedLookupBatch unicast/broadcast、GridH2IndexRangeRequest/Response、replicated 参与方式 | 三表 join：colocated 对比 distributedJoins=true 结果一致且后者抓到远程查找消息 | §3.1-3.2（`DistributedLookupBatch.java`、`CollocationModel.java:635-733`） | — |
| 8.6 | 查询进阶：分区裁剪树、ORDER BY/LIMIT/OFFSET/DISTINCT 边界（§3.5）、UNION/子查询上拉、EXPLAIN、lazy | EXPLAIN 可见分区裁剪生效；`ANALYZE` 风格断言 map 计划 | §3.3-3.7（`PartitionExtractor.java`、`GridSqlQuerySplitter.java:1228-1262`） | — |
| 8.7 | DML 全链：SELECT 改写（_upd_ 列）、UpdatePlan、DmlBatchSender/invokeAll、EntryProcessor CAS+重跑、分布式 DML（skipReducerOnUpdate，选做） | **跨节点 UPDATE/DELETE** 计数正确；并发改写触发重跑或 CONCURRENT_UPDATE | §1 全部（`DmlUtils.java:166-406`、`DmlStatementsProcessor.java`） | 复用章 1 EntryProcessor/keepBinary 概念 |
| 8.8 | 查询治理：GridQueryCancel/RunningQueryManager/KILL 链、timeout、metrics、public schema 语义；（可选半课）TextQuery+Lucene | KILL QUERY 取消长查询（两阶段各节点资源释放）；`cache.queryMetrics()` 有数 | §4 全部（`RunningQueryManager.java:616-784`） | — |

8.7 依赖 8.3（派生 SELECT 走两阶段）与 8.4（DML 需要 cache 就绪语义）；8.6 依赖 8.5（EXPLAIN 展示 join 计划更有料）。若总量吃紧，8.8 的 Lucene 半课与 8.6 的 lazy 细节是最先让位的两块（见裁决材料）。

### 5.1 已定约束核对

- **①InlineIndexTree 复用章 7.3 BPlusTree**：落位 8.2。13 §1.6 已给证据（索引树=数据树同引擎两种用法）；8.2 验收里应包含"索引行复用 7.3 树"的用例（13 §6 末段同款建议，勿把树特化进数据路径）。
- **②SchemaManager 事件源是 calcite 弧前置**：落位 8.1（类型注册事件面）+ 8.4（schema/index/propose 协议面）。06 §3 已验收 calcite 只做 `SchemaChangeListener` 消费者——所以 8.1/8.4 完成即视为章 9 的 schema 前置就绪，章 9 不得新建基础设施。

### 5.2 continuous queries 归属建议

**建议：不并入章 8，归入"事件与监听体系"fog，作为其独立小弧的入口课。** 理由（全部实据）：

1. **链路主体不在 SQL 引擎**：ContinuousQuery 的运行时是 `CacheContinuousQueryManager`（5685 行包，`cache/query/continuous/`，JCache `CacheEntryListener` 注册与事件过滤）+ `GridContinuousProcessor`（4894 行包，`processors/continuous/`，routine 管理、批量投递、ack 缓冲）——两者都是 cache 事件基础设施，与 H2 无耦合；唯一 SQL 接触点是可选的**初始查询就是一个普通 `SqlFieldsQuery`**（8.3 之后免费获得）。
2. **共享底座指向事件弧**：`GridContinuousProcessor` 同时服务 `IgniteMessaging` 与 node discovery routine（同一条 routine/消息通道体系），把 continuous query 放进 SQL 章会让学生在 H2 语境里突然遇到一套全新的 discovery 常规注册协议——正是"一课双簇"反模式。
3. **地图上挂三章的重访点恰好需要它**：作为 fog 重访点，它天然串起 章 3（通信消息）、章 4（partition rebalance 时的 `CacheContinuousQueryPartitionRecovery`）、章 10（data streamer/compute 的批量回传思想）。放在章 8 之后、章 9 之前的独立小弧（约 2 课：routine 基础设施 + cache listener 桥），可同时向三个方向回访，教学杠杆最高。

### 5.3 可简化项裁决材料（默认全做，重点三项）

1. **Lucene/TextQuery**：in-tree 义务 = 6 类 1545 行 + 3 个依赖。保真的"行为面"是：启用条件（String 值 cache 开箱即建索引）、`TextQuery` 走 `queryLocalText` 独立路径、写路径同步。**可简化点**：`GridLuceneDirectory` 的 offheap 文件系统模拟（1113 行）可用 Lucene 自带 `RAMDirectory` 平替——它是存储工程而非查询语义，验收测试（TextQuery 命中）不感知；Lucene 本身与 H2 同等定位（黑盒依赖，ADR 0005 的口径一致）。裁决：**8.8 做接线全保真、Directory 用 RAMDirectory**，offheap Directory 列为课后可选。
2. **geospatial**：官方源码树就没有实现（§4.7），复刻义务只剩反射钩子 + GEOSPATIAL 枚举 + proxy 索引（几十行）。**范围裁决：钩子全做、R-tree 零义务**——这不是简化，是"全保真"按 2.18 源码树的字面边界。注意 CREATE INDEX ... SPATIAL 在无扩展时行为要一致（找不到类即 `checkSpatialIndexEnabled()=false` 报不支持）。
3. **BULK LOAD（COPY）**：dml 侧很薄（`planForBulkLoad` + converter），重头在 core 的 1270 行 bulkload 流水线 + **客户端逐包推文件的 wire 协议**（BulkLoadAck 往返）。若 thin client 协议弧（章 12）尚未到位，COPY 的客户端路径没法全保真验收。裁决：**本地文件变体全做**（服务端已有文件即可触发整条流水线），客户端推包变体挂到章 12 thin client 课后回访补——与"fog 重访"模式一致。教学价值中上：流水线 block 组合 + data streamer 复用本身就是好素材。与 COPY 同族的还有流式 INSERT（`SET STREAMING`，`SqlSetStreamingCommand` 分支 `CommandProcessor.java:185-186` + `isStreamableInsertStatement` 判定 `IgniteH2Indexing.java:1498-1502`）——它同样终点是 data streamer，可与 COPY 同课或并入章 10 data streamer 课。

其余不建议简化：DML 的 EntryProcessor CAS + 重跑（行为正确性骨架）、SchemaProposeDiscoveryMessage 两阶段（DDL 一致性）、分区裁剪树（EXPLAIN 可观测）、cancel 链（资源释放语义）。

---

## 引用文件清单（全部实际打开核验）

indexing 模块：

1. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlAstUtils.java`
2. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlArguments.java`（存在性核验，未展开）
3. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlBatchSender.java`
4. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlBulkLoadDataConverter.java`
5. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlDistributedPlanInfo.java`
6. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlDistributedUpdateRun.java`
7. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/DmlUtils.java`
8. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/FastUpdate.java`
9. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/UpdateMode.java`
10. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/UpdatePlan.java`
11. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/dml/UpdatePlanBuilder.java`
12. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/CommandProcessor.java`
13. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/DmlStatementsProcessor.java`
14. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2DmlInfo.java`
15. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2IndexFactory.java`
16. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2SchemaManager.java`
17. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2TableDescriptor.java`
18. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2Utils.java`
19. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/IgniteH2Indexing.java`
20. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/QueryParser.java`
21. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/GridSubqueryJoinOptimizer.java`
22. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/sql/GridSqlQueryParser.java`
23. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/sql/GridSqlQuerySplitter.java`
24. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/GridCacheTwoStepQuery.java`
25. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/affinity/PartitionExtractor.java`
26. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/database/H2TreeIndex.java`
27. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/GridH2IndexBase.java`
28. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/join/CollocationModel.java`
29. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/join/DistributedLookupBatch.java`
30. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/join/DistributedJoinContext.java`（存在性核验）
31. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/GridMapQueryExecutor.java`
32. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/GridReduceQueryExecutor.java`
33. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/MapQueryResult.java`
34. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/msg/GridH2QueryRequest.java`
35. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/GridLuceneIndex.java`
36. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/GridLuceneDirectory.java`
37. `modules/indexing/pom.xml`

core 模块：

38. `modules/core/src/main/java/org/apache/ignite/internal/sql/SqlCommandProcessor.java`
39. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/GridQueryProcessor.java`
40. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/GridQueryCancel.java`
41. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/QueryUtils.java`
42. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/QueryTypeDescriptorImpl.java`（存在性核验）
43. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/running/RunningQueryManager.java`
44. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheProcessor.java`
45. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/GridCacheQueryManager.java`
46. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/SqlFieldsQueryEx.java`
47. `modules/core/src/main/java/org/apache/ignite/cache/QueryEntity.java`
48. `modules/core/src/main/java/org/apache/ignite/cache/query/SqlFieldsQuery.java`
49. `modules/core/src/main/java/org/apache/ignite/internal/processors/bulkload/BulkLoadProcessor.java`（存在性与行数核验）
50. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousProcessor.java`（存在性与行数核验）
51. `parent/pom.xml`

---

## 对复刻课的启示

1. **DML 的教学主线是"改写成 SELECT + EntryProcessor CAS 写回"，不是"SQL 引擎写数据"**。复刻 8.7 时先让学生写出 `selectForUpdate` 的 `_upd_` 投影（`DmlAstUtils.java:350-361`），再对接 `ModifyingEntryProcessor` 的比较-修改（`DmlStatementsProcessor.java:72-89`）——整条链没有任何新的存储路径，全部复用章 1 的 invokeAll 与两阶段查询，是"螺旋重访"原则的完美样本。
2. **DDL 是 cache 生命周期课的 SQL 皮肤**。`CREATE TABLE` 的全部新知识只有一个映射函数 `toQueryEntity`；落库动作就是章 4 的 `getOrCreateCache0 → DynamicCacheChangeBatch → PME`（§2.2）。切课时把它放 8.4 而不是与 8.1 合并，正是为了避免"表映射 + discovery 协议"双簇；`SchemaProposeDiscoveryMessage` 是第二条 discovery custom event 协议（与 DynamicCacheChangeBatch 平行），讲义里画成对照图可复用章 2/4 的全部词汇。
3. **分布式 join 的可观测锚点是"map 侧远程索引查找"**。复刻验收建议抓包式断言：distributedJoins=true 时 map 节点发出 `GridH2IndexRangeRequest`（unicast 当 affinity 可提取、broadcast 否则，§3.1）。replicated 表的参与方式（永远本地 + 必须排 join 序列末尾，`CollocationModel.java:728-730` 的报错即规范文本）是学生最常踩的坑，直接把这条报错写进测试。
4. **分区裁剪是独立于 join 的第二个优化器子系统**（`PartitionExtractor` 的代数树，§3.3），且明确"UNION 不裁剪"。8.6 的 tracer 用 EXPLAIN 双输出（map 计划 + reduce 计划，§3.6）验收——EXPLAIN 本身就是免费的测试钩子。
5. **lazy 不是结果集类而是两侧协议标志**（§3.7 的纠偏写进讲义防学生搜 `LazyQueryList`）；它和 DML 的 lazy 禁用条件（`UPDATE SET val=val+1 WHERE val>=?` 反例，`IgniteH2Indexing.java:2211-2215` 注释原文）是同一概念的两面，宜同课讲。
6. **Lucene/geospatial/BULK LOAD 三个"周边"的范围裁决都已按源码树字面边界给出**（§5.3）：geospatial 在 2.18 源码树里本来就只有钩子；Lucene 保真在接线、Directory 可平替；COPY 客户端推包协议挂到 thin client 章回访。三者都不阻塞章 8 主干 tracer（跨节点 SELECT / UPDATE）。
7. **continuous query 判出 SQL 章**（§5.2）：它的 5685+4894 行主体是事件基础设施，SQL 只是初始查询的借道。把它做成"事件与监听体系"小弧的入口课，三个挂章（通信/rebalance/compute）都能回访——这比塞进章 8 省一次概念簇爆炸。
8. **8.1 必须把 `QueryEntity`（声明式）与 `@QuerySqlField`（注解式）两条入口都打通**（§4.5 汇到同一 `QueryTypeDescriptorImpl`）：章 9 calcite 与后续所有 SQL 课都消费这份描述符，这里偷懒会让全弧返工。
