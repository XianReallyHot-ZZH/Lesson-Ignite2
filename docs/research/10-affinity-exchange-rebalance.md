# 10 · 控制面三大件：affinity 分配、partition exchange 与 rebalance

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文回答三个子系统"内部怎么实现"：谁能成为 primary/backup（affinity）、集群如何就分区归属达成一致（exchange）、数据如何搬（rebalance），并覆盖 cache group / REPLICATED / client 视角。数据面如何消费这些结论见 03 号报告（`nodesByKey → GridAffinityAssignmentCache → Rendezvous 备份复制`），节点启动期如何被 exchange/rebalance 阻塞见 01 号报告 §5.1，本文不重复、只补内部结构。正文短路径是 `org/apache/ignite/internal/processors/cache/` 下的缩写（如 `.../dht/preloader/…`），**文末"引用文件清单"给出全部完整路径**（均已逐一打开验证），行号以当前 submodule 为准。注意 2.18 的分区拓扑类在 `.../cache/distributed/dht/topology/` 子包（不是旧的 `dht/` 包）。

---

## 0. 先决事实（影响全篇的四个核对结果）

三件套的一句话架构（细节见各节）：

| 子系统 | 核心类（每节点 × 每 cache group） | 一句话 |
|---|---|---|
| affinity | `GridAffinityAssignmentCache` 包 `RendezvousAffinityFunction` | 纯函数从 discoCache 快照算出"每分区 owner 列表"，按 `AffinityTopologyVersion` 版本化缓存；server 各自重算、结果必然一致 |
| exchange | `GridCachePartitionExchangeManager`（单线程 `ExchangeWorker`）+ `GridDhtPartitionsExchangeFuture` | discovery 事件 → 排队 → coordinator（最老 server）收齐全网分区快照 → 广播权威分区图；数据面靠 topology future 挡在映射前 |
| rebalance | `GridDhtPreloader`（`GridDhtPartitionDemander` + `GridDhtPartitionSupplier`） | exchange 完成后由 worker 统一发起，demander 向 owner 拉数据，分区 MOVING→OWNING，齐了才做 late affinity 切换 |

四个核对结果（针对常见"想当然"）：

1. **late affinity assignment 不可关闭**。`IgniteConfiguration#isLateAffinityAssignment()` 已 `@Deprecated` 且**硬编码返回 true**，setter 是空操作（`configuration/IgniteConfiguration.java:3315-3331`）；节点属性 `ATTR_LATE_AFFINITY_ASSIGNMENT`（`internal/IgniteNodeAttributes.java:175`）在 kernal 启动时**固定写入 true**（`internal/IgniteKernal.java:1540`），discovery join 校验只保证全网一致、不可能不一致（`internal/managers/discovery/GridDiscoveryManager.java:1350-1359`）。因此 2.18 只存在"eager 计算理想分配 + late 生效切换"这一条路径，"eager 模式"是纯历史概念。
2. **`detectNodeLeft` 不存在**。2.18 的实际机制是 `GridDhtPartitionsExchangeFuture#onNodeLeft`（`.../dht/preloader/GridDhtPartitionsExchangeFuture.java:4917`）；coordinator 中途死亡由 `InitNewCoordinatorFuture` 接管（新协调者向全体 server 发 `GridDhtPartitionsSingleRequest` 恢复状态，`.../dht/preloader/InitNewCoordinatorFuture.java:162-170`）。
3. **`IGNITE_PARTITION_LOSS_POLICY` 系统属性不存在**（全 core 模块 grep 无命中）。分区丢失策略的真正开关是 `CacheConfiguration#partitionLossPolicy`（枚举 `PartitionLossPolicy`：`READ_ONLY_SAFE/READ_WRITE_SAFE/IGNORE` + 两个 deprecated 值，`cache/PartitionLossPolicy.java:46-79`），执行点在 `.../dht/GridDhtTopologyFutureAdapter.java:121-133`。
4. **分区状态是 5 个**而非 4 个：`GridDhtPartitionState = MOVING / OWNING / RENTING / EVICTED / LOST`（`.../dht/topology/GridDhtPartitionState.java:25-39`），`active()` 仅排除 `EVICTED`（L55-57）。

---

## 1. affinity：函数、缓存与"late"语义

### 1.1 RendezvousAffinityFunction 内部（HRW — Highest Random Weight）

- **key → partition 不是摘要函数**。`partition(key)` 调 `calculatePartition`（`cache/affinity/rendezvous/RendezvousAffinityFunction.java:504-510`，静态实现在 L131-139）：分区数是 2 的幂时用 mask 快速取模 `((h ^ h>>>16) & mask)`（mask 由 `calculateMask` 算出，L119-121），否则 `U.safeAbs(key.hashCode() % parts)`。**没有 MD5/SHA**。
- **node × partition → 权重才是"rendezvous"**。`hash(int key0, int key1)`（L478-491）把 `(nodeHash.hashCode(), partition)` 两个 int 打包成 long，再用 **Wang/Jenkins 64-bit mix function**（注释明示，L470-477）扩散。nodeHash 来自 `resolveNodeHash(node) = node.consistentId()`（L340-342）——这就是"节点重启只要 consistentId 不变、分配不变"的机制根源。
- **分配算法**（`assignPartitions` L513-528 逐分区调 `assignPartition` L353-444）：
  1. 对每个节点算 `hash(nodeHash.hashCode(), part)`，装入数组；
  2. 用 `LazyLinearSortedContainer`（L552-626）**按权重升序惰性排序**（只需前 `backups+1` 名时避免全排序，预估量大于 `log(n)` 则退化为全排序，L566-571；比较器 tie-break 用 node id，L538-547）；
  3. **最小者为 primary**（`primary = it.next()`，L387），随后依次取 `backups` 个；
  4. `backups` 不来自 function 自身，来自 `AffinityFunctionContext#backups()`（即 cache 配置）；`backups == Integer.MAX_VALUE` 走 `replicatedAssign`：primary 取排序首位、**其余所有节点全部追加为 backup**（L453-467，触发条件 L377-379）。
- **配置语义**：`DFLT_PARTITION_COUNT = 1024`（L79）；`excludeNeighbors` 默认 false（L91，构造器 L157-159），开启时用 `GridCacheUtils.neighbors` 建同宿主机邻接表（L516-517），backup 跳过 primary 的邻居（L403-411），凑不齐时**降级为无过滤并告警一次**（L420-438）；`backupFilter`（deprecated，L269-275）与 `affinityBackupFilter`（L302-307）在无 exclNeighbors 时按谓词筛选 backup（L400-402）。`setPartitions` 校验上限 `CacheConfiguration.MAX_PARTITIONS_COUNT` 并重算 mask（L233-243）。
- **AffinityFunctionContext 传参**：实现是 `GridAffinityFunctionContextImpl`，5 个字段——`currentTopologySnapshot()`（按 `NodeOrderComparator` 排序后的 cache group 节点快照，`.../affinity/GridAffinityAssignmentCache.java:345-347`）、`previousAssignment(part)`（用于增量）、`discoveryEvent()`（触发本次变更的事件）、`currentTopologyVersion()`、`backups()`（`.../affinity/GridAffinityFunctionContextImpl.java:30-82`）。

### 1.2 GridAffinityAssignmentCache：每个 cache group 一个的版本化缓存

- **结构**（`.../affinity/GridAffinityAssignmentCache.java`）：`ConcurrentNavigableMap<AffinityTopologyVersion, HistoryAffinityAssignment> affCache`（L120）是"拓扑版本 → 分配"主缓存；`volatile IdealAffinityAssignment idealAssignment`（L123）是**理想分配**（function 直接算出的）；`AtomicReference<GridAffinityAssignmentV2> head`（L132）指向最新已生效分配；`readyFuts`（L135，`ConcurrentSkipListMap`）是等待某版本分配就绪的 future 表。每条历史项分**全量实例**（`HistoryAffinityAssignmentImpl`）与**浅拷贝**（`HistoryAffinityAssignmentShallowCopy`，client 事件复用上一版分配时使用，L578-580）。
- **写入路径两条**：`initialize(topVer, assignment)`（L234-264）写入新分配、推进 head、并把 `readyFuts` 中所有 ≤topVer 的 future 完成（L247-255）；`clientEventTopologyChange`（L561-597）处理不改变 affinity 的事件（client join/leave 等）——直接复用 head 的分配生成浅拷贝。
- **计算入口** `calculate(topVer, events, discoCache)`（L329-452）：若本组所有触发事件都不来自 affinity 节点（client 或被 nodeFilter 过滤）则**跳过重算复用旧分配**（L364-389）；否则调 `aff.assignPartitions(new GridAffinityFunctionContextImpl(...))`（L401-407）。baseline 相关分支（L349-399）只影响持久化部署，复刻课可后置。
- **清除时机**：`onHistoryAdded`（L945-991）在历史数超限后收缩——非浅拷贝上限 `IGNITE_AFFINITY_HISTORY_SIZE` 默认 25（L70，L79），总数上限 250（L86）；始终保留 ≥2 个全量实例（`MIN_NON_SHALLOW_HIST_SIZE`，L95），且**永不删除 `lastAffinityChangedTopologyVersion` 对应项**（L971-972，供 exchange 查询）。`onReconnected` 时整体清空（L309-319）。
- **"ready" 语义**：`readyFuture(topVer)`（L641-663）head 已 ≥topVer 则返回 null（已完成），否则挂 `AffinityReadyFuture`（内部类，L1044-1066，完成后自摘除）。数据面不用它直接等，而是走 topology future（见 §2.5），但 `GridCacheAffinityManager#affinityReadyFuture`（`cache/GridCacheAffinityManager.java:80-106`）把同一批 future 暴露给缓存加载、CDC 等组件。
- **同构组复用**：构造时算 `similarAffKey = ctx.affinity().similaryAffinityKey(aff, nodeFilter, backups, partsCnt)`（L187-189）——function/filter/backups/分区数全相同的两个 cache group 共享同一 key，上层可借此合并 affinity 计算（字段 L150，暴露方法 L210-212）。复刻课可作为优化点后置，但**字段本身必须建**，否则 group 间一致性无从校验。

### 1.3 late assignment 的真正含义：ideal 与 current 的差

"late"不是算得晚，而是**生效得晚**：exchange 里每节点先把 function 算出的 ideal 存进 `idealAssignment`，但写入 `affCache` 生效的"current assignment"可能仍指向旧 owner（新 primary 数据没到位时，老 owner 继续当 primary、新 owner 暂列 backup）。支撑结构有四：

1. coordinator 在收齐消息后计算 `idealAffDiff`（理想 vs 可立即生效的差异），随 `GridDhtPartitionsFullMessage#idealAffinityDiff` 下发（`.../dht/preloader/GridDhtPartitionsExchangeFuture.java:3866-3875`；消息字段 `GridDhtPartitionsFullMessage.java:120`）；各节点用 `CacheAffinitySharedManager#applyAffinityFromFullMessage` 把 current 初始化为"ideal 减去 diff 分区"（`cache/CacheAffinitySharedManager.java:1456-1486`，调用点 exchange future L4596/L4611）。
2. `GridDhtPartitionTopologyImpl` 维护 `diffFromAffinity`：在 OWNING/MOVING/RENTING 却不在 current assignment 里的节点集合（`.../dht/topology/GridDhtPartitionTopologyImpl.java:1636-1663`）；`nodes0()`（L1196-1282）返回 **assignment 节点 + diff 节点**（过滤条件 `state == MOVING || OWNING || RENTING`，L1245/L1263）——所以 rebalance 未完成时数据面仍会把请求发给老 owner。**REPLICATED 直接短路返回 affinity 节点**（L1197-1198）。
3. coordinator 记录 `WaitRebalanceInfo`：exchange 结束时把"owner 还不齐"的分区登记进 wait 组（`GridDhtPartitionTopologyImpl.java:2462-2474` 调 `CacheAffinitySharedManager#addToWaitGroup`，`CacheAffinitySharedManager.java:378-385`）。
4. 每当某组分区全部补齐，coordinator 的 `checkRebalanceState` 广播 **`CacheAffinityChangeMessage`**（discovery custom 事件，`CacheAffinitySharedManager.java:283-333`，L391-396 构造），触发一次轻量 minor exchange，各节点在 `onChangeAffinityMessage` 中用 `aff.initialize(topVer, idealAssignmentRaw())` 完成**最终切换**（L1166-1214）。

---

## 2. partition exchange：PME（Partition Map Exchange）

### 2.1 触发与队列：GridCachePartitionExchangeManager

- 入口 `onDiscoveryEvent(evt, cache)`（`cache/GridCachePartitionExchangeManager.java:538-690`）断言只接受 `EVT_NODE_JOINED / EVT_NODE_LEFT / EVT_NODE_FAILED / EVT_DISCOVERY_CUSTOM_EVT`（L541-542）。非 custom 事件（server/client 节点增删）一律建 exchange future（L549-574）；custom 事件按类型分派：`ChangeGlobalStateMessage`（集群激活，L578-588）、`DynamicCacheChangeBatch`（动态建/删 cache，L589-599）、`CacheAffinityChangeMessage`（late 切换，L600-613）、`ExchangeFailureMessage`（L614-622）、`SnapshotDiscoveryMessage`/`WalStateAbstractMessage`（L623-634）、其余交给 worker 的自定义任务（L636-641）。
- future 创建后 `addFuture` 入队 `ExchangeWorker`（L674）。**ExchangeWorker 是单线程 `GridWorker`**（内部类，L2771-2794，线程名 `partition-exchanger`，由 `onKernalStart` 以 `IgniteThread` 启动，L750），任务队列 `LinkedBlockingDeque<CachePartitionExchangeWorkerTask> futQ`（L2773）——既装 exchange future 也装自定义任务（rebalance 重分配、schema 变更等，L2799-2835）。worker 主循环 `body0`（L3017-…）：`futQ.poll(timeout)` 取任务（L3061）→ 非交换任务就地执行（L3069-3073）→ 交换任务则 `exchFut.init(...)` 后**阻塞 `exchFut.get(exchTimeout)`**（L3184，超时打诊断日志并按 `txTimeoutOnPartitionMapExchange` 回滚长事务，L3194-3223）→ 完成后 `grp.topology().afterExchange(exchFut)`（L3246-3247）并触发 rebalance（见 §3.2）。
- 节点启动时（`onKernalStart` L730-812）：用本地 join 事件构造首个 exchange id（`initialExchangeId` L711-721），建 future 后**同步 `fut.get(preloadExchangeTimeout)` 等它完成**（L774-808）——这就是 01 号报告证实的"首 exchange 阻塞 onKernalStart"的现场。exchange 历史默认保留 1000 条（`DFLT_EXCHANGE_HISTORY_SIZE = 1_000`，L183，`IGNITE_EXCHANGE_HISTORY_SIZE` 可调，L192-193），晚到的 client join 超出历史会被强制重连（L1876-1890）。

### 2.2 GridDhtPartitionsExchangeFuture 状态机

- **两套枚举**：对外结果 `ExchangeType {CLIENT, ALL, NONE}`（exchange future 文件 L5586-5595）——ALL=server 全量交换、CLIENT=只向 coordinator 发一条 SingleMessage、NONE=无需交换直接 done；对内 `ExchangeLocalState {CRD, SRV, CLIENT, BECOME_CRD, DONE, MERGED}`（L5600-5621），init 时按"本节点是否 coordinator / 是否 client"置初值（L918-923）。coordinator 恒为 `firstEvtDiscoCache.serverNodes().get(0)`（最老 server，L910）。
- **init(boolean newCrd)**（L887-1123）按序：等 discovery 事件到齐（evtLatch）→ 建 `ExchangeContext`（L914）→ 定 `ExchangeType`：`exchangeFreeSwitch`（已 rebalance 的集群上 baseline 节点故障/snapshot，免 PME 快速路径）优先（L944-959），custom 事件按消息类型（L960-991），普通节点事件：join 先启动收到的 cache（L993-1003），再 `onServerNodeEvent`/`onClientNodeEvent`（L1527-1541 / L1499-1520）→ `updateTopologies` → 分派（L1047-1077）：ALL→`distributedExchange()`，CLIENT→`clientOnlyExchange()`，NONE→直接 `initTopologies + onDone`。
- **server 全量交换 `distributedExchange()`**（L1611-1775）阶段：
  1. `grp.preloader().onTopologyChanged(this)` 通知 preloader 停旧 rebalance（L1616-1625）；
  2. **两阶段分区释放**：`waitPartitionRelease(EXCHANGE_LATCH_ID, distributed=true, doRollback=true)` 用分布式 latch（`.../dht/preloader/latch/ExchangeLatchManager`）等全网在途事务/atomic 更新/锁收尾，第二阶段等 primary→backup 的残留更新（L1642-1647，实现 L1829-1845；本地 join 可跳过，L1630-1635）；
  3. write-behind flush、`cctx.database().beforeExchange`（持久化恢复，L1682）、`grp.topology().beforeExchange` **预建缺失分区并按理想分配置 MOVING**（L1699）；
  4. WAL 历史预留（L1725-1728）、`finalizePartitionCounters`（server 离开时，L1740-1741）；
  5. 分角色收尾：coordinator 若无其他 server（单节点集群）直接 `onAllReceived(null)`（L1760-1764）；否则**普通 server `sendPartitions(crd)` 发 SingleMessage**（L1767 → L2283 → L2101）；client 见 §4.3。
- **coordinator 收尾 `onAllReceived` → `finishExchangeOnCoordinator`**（L3670-3722 / L3727-3997）：merge 协议下先尝试合并后续 exchange（L3694-3712）；`processSingleMessageOnCrdFinish` 汇总各节点分区 update counters（L3787-3796）；`assignPartitionsStates` 对持久化组**按最大 counter 决定历史 supplier 与归属**（wrapper L4095-4132，核心 `assignPartitionStates` L3350-3429 + `resetOwnersByCounter` L3439-3453：counter 落后的 OWNING 分区强制降为 MOVING）；构造 `GridDhtPartitionsFullMessage` 广播（`sendAllPartitions` L2193-2222+，`rebalanced` 标志 L3863-3864），置 `state=DONE`、`onDone(resTopVer)`（L3881-3940），随后 `cctx.exchange().checkRebalanceState()`（L3986）看能否立刻做 late 切换。
- **非 coordinator 收到 FullMessage**：`processFullMessage`（L4476-4553）校验发送者身份后置 `FinishState`、`state=DONE`，应用分区全表/丢失分区/affinity diff，未来即完成。状态机收到"非协调者发来的 FullMessage"时缓存进 `fullMsgs` 备用（更高 node order 的发送者可能正是新协调者，L4520-4521）——这是死亡接管期的防御分支。
- **coordinator 身份不选举、按序继承**：worker 每次取到 exchange 任务时，若尚未当过 coordinator，就用 `firstEventCache().serverNodes().get(0).isLocal()` 判定（`GridCachePartitionExchangeManager.java:3126-3132`）；exchange future 内部同样取 `srvNodes.get(0)`（L910）。即**最老存活 server 就是 coordinator**，与 discovery ring 的 oldest 语义一致（09 号报告），没有任何投票协议。worker 等待期间的诊断输出（pending future dump、长事务回滚建议）集中在 L3194-3223。
- **中途死亡**：`onNodeLeft`（L4917-4990+）把离开节点从 `srvNodes/remaining` 移除；若死的是 coordinator，最老 server 置 `BECOME_CRD` 并建 `InitNewCoordinatorFuture`（L4983-4987）——新协调者向所有 server 发 `GridDhtPartitionsSingleRequest.restoreStateRequest`（`InitNewCoordinatorFuture.java:162-170`）拉回各自的 SingleMessage 快照，重建全局视图。merge 协议下多个连续拓扑事件可被合并成一个 future（`ExchangeContext#merge`，`cache/ExchangeContext.java:59-90`；结果版本 `resTopVer` 随 FullMessage 回传，落后节点在 `processFullMessage` 里检测合并，L4559-4575）。

### 2.3 GridDhtPartitions*Message 族逐个

| 消息 | 方向/时机 | 关键字段（定义处） | 用途 |
|---|---|---|---|
| `GridDhtPartitionExchangeId` | 所有消息的身份头 | `nodeId`(L49)、`evt`(L54，事件类型)、`topVer`(L58)（`GridDhtPartitionExchangeId.java`） | 标识"哪个节点的哪个事件触发的这次交换" |
| `GridDhtPartitionsAbstractMessage` | 基类 | `exchId`(L39)、`lastVer`(L43，最后 grid 版本)、压缩/restore 标志位(L32-35)（`GridDhtPartitionsAbstractMessage.java`） | 公共头 |
| `GridDhtPartitionsSingleMessage` | server→crd（交换中）或任意节点→crd（增量更新）；client→crd 带第 2 参 `client=true` 构造 | `parts`(L44，grpId→`GridDhtPartitionMap` 本节点各分区状态)、`partCntrs`(L61，部分 update counters)、`partsSizes`(L66)、`errMsg`(L76)、`client`(L80)、`grpsAffReq`(L84，join 时请求 affinity)、`finishMsg`(L95)（`GridDhtPartitionsSingleMessage.java`） | 上报"我拥有的分区及状态/计数器"。client 版本只含版本不含分区（构造点 exchange future L2117-2121） |
| `GridDhtPartitionsFullMessage` | crd→所有 server/client | `parts`(L61，grpId→`GridDhtPartitionFullMap` 全网全表)、`partCntrs`(L78)、`partHistSuppliers`(L83，历史 rebalance supplier 指派)、`partsToReload`(L88)、`topVer`(L96)、`resTopVer`(L112，merge 结果版本)、`joinedNodeAff`(L116，新节点 affinity)、`idealAffDiff`(L120，late 差异)、`lostParts`(L129)、`REBALANCED_FLAG`(L57)（`GridDhtPartitionsFullMessage.java`） | 全网分区地图 + 计数器 + 各种 reassignment 指派的**权威广播** |
| `GridDhtPartitionsSingleRequest` | 新协调者→所有 server | `restoreExchId`(L31-32)（`GridDhtPartitionsSingleRequest.java:48-56`） | coordinator 死亡后恢复交换状态 |
| `CacheGroupAffinityMessage` | FullMessage 内嵌 | grp 级分配的紧凑 UUID 序列（同包文件） | 节点序号化传输 assignment，支撑 `joinedNodeAff`/`idealAffDiff` |

handler 注册集中在 `GridCachePartitionExchangeManager`（`GridDhtPartitionsSingleMessage` → `processSinglePartitionUpdate` L1830-1899，区分"交换中"与"交换外的增量更新"后者走 `topology.update` + `scheduleResendPartitions`；FullMessage → L1782-1819 更新本地 topology 后 `refreshPartitions()`）。

**交换外的增量同步**值得单独点名：exchange 结束后若某节点分区状态又变化（如分区补齐 OWNING），`scheduleResendPartitions` 会以 `exchangeId == null` 的 SingleMessage 单播给 coordinator，coordinator 视情况再广播——这是 late 切换（§1.3）之外保持全网分区图新鲜的第二条低开销通道。序列化细节：Single/Full 的分区表都有 `partsBytes` 字节数组形态（`GridDhtPartitionsSingleMessage.java:52`、`GridDhtPartitionsFullMessage.java:69`）且基类带压缩标志位（`GridDhtPartitionsAbstractMessage.java:32-35`），大集群 FullMessage 默认压缩后广播（`createPartitionsMessage(true)`，exchange future L2171-2179）。

### 2.4 exchange 期间 cache 启动与 CacheAffinitySharedManager

`CacheAffinitySharedManager` 是"affinity 的共享协调者"（每个节点一个，管理**所有** group 的 `GridAffinityAssignmentCache`）：

- **本节点 join**：对每个注册 group 建 holder；coordinator 直接 `calculateAndInit`（本地跑 function，`CacheAffinitySharedManager.java:1709-1741`，L1790-1798）；非 coordinator 走 `fetchAffinityOnJoin`——本 exchange 新启动的组本地算，已存在的组**旧协议**用 `GridDhtAssignmentFetchFuture` 向 coordinator 拉（L1804-1829），**新协议**（merge 允许时）等 FullMessage 里的 `joinedNodeAff`。
- **其他 server join**：`initAffinityOnNodeJoin`——因为 server 事件全网都跑同一 function + 同一 discoCache，**每节点独立重算**即可保证一致（这正是 rendezvous 免中心化的收益）；client 事件走 `onClientEvent` → `clientEventTopologyChange`（§1.2）。
- **cache 在 exchange 里启动**：join 事件的 `initCachesOnLocalJoin`/`initStartedCaches`（L199 / L1355）在 `init` 的 L993-1002 被调——本地 join 走 `initCachesOnLocalJoin`，别人的 join 则把对方节点带上来的 cache 注册并启动（`startReceivedCaches`）；动态 cache 启停的 `ExchangeActions` 由 `DynamicCacheChangeBatch` 携带（§2.1），`onCacheChangeRequest` 决定 exchange 类型并准备各组的 holder（L1439-1468）。**关键点：新组的 affinity 计算与 cache 数据结构创建都发生在 exchange future 内部**（coordinator 算/下发 assignment，`initCoordinatorCaches` 在 L958/L990/L1005 三个分支都被调），所以"建 cache"天然与"达成分区一致"是同一次 PME——复刻时把 cache 启动挪出 exchange 会直接破坏 SINGLE/ALL 的消息内容完整性（新组必须出现在 SingleMessage 的 `parts` 里）。
- **exchange 里还做三件杂事**：write-behind store flush（L1661-1671）、schema 变更等 `PartitionsExchangeAware` 组件回调（L1040-1041 / L1079-1080）、集群激活/去激活的状态收尾（L3953-3982，`ChangeGlobalStateFinishMessage`）。复刻课可先留 `PartitionsExchangeAware` 空列表扩展点。

### 2.5 affinity-ready / topology future 如何挡住数据面

- **topology future**：`GridDhtPartitionTopologyImpl#topologyVersionFuture()`（`.../dht/topology/GridDhtPartitionTopologyImpl.java:333-346`）返回当前 exchange future（若其不改变 affinity 则返回上一个已完成的）。数据面在 `GridNearAtomicSingleUpdateFuture#mapOnTopology`（`.../dht/atomic/GridNearAtomicSingleUpdateFuture.java:385-418`）拿它：未完成就 `fut.listen` 挂回调返回、**用户线程不阻塞但操作排队**；完成后 `fut.validateCache(...)`（含丢失分区检查，§3.5）再取 `topVer` 做 affinity 映射——这就是 03 号报告 put 链第 4 步之前的"等 exchange"现场。
- **读 assignment 的三种姿势**（都在 `GridAffinityAssignmentCache`）：
  - `readyAssignments/readyAffinity(topVer)`（L627-633 / L749-768）——"必须已就绪"，head 不匹配且历史查不到就直接抛 `IllegalStateException`；
  - `cachedAffinity(topVer)`（L779-844）——**数据面标准读法**：先用 `exchange().lastAffinityChangedTopologyVersion` 把请求版本折算到"最后一次真正改分配的版本"（client 事件的 minor 版本分配没变，直接复用），再 `awaitTopologyVersion` 阻塞等待、随后从 head 或历史 `ceilingEntry` 取；请求的版本老到被历史收缩清掉则报错并提示调 `IGNITE_AFFINITY_HISTORY_SIZE`（L834-843）；
  - `readyFuture(topVer)`（§1.2）——非阻塞登记式等待。
  这三者的分工解释了为什么 03 号报告的 `nodesByKey` 在 exchange 期间不报错而是"等"：走的是 cachedAffinity 的 await 分支。
- **affinity ready future**：`GridCacheAffinityManager#affinityReadyFuture`（`cache/GridCacheAffinityManager.java:80-106`）把 readyFuture 暴露给缓存预加载等场景。

---

## 3. rebalance：GridDhtPreloader 一族

### 3.1 总体结构：demander / supplier 与两消息

- 每 group 的 preloader 是 `GridDhtPreloader`（`cache/CacheGroupContext.java:883` 创建并 start；client 节点同样创建但 rebalance 被短路，见 §4.3），内部两个协作者：`GridDhtPartitionSupplier`（响应别人的拉取）与 `GridDhtPartitionDemander`（主动拉）+ `startFut`（`.../dht/preloader/GridDhtPreloader.java:82-88`）。拓扑变化回调 `onTopologyChanged`（L164-168）：先 `supplier.onTopologyChanged()`（丢弃旧 supply 上下文）再 `demander.onTopologyChanged`。
- **`GridDhtPartitionDemandMessage`**（demander→supplier）：`rebalanceId`(L32，代次号，负值表示"清理远端上下文"，见 `GridDhtPartitionDemander.java:1477-1482`)、`parts`(L36，`IgniteDhtDemandedPartitionsMap`，分 **full** 与 **historical** 两类)、`timeout`(L40)、`workerId`(L44)、`topVer`(L48)。
- **`GridDhtPartitionSupplyMessage`**（supplier→demander）：`rebalanceId`(L47)、`topVer`(L51)、`last`(L55，分区→最终 update counter)、`missed`(L60，数据已不存在的分区)、`infos`(L64，`CacheEntryInfoCollection` 按 partition 分桶的 KV)、`msgSize`(L68)、`errMsg`(L72)。

### 3.2 触发与分配：exchange worker 是唯一发起点

- exchange future 完成后，`ExchangeWorker#body0` 检查 `rebalanceRequired(exchFut)`，按 `rebalanceOrder`（`CacheRebalanceOrderComparator`）倒序对每个 group 调 `grp.preloader().prepare(...)`（`GridCachePartitionExchangeManager.java:3257-3310`），把各组 `RebalanceFuture` **串成链**（前组完成后 `next.requestPartitions()` 下一组，`GridDhtPartitionDemander.java:1424`）。源码注释给出的链示例：`ignite-sys-cache -> cacheGroupR1 -> cacheGroupP2 -> cacheGroupR3`（L3290-3295）——**系统 cache 永远排最前**，因为 rebalance 本身要用它存元数据。无可拉数据时打 "Skipping rebalancing (nothing scheduled)"（L3319-3322）；无 affinity 变化时 "Skipping rebalancing (no affinity changes)"（L3326-3329）。这两个日志行本身就是绝佳的 tracer 断言点。
- `prepare` → `generateAssignments`（`GridDhtPreloader.java:401-418 / 171-322`）：对本节点在**理想 assignment** 中拥有、但状态非 OWNING/LOST 的分区（必须已是 MOVING，否则断言失败，L227-235）决定 supplier——持久化组优先从 exchange 指派的**历史 supplier**（WAL 增量）拉（L237-267），否则从现有 owner 里按 `p % owners.size()` 轮询挑一个做**全量** supplier（L270-293）。产出 `GridDhtPreloaderAssignments`：`ClusterNode → GridDhtPartitionDemandMessage`。
- `demander.addAssignments`（`GridDhtPartitionDemander.java:331-465`）：空分配直接完成 `syncFuture` 并返回（L358-369）；与进行中的分配**兼容**则复用（L373-378）；不兼容先 `tryCancel` 旧的（L381-382）；`rebalanceDelay > 0` 时挂超时对象延迟触发 `forceRebalance`（L431-462）。
- `RebalanceFuture#requestPartitions`（L1126-1221）：对每个 supplier，先把 full 集分区 `clearAsync` 清空（防半新半旧，L1189-1213），然后 `requestPartitions0` 发 DemandMessage（带 `rebalanceId` 与 `timeout`，L1179-1180）。

### 3.3 供应侧与消费侧

- **supplier**（`GridDhtPartitionSupplier.java`）：`processDemandMessage` 按 `rebalanceId` 比对 SupplyContext 新旧（L227-260），用 `IgniteRebalanceIterator` 遍历分区数据，**按字节预算切块**：`supplyMsg.messageSize() >= grp.preloader().batchSize()` 即回复并保存/新建上下文（L297-323）；分区数据缺失记 `missed`（L334-346）；一个分区发完记 `addLast(part, updateCounter)`（L362-364）；批间 sleep `throttle()`（L525-527，可被系统属性 `IGNITE_REBALANCE_THROTTLE_OVERRIDE` 覆盖，L80/L95）。
- **demander 消费**：SupplyMessage 到达经 `GridDhtPreloader#handleSupplyMessage`（L371-377）入 `registerSupplyMessage`——**historical 分区用 striped 线程池保序、full 分区用普通 rebalance 池可乱序**（`GridDhtPartitionDemander.java:470-488`）；每分区收完 `partitionDone`：无持久化时立即 `topology.own(part)`（MOVING→OWNING，L1502-1539），持久化组则由 exchange worker 在全部完成时 `finishPreloading` 批量 own（L1502-1511 注释）。
- **demand 的发送是"有序消息"**：`requestPartitions0` 用 `ctx.io().sendOrderedMessage(supplierNode, REBALANCE_TOPIC, msg, ioPolicy, msg.timeout())`（L1245）——同一 supplier 的 demand/supply 序列必须保序（supplier 靠 `rebalanceId` + SupplyContext 续传，乱序会破坏增量游标）；取消时发**负 rebalanceId** 的清理消息让 supplier 丢弃上下文（`cleanupRemoteContexts` L1471-1495）。
- **RebalanceFuture 内部状态机**：`RebalanceFutureState = INIT → STARTED（已发 demand）/ MARK_CANCELLED`（L897-907，CAS 更新 L928-936）；类 javadoc（L920-926 附近）写明两个终止值——`true`=组全部到位（触发 syncFuture 完成）、`false`=被不兼容的新 assignment 取消；持久化组 rebalance 期间**临时关本地 WAL**、checkpoint 后再批量 own（`ownPartitionsAndFinishFuture` L1274-1308：校验 rebalanceId 与 `lastAffinityChangedTopologyVersion` 后 `grp.localWalEnabled(true, true)` + `grp.topology().ownMoving()`）。
- **`syncFuture` 是独立字段**：demander 自己的 `syncFut`（L244-246）在空分配（L367）或 RebalanceFuture 完成时 onDone——`GridCacheProcessor#awaitRebalance` 等的就是它，与"是否真的搬过数据"解耦。
- **参数语义**（全局值覆盖 cache 级默认，`cache/GridCachePreloaderAdapter.java:186-206`）：`rebalanceBatchSize` 默认 **512 * 1024 字节**（按消息体积不是条数！`configuration/IgniteConfiguration.java:179`）；`rebalanceThrottle` 默认 **0**（L176）；`rebalanceTimeout` 默认 **10 秒**（L170，作为 demand/supply 消息超时）；`rebalanceDelay` 无初始化器、默认 **0**（`configuration/CacheConfiguration.java:356`）。

### 3.4 分区状态机与 RENTING/EVICT

`GridDhtLocalPartition` 的状态迁移由 `GridDhtPartitionTopologyImpl` 驱动，一条主链 + 两条支线：

```
              beforeExchange（理想拥有的新分区）
   (不存在) ────────────────────────► MOVING ──own()/ownMoving()──► OWNING
                                        │                              │
   不再属于理想分配: locPart.rent()      │ rebalance 失败/取消           │ 下一轮 exchange 判定不再拥有
   (L480 / L838, GridDhtLocalPartition#rent L697)                     │ locPart.rent()
                                        ▼                              ▼
  OWNING/MOVING ──rent()──► RENTING ──异步清空数据──► EVICTED（分区销毁，active()==false）
  全网无有效副本: coordinator 判 LOST（FullMessage.lostParts → 本地 markLost，L1600-1625 / L2447-2454）
```

- **RENTING 是"宽限租约"**：进入 RENTING 只表示"按理想分配我该交出这个分区"，但**数据还在、仍可读可供应**——`nodes0` 把 OWNING/MOVING/RENTING 三态都算进 owner（L1245/L1263），所以 rebalance 期间 demander 仍能从 RENTING 的老 owner 拉全量数据。清空是后台异步的，完成后才 EVICTED。
- **MOVING→OWNING 的两种节奏**：内存组逐分区 own（demander `partitionDone`，`GridDhtPartitionDemander.java:1510-1511`）；持久化组攒到 rebalance 结束由 exchange worker 批量 `ownMoving()`（L1274-1308），期间本地 WAL 暂关。

### 3.5 partition loss policy

丢失分区的**执法点在数据面入口**：`GridDhtTopologyFutureAdapter#validateCache`（`.../dht/GridDhtTopologyFutureAdapter.java:100-148`）——`READ_ONLY_SAFE/READ_ONLY_ALL` 拒绝写；`IGNORE` 连读也不限（交给用户）；持久化部署强制 `READ_WRITE_SAFE`（`PartitionLossPolicy.java:77` 注释）。因为 `mapOnTopology` 必调 `validateCache`（§2.5），丢失分区在 put/get 映射阶段即被拦截，无需数据路径额外检查。恢复靠 `resetLostPartitions`（`CacheAffinityChangeMessage`/`DynamicCacheChangeBatch` 的 exchange actions，exchange future L2106-2115 / L3808-3825）。

### 3.6 SYNC/ASYNC/NONE 与 join 阻塞

`CacheRebalanceMode`（`cache/CacheRebalanceMode.java:36-53`）：**SYNC**=cache 不就绪前操作阻塞；**ASYNC**=cache 立即就绪、后台拉数据（**默认值**，`CacheConfiguration.java:146`）；**NONE**=不 rebalance。join 阻塞实现在 `GridCacheProcessor#onKernalStart`：`awaitRebalance(joinVer).get()`（`cache/GridCacheProcessor.java:711`），其中 `awaitRebalance`（L729-739）只筛 **affinity 节点 + SYNC 模式 + 本次 join 启动** 的 cache，对它们等待 `ctx.preloader().syncFuture()`（`GridDhtPreloader#syncFuture` L428-429：client 返回 `startFut`，server 返回 `demander.syncFuture()`，即 RebalanceFuture 链的完成）——与 01 号报告结论一致：**只有 SYNC cache 才会让节点 join 卡在 rebalance 上，ASYNC 不卡**。

---

## 4. cache mode、cache group 与 client

### 4.1 REPLICATED = PARTITIONED + backups=∞

`initializeConfigDefaults` 把 REPLICATED 的 `backups` **强制改写为 `Integer.MAX_VALUE`**（`cache/GridCacheUtils.java:1669-1670`），且默认 affinity 换成 **512 分区**的 Rendezvous（L1658-1663；PARTITIONED 默认 1024 分区，L1653-1656）。于是 `assignPartition` 走 `replicatedAssign`：每分区 primary=排序首位、其余节点全为 backup（§1.1），`DFLT_BACKUPS = 0`（`CacheConfiguration.java:118`）只对 PARTITIONED 有意义。读写路径上 REPLICATED 与 PARTITIONED **没有独立实现类**，仅 `nodes0` 短路（§1.3）、preloader 的 owner 集合是全体节点这两处体现差异。

### 4.2 cache group：共享 topology/affinity/preloader 的容器

`CacheGroupContext` 是共享单元：`affinity()`（L753）返回**组级** `GridAffinityAssignmentCache`、`topology()`（L677）返回组级 `GridDhtPartitionTopology`、`preloader()`（L315）返回组级 preloader；同组多个 cache 通过 `addCacheContext` 挂进 `caches` 列表（L353-364），共享同一套分区与 rebalance（SupplyMessage 的 entry 自带 `cacheId`，supplier L352-360 按 cache 分桶，写回时按 cacheId 落到各自 cache）。未分组的 cache 一组一 cache。交换消息（Single/Full）全部以 **groupId** 为 key，天然按组交换。同组各 cache 的 affinity function/backups/partitions 必须一致（`ClusterCachesInfo` 的组属性校验，如 `ClusterCachesInfo.java:2590-2591`）。

### 4.3 client 的三件事

1. **affinity**：client 不跑 function 参与分配（discoCache 的 affinity 节点集合只含 server）。它通过 exchange 拿 assignment：join 时在 SingleMessage 带 `grpsAffReq`（exchange future L2137-2138），coordinator 回的 FullMessage 附 `joinedNodeAff`（L2206-2222+），client 端初始化自己的 `GridAffinityAssignmentCache`；旧协议/公共 `Affinity` API 则走 `GridDhtAssignmentFetchFuture` 拉（`CacheAffinitySharedManager.java:1865`），server 侧 handler 只在非 client 节点注册（`CacheGroupContext.initializeIO` L878-881）。公共 API 层 `GridAffinityProcessor#affinityCacheFuture`：本地有 cache 用本地 assignment，否则 `remoteAffinityInfo` 向远端节点发闭包取（`.../affinity/GridAffinityProcessor.java:412-524`）。
2. **exchange**：client join/left 触发 `ExchangeType.CLIENT`——只向 coordinator 发一条**无分区数据、`client=true`** 的 SingleMessage（`clientOnlyExchange`，exchange future L1567-1606；消息构造 L2117-2121），**client 也收 FullMessage** 并更新 `GridClientPartitionTopology`（`GridCachePartitionExchangeManager` 的 `clientTops` map，L224；无本地组上下文的 grpId 走它，L1782）。
3. **rebalance 与 put**：client 不 rebalance——`syncFuture()` 返回 `startFut`、`rebalanceFuture()` 直接完成（`GridDhtPreloader.java:428-435`）。put 路径与 03 号报告完全同链（`GridDhtAtomicCache` 同款类），区别仅在于 client 永远不是 primary：`mapSingleUpdate` 算出的 owner 必是 server 节点，`sendSingleRequest` 走网络分支（03 号报告 §5 的分布式路径）。
4. **掉队与重连**：client 的 exchange 信息可能已被 server 侧 exchange 历史清掉（默认保留 1000 条，§2.1），此时 coordinator 会 `forceClientReconnect` 让 client 重走 discovery（`GridCachePartitionExchangeManager.java:1876-1890`）；client 与集群失联则各类 future 以 `IgniteNeedReconnectException` 完成（exchange future L1098-1113），重连后由 reconnect exchange future 重建状态（L738-773）。复刻课的 fat-client 课应把"client 重连"列为验收场景之一。

### 4.4 一条贯穿三件套的时间线（新 server join，供 tracer 设计）

把上面所有引用串成一个端到端剧本（全部步骤均有上文引用支撑）：

1. discovery 完成 join，`onDiscoveryEvent` 建 `GridDhtPartitionExchangeId(新节点id, JOINED, topVer=N)` 并入队（§2.1）；
2. 老节点 worker 取出 future，`init` 判定类型：coordinator=最老 server；非 coordinator 的老 server 走 `onServerJoin→initAffinityOnNodeJoin` **各自重算** ideal assignment（§2.4）；新节点自身是本地 join，非 crd 时 fetch/joinedNodeAff 拿 assignment（§2.4）；
3. `distributedExchange`：两阶段分区释放 → `topology().beforeExchange` 把新拥有分区置 MOVING → 发 SingleMessage 给 coordinator（§2.2）；
4. coordinator 收齐：算 update counters、`assignPartitionsStates`（counter 落后强制 MOVING）、构造 FullMessage（含各组的分区全表、histSuppliers、是否 rebalanced）广播，`onDone`（§2.2）；
5. 各节点应用 FullMessage：`node2part` 全表替换、`diffFromAffinity` 建表（late 差异），exchange future 完成 → topology version future 完成 → **数据面排队中的 put 恢复映射**（§2.5）；
6. worker 的 `afterExchange` + `rebalanceRequired` → 各组 `prepare → generateAssignments`（新节点的 MOVING 分区找 owner 当 supplier）→ RebalanceFuture 链 `requestPartitions` 发 DemandMessage（§3.2）；
7. supplier 按 512KB 批回 SupplyMessage，demander 收完一个分区：内存组立即 own（MOVING→OWNING），持久化组等批量 own（§3.3）；老节点不再拥有的分区进入 RENTING、异步清空（§3.4）；
8. 新节点组内分区齐了 → coordinator `checkRebalanceState` 发现某组 wait 清空 → `CacheAffinityChangeMessage` → 轻量 exchange → `aff.initialize(ideal)`，**primary 正式切给新节点**，`diffFromAffinity` 收缩为空（§1.3）；
9. 若某 cache 是 SYNC 模式：新节点 `onKernalStart` 的 `awaitRebalance(joinVer).get()` 在第 7 步完成前一直阻塞（§3.6）——ASYNC 则早已放行。

这条时间线就是把 8 门课的 tracer 拼成一章毕业测验的现成剧本。

---

## 5. 分级结论：章 4 切课建议与可简化项

### 5.1 切课建议表（每课一个概念簇，2–4 小时 agent 实施）

| # | 课 | 概念簇（新东西） | tracer（验证手段） | 依赖 |
|---|---|---|---|---|
| 1 | Rendezvous affinity function | `hash`/`calculatePartition`/`assignPartition`/`AffinityFunctionContext`；`AffinityTopologyVersion` 主/次版本语义 | 单测：固定 consistentId 集合 + 分区数，断言分配稳定、primary=最小权重、备份复制语义与 03 号报告链路衔接 | 03 号数据面（需要 nodesByKey 消费者） |
| 2 | assignment cache | `affCache`/`idealAssignment`/`head`/`readyFuts`/历史收缩；`clientEventTopologyChange` 浅拷贝 | 单测：注入拓扑事件序列，断言 ready future 完成顺序与历史条数上限 | 课 1 |
| 3 | exchange 骨架（server 全量） | `GridDhtPartitionExchangeId`、ExchangeWorker 单线程队列、`init` 两枚举、`distributedExchange` 释放两阶段、Single/Full 消息、coordinator 收齐-广播 | 双节点集成：kill 一个 server，断言另一节点 exchange future 完成且分区图一致 | 课 2 |
| 4 | exchange 高级路径 | merge 协议（resTopVer）、`onNodeLeft`→`BECOME_CRD`→SingleRequest 恢复、exchangeFreeSwitch、custom 事件（cache 启动/state change） | 三节点集成：exchange 进行中杀 coordinator，断言新协调者完成交换 | 课 3 |
| 5 | 分区状态机 + late affinity | `GridDhtPartitionState` 五态、RENTING/EVICT、`diffFromAffinity`、`idealAffDiff`、`CacheAffinityChangeMessage` 切换 | 集成：join 新节点，断言 put 在 rebalance 完成前后路由到不同 primary（diff 消失） | 课 3、课 6 |
| 6 | rebalance | demander/supplier、Demand/Supply 消息、batchSize/throttle/timeout、SYNC/ASYNC/NONE 与 `awaitRebalance` | 集成：预填数据后 join 新节点，SYNC 模式断言 join 期间 get 不可见半量数据、完成后可见 | 课 3（复用 exchange 完成回调） |
| 7 | REPLICATED 与 cache group | backups=∞、512 分区默认、`CacheGroupContext` 共享三件套、多 cache 一组的 entry 分桶 | 集成：同组两 cache，断言一次 rebalance 同时供两 cache、分区图仅一份 | 课 1、6 |
| 8 | client cache 语义 | CLIENT 型 exchange、`grpsAffReq`/`joinedNodeAff`、clientTops、不 rebalance、put 转发（衔接 03 号 §5） | 集成：fat client join + put/get，断言 client 从未成为 owner 且断连 server 后重连恢复 | 课 3、4、6 |

课时估计 8 课；课 5 与 6 顺序可互换（状态机偏 rebalance 侧的可在课 6 一并做，见 5.2）。课间依赖图：

```
03 数据面(put) ──► 课1 function ──► 课2 assignment cache ──► 课3 exchange 骨架
                                                                │      │
                                              ┌─────────────────┘      └──────┐
                                              ▼                               ▼
                                          课6 rebalance ◄──(afterExchange回调)─┤
                                              │                               ▼
                                              └──► 课5 状态机+late ◄──────── 课4 exchange 高级
                                                        │
                                                        ▼
                                              课7 replicated/group ──► 课8 client
```

关键耦合点（源码实测，切课时不可拆散的对子）：课 3 与课 6 共享 `afterExchange`→`prepare` 这一个接缝（§5.2 第 3 条）；课 5 的 `idealAffDiff` 依赖课 4 的 merge 协议分支（非 merge 路径下 server 离开也走 `initAffinityOnNodeLeft`，exchange future L3887-3896）；课 8 的 `grpsAffReq`/`joinedNodeAff` 依赖课 4 的 merge 分支（`ExchangeContext` 构造器里 `fetchAffOnJoin` 仅在 crd 本地 join 的旧协议为 true，`cache/ExchangeContext.java:79-82`）。

### 5.2 可简化项判断（默认全做的全保真裁决下，仅列真正可争议项）

| 项 | 源码体量 | 教学价值 | 建议 |
|---|---|---|---|
| historical（WAL 增量）rebalance | `partHistSuppliers`/`IgniteHistoricalIterator`/striped 池一大片（supplier L237-267、demander L470-488 等） | 依赖持久化/WAL 章，先行实现会引入反向依赖 | **最可争议**：主课只做 full rebalance，historical 降为附录课/作业（衔接持久化章后补） |
| merge 协议 | `mergeExchanges`/`resTopVer`/`finishMerged` 贯穿 exchange future 与 manager | 理解"多个事件一次交换"对生产必要，教学上属优化 | 可降为作业（课 4 内标注），但 `onNodeLeft` 恢复路径建议保留 |
| baseline topology 分支 | `GridAffinityAssignmentCache.calculate` 的 L349-399、`recalculateBaselineAssignment` | 与持久化强耦合 | 附录（与历史 rebalance 同批） |
| partition loss policy | 很小（一个枚举 + validateCache 分支 + lostParts 应用） | 高——丢失语义是分布式 cache 核心承诺 | **保留**（课 5 内，半天足够） |
| rebalance throttle/batchSize/timeout | 小（几个访问器 + supplier L297-327、L525-527） | 中——默认值下基本不感知 | 保留但 tracer 不必覆盖 throttle>0 |
| `exchangeFreeSwitch`（PME-free） | 中（ExchangeContext L73-78 + init 分支） | 低——纯性能优化路径 | 可简化为"直接走 ALL"并在课 4 注明差异 |
| excludeNeighbors / backupFilter | 小（assignPartition L391-439） | 中——体现 filter 扩展点 | 保留 primary 路径，filter 族作为课 1 作业 |
| discovery custom 事件族（snapshot/WAL state） | 各自入口很小 | 低（snapshot/WAL 各有专章） | 章内 stub 掉，留 TODO 对接各专章 |

不可简化（结构性）：两枚举状态机、Single/Full/SingReq 三消息、coordinator 收齐-广播模型、五态分区状态机、`diffFromAffinity` late 机制、`awaitRebalance` 的 SYNC 过滤——它们互相咬合，抽掉任何一个数据面/控制面就对不上。

---

## 引用文件清单（均实际打开验证，相对 `vendors/ignite/`）

```
modules/core/src/main/java/org/apache/ignite/cache/affinity/rendezvous/RendezvousAffinityFunction.java
modules/core/src/main/java/org/apache/ignite/cache/CacheRebalanceMode.java
modules/core/src/main/java/org/apache/ignite/cache/PartitionLossPolicy.java
modules/core/src/main/java/org/apache/ignite/configuration/CacheConfiguration.java
modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java
modules/core/src/main/java/org/apache/ignite/internal/IgniteNodeAttributes.java
modules/core/src/main/java/org/apache/ignite/internal/IgniteKernal.java
modules/core/src/main/java/org/apache/ignite/internal/managers/discovery/GridDiscoveryManager.java
modules/core/src/main/java/org/apache/ignite/internal/processors/affinity/AffinityTopologyVersion.java
modules/core/src/main/java/org/apache/ignite/internal/processors/affinity/GridAffinityAssignmentCache.java
modules/core/src/main/java/org/apache/ignite/internal/processors/affinity/GridAffinityFunctionContextImpl.java
modules/core/src/main/java/org/apache/ignite/internal/processors/affinity/GridAffinityProcessor.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/CacheAffinitySharedManager.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/CacheGroupContext.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/ClusterCachesInfo.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/ExchangeContext.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheAffinityManager.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCachePartitionExchangeManager.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCachePreloader.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCachePreloaderAdapter.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheProcessor.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheUtils.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTopologyFutureAdapter.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/atomic/GridNearAtomicSingleUpdateFuture.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/CacheGroupAffinityMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionDemandMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionDemander.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionExchangeId.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionSupplier.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionSupplyMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPreloader.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsAbstractMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsExchangeFuture.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsFullMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsSingleMessage.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsSingleRequest.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/InitNewCoordinatorFuture.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridDhtLocalPartition.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridDhtPartitionState.java
modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridDhtPartitionTopologyImpl.java
```

（`IgniteSystemProperties.java`、`latch/ExchangeLatchManager.java`、`GridClientPartitionTopology` 仅经上述文件内的引用间接核验存在，未单开。）

---

## 对复刻课的启示

1. **控制面是"单线程大脑 + 消息收敛"**：`ExchangeWorker` 一根线程跑完所有 exchange，其余全部并发都挂在它派生的 future 上。复刻时应先立这根线程与 futQ 的骨架，再往里填阶段——顺序反了会陷入并发泥潭。
2. **affinity 的三层解耦是 2.18 最值得抄的设计**：纯函数（`AffinityFunction`）× 版本化缓存（`GridAffinityAssignmentCache`）× 生效差量（`diffFromAffinity` + `CacheAffinityChangeMessage`）。三者接口极窄，正好一课一层；late assignment"不可关闭"这个事实让复刻可以**只实现一条路径**，比旧文档描述的 eager/late 双模式省一半心智。
3. **exchange 与 rebalance 的接缝只有两个调用点**：`preloader().onTopologyChanged()`（停旧的）与 `prepare()`（排新的）。课 3 与课 6 的分界线就画在这里，两课团队可以并行。
4. **消息即协议**：Single/Full/SingleRequest/Demand/Supply 五种消息的字段就是协议规格书，建议每课 tracer 直接断言消息字段（rebalanceId、client 标志、lostParts、idealAffDiff），比断言行为更能锁住保真度。
5. **client 是"缩小参与度的 server"**：同一套类、同一套消息，差异只在 ExchangeType.CLIENT、`syncFuture()=startFut`、assignment 下载三个开关上——复刻时不要为 client 写平行实现，把这三个开关做成显式分支即可。
