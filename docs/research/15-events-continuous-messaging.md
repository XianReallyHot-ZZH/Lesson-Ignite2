# 15 · 事件与消息体系：GridEventStorageManager、Continuous Query、GridContinuousProcessor、IgniteMessaging

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本弧覆盖三个子系统：本地/远程**事件**（`events()` facade 背后的 `GridEventStorageManager` + `EventStorageSpi`）、**continuous query**（JCache `CacheEntryListener` 与 `ContinuousQuery` 共用的 `cache/query/continuous/` 包）、**消息**（`IgniteMessaging`），以及把三者串起来的**通用 routine 基础设施** `GridContinuousProcessor`。正文短路径（`continuous/`、`eventstorage/` 等）是 `org/apache/ignite/internal/` 下的缩写；**文末"引用文件清单"给出全部完整路径**（均已逐一打开验证），行号以当前 submodule 内容为准。

---

## 0. 先决事实（纠正四个"想当然"）

1. **`IgniteEvents.remoteListen` 不走 compute closure**。它构造 `GridEventConsumeHandler` 后调 `ctx.continuous().startRoutine(...)`（`internal/IgniteEventsImpl.java` L135–144）——与 continuous query、messaging remoteListen 共用同一套 routine 基础设施。事件查询 `remoteQuery` 才走独立的 `GridEventStorageMessage`/`TOPIC_EVENT` 请求-响应通道（见 §1.4）。
2. **`MemoryEventStorageSpi` 不是环形缓冲**。它是 `ConcurrentLinkedDeque8` FIFO 双端队列 + "超量出队 / 超龄摘除"的惰性清理（`spi/eventstorage/memory/MemoryEventStorageSpi.java` L116、L272–L301），默认 `DFLT_EXPIRE_COUNT = 10000`、`DFLT_EXPIRE_AGE_MS = Long.MAX_VALUE`（L100、L103）。
3. **2.18 没有 `excludeEventTypes` 配置，CQ 也没有 `BackupFilter` API**。配置侧只有 `includeEventTypes`（`configuration/IgniteConfiguration.java` L2874 `getIncludeEventTypes()`；全文件 grep 无 exclude*）；运行时开关是 `enableEvents`/`disableEvents`（§1.3）。backup 事件的处理藏在 handler 内部的 primary/backup 分叉（§2.4），不是用户可见的过滤器。
4. **2.18 的 handler 族只有三个实现**：`CacheContinuousQueryHandler`（含 V2/V3 子类）、`GridEventConsumeHandler`、`GridMessageListenHandler`（grep `implements GridContinuousHandler` 全 core 仅三处命中）。老文档里的 `CacheEntryLsnrGroupHandler`/`CacheEntryLsnrHandler` 类名已不存在；cache 侧的"监听器"是另一个接口 `CacheContinuousQueryListener`（§2.3）。

---

## 0.5 全景：三个 API 落到两条通道 + 一套地基

```
用户 API                    内部组件                              传输通道
─────────────────────────────────────────────────────────────────────────────
events().localListen   →   GridEventStorageManager.lsnrs        （本地，无网络）
events().remoteQuery   →   GridEventStorageMessage 请求-响应      TOPIC_EVENT（一次性 RPC）
events().remoteListen  →   GridEventConsumeHandler ─┐
cache.query(CQ/JCache) →   CacheContinuousQueryHandler ─├→ GridContinuousProcessor（routine）
message().remoteListen →   GridMessageListenHandler ─┘      ├ StartRoutineDiscoveryMessageV2（discovery 广播）
message().send/localListen → GridIoManager 用户消息          │ ContinuousRoutineStartResultMessage（TOPIC_CONTINUOUS）
                              （TOPIC_COMM_USER，不经过 routine） ├ GridContinuousMessage(MSG_EVT_NOTIFICATION)（有序 topic 或 TOPIC_CONTINUOUS）
                                                                   └ MSG_EVT_ACK / CacheContinuousQueryBatchAck
```

要点：`GridContinuousProcessor` 是三个"订阅类" API 的公共地基（部署、生命周期、批量、保序）；`send`/`remoteQuery` 这两个"即时类" API 各走各的直达通道，互不纠缠。

---

## 1. GridEventStorageManager：本地事件总线 + 事件存储

### 1.1 SPI 两个实现与默认值

- `EventStorageSpi` 只有两个实现。**默认是 `NoopEventStorageSpi`**：`IgnitionEx` 在用户未配置时 `cfg.setEventStorageSpi(new NoopEventStorageSpi())`（`internal/IgnitionEx.java` L2043–2044），其 `record` 为空操作、`localEvents` 返回空集（`spi/eventstorage/NoopEventStorageSpi.java` L35–42）。因此默认配置下 `IgniteEvents.localQuery` 会抛错提示改配 `MemoryEventStorageSpi`（`internal/managers/eventstorage/GridEventStorageManager.java` L915–924）。
- `MemoryEventStorageSpi.record`：可配 `filter` 谓词决定收不收（L251–264），入队前 `cleanupQueue()`——先按数量超限从头 `poll`（L275–282），再按时间超龄从头部 `unlinkx`（L284–300）。即**事件历史是一个有界 FIFO**，不是 ring buffer 数组。

### 1.2 本地监听注册的数据结构

- 核心就一张表：`ConcurrentMap<Integer /*eventType*/, Listeners> lsnrs`（`GridEventStorageManager.java` L96）。`Listeners`（L1288–1356）内部分两组：`highPriorityLsnrs`（有序 List，按 `HighPriorityListener.order()` 排序，volatile 整体替换）与普通监听的 `GridConcurrentLinkedHashSet`。
- 三种监听被 `ListenerWrapper` 适配：内部 `GridLocalEventListener`（L1382）、带 `DiscoCache` 参数的 `DiscoveryEventListener`（L1430，由 DiscoveryManager 专用重载 `record(DiscoveryEvent, DiscoCache)` L318 投递）、用户 `IgnitePredicate`（`UserListenerWrapper` L1480）。用户谓词 `apply` 返回 **false 即自动注销自己**（L1497–1500）。
- 注册路径：`addLocalEventListener`（L631 用户版 / L652 内部版）→ `addEventListener` → `registerListener`（L751–767，往对应 type 的 `Listeners` 挂 wrapper；若该 type 未启用会 warn L765–766）。`IgniteConfiguration.getLocalEventListeners()` 配置的监听在 `start()` 时统一注册（L282–291）。一次性等待用 `waitForEvent`（L857–872，包装成 future 的自注销监听）。对外注册面一览：

| 入口 API | 落到 manager 的方法 | wrapper |
|---|---|---|
| `IgniteEvents.localListen(pred, types)`（`IgniteEventsImpl.java` L298–310） | `addLocalEventListener(pred, types)` L631 | `UserListenerWrapper`（false 即注销） |
| 内部组件（cache/discovery/continuous 等） | `addLocalEventListener(GridLocalEventListener, ...)` L652 / `addDiscoveryEventListener` L681 | `LocalListenerWrapper` / `DiscoveryListenerWrapper` |
| 配置 `setLocalEventListeners` | `start()` L282–291 | 同用户版 |
| `IgniteEvents.waitForLocal(filter, types)`（L234–247） | `waitForEvent` L857 | 自注销内部监听 |

### 1.3 事件启用：includeTypes 如何决定 fire 与否

- 构造器把事件启用状态预计算成两个布尔数组（L138–207）：`recordableEvts[]`——内部事件**恒为 true**（`isInternalEvent` L520–535：全部 discovery 事件 + `EVT_TASK_FINISHED/FAILED`、`EVT_JOB_MAPPED`、集群激活等）加上用户经 `includeEventTypes` 启用的类型；`userRecordableEvts[]`——只有用户启用的类型。事件类型 ID 上限 1000（L166–168 断言）。
- 两个隐藏事件 `EVT_NODE_METRICS_UPDATED`、`EVT_DISCOVERY_CUSTOM_EVT` 永不下沉到 SPI（`isHiddenEvent` L507–509），只做本地通知。
- 判定 API：`isRecordable(type)`（通知层面，L557–561）与 `isUserRecordable(type)`（存储层面，L543–547）；type 超出数组长度时退化为对排序数组 `inclEvtTypes` 的查找（L608–623）。**fire 与否由调用方先查 `isRecordable` 再决定是否构造事件**（§1.5），manager 的 `record0` 只是兜底告警（L340–343）。
- 运行时开关 `enableEvents`/`disableEvents`（L387–422 / L429–472）：修改数组后 volatile 整体发布；disable 不能关掉配置文件里启用的类型（L442–443）。

### 1.4 record、本地/远程查询、remoteListen 的真实通道

- `record(Event)` → `record0`（L308 → L328–361）：`recoveryMode` 直接丢（L331）；`isUserRecordable && !isHiddenEvent` 时下沉 SPI（L346–353）；`isRecordable` 时 `notifyListeners`（L355–356，先高优先级后普通，L879–909）。
- **本地查询**：`localEvents(p)` 直接委托 SPI（L915–940）。
- **远程查询（`IgniteEvents.remoteQuery`）**：`IgniteEventsImpl.remoteQuery` L81–94 → `remoteEventsAsync`（L948–967，丢进公共池异步化）→ 私有 `query(p, nodes, timeout)`（L976–1136）。流程：序列化 filter（L1069）→ P2P 部署 filter 类（`ctx.deploy().deploy` L1071–1074）→ 构造 `GridEventStorageMessage(resTopic, filterBytes, clsName, depInfo...)`（L1076–1083）→ 经 `TOPIC_EVENT` 用 `sendToGridTopic` 发给全部目标节点（L1085；`sendMessage` L1147–1161 区分本地/远端）。响应 topic 是 `TOPIC_EVENT.topic(IgniteUuid.fromUuid(localNodeId()))`（L1059），临时注册 `GridMessageListener` 收响应（L1013–1057）、临时注册 `EVT_NODE_LEFT/FAILED` 监听在节点掉线时提前完成 future（L1000–1011、L1062–1065），`wait/notifyAll` 聚合（L1096–1115）。远端节点的 `RequestListener`（L1176–1283）在 `TOPIC_EVENT` 上收请求：`getGlobalDeployment` 拿部署类加载器（L1215–1223）→ 反序列化 filter + 资源注入（L1229–1232）→ `localEvents(filter)`（L1235）→ 组装 `GridEventStorageMessage(events, ex)` 回发 `sendToCustomTopic`（L1257–1268）。**这是一条一次性请求-响应协议，与 continuous routine 无关**。
- **remoteListen（`IgniteEvents.remoteListen`）**：见 §3——它就是一条 routine。默认参数 `(bufSize=1, interval=0, autoUnsubscribe=true)`（`IgniteEventsImpl.java` L113–116），handler 是 `GridEventConsumeHandler`。该 handler 在**远端节点**注册 `GridLocalEventListener`（`internal/GridEventConsumeHandler.java` L182–289：先跑用户 filter `filterDropsEvent` L300–311，再 `ctx.event().addLocalEventListener(lsnr, types)` L287）；master 是本机则直接调 `cb.apply`（返回 false → `stopRoutine`，L192–195）；否则把事件包成 `EventWrapper` 经 `ctx.continuous().addNotification(...)` 发回 master（L250–251），master 端 `notifyCallback`（L348 起）解包调用用户回调。
- `remoteQuery` 协议时序（复刻对照用）：

| # | 方向 | 动作 | 位置 |
|---|---|---|---|
| 1 | master | marshal filter、P2P 部署、构造 `GridEventStorageMessage` | `GridEventStorageManager.java` L1069–1083 |
| 2 | master → nodes | `TOPIC_EVENT` 发请求（本地走内存直投，远端 marshal responseTopic） | L1085、L1147–1161 |
| 3 | node | `RequestListener.onMessage`：恢复部署、unmarshal filter、资源注入 | L1212–1232 |
| 4 | node | `localEvents(filter)` 查 SPI 存储 | L1235 |
| 5 | node → master | `GridEventStorageMessage(events, ex)` 回发到临时 response topic | L1257–1268 |
| 6 | master | response listener 聚合事件、去重、异常短路 | L1013–1057 |
| 7 | master | `EVT_NODE_LEFT/FAILED` 监听把失联节点移出等待集 | L1000–1011、L1062–1065 |
| 8 | master | 超时/齐活后清理两个临时监听 | L1129–1133 |

### 1.5 EVT_CACHE_OBJECT_PUT 的 fire 链（写入路径实例）

沿用 [03 号报告](03-data-plane-put-path.md) 的 put 链路（`GridDhtAtomicCache#update → updateSingle → entry.innerUpdate`）：

1. **fire 点在 entry 层**。`GridCacheMapEntry.innerUpdate`（ATOMIC 路径，签名 `internal/processors/cache/GridCacheMapEntry.java` L1433）内，写入成功后：`if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) cctx.events().addEvent(...)`（L1635–1653）；事务路径的 `innerSet`（签名 L975）在 L1127–1143 有对称代码。`evt` 参数由上层传入（事件开关），**先查 `isRecordable` 再构造事件**是全 cache 的惯例。
2. **同一点还并行触发 continuous query**：`innerUpdate` 在同一把 entry 锁内调 `cctx.continuousQueries().onEntryUpdated(lsnrs, key, ...)`（L1687–1699，注释明确 "Continuous query filter should be perform under lock"；`innerSet` 对应 L1145–1157）——所以 §2 的事件流与 §1 的 fire 是同源同点。
3. **`GridCacheEventManager.addEvent`**（`internal/processors/cache/GridCacheEventManager.java` L265–347）：再查一次 `isRecordable`（L283–284，`GridCacheEventManager.isRecordable` L382–398 要求 `userCache() && gridEvents().isRecordable(type) && !cfg.isEventsDisabled()`）；内部 key 不发事件（L287）；key/val 按需 `unwrapBinaryIfNeeded`，失败则整体降级 keepBinary（L305–322）；最后 `cctx.gridEvents().record(new CacheEvent(...))`（L328–345）→ `GridEventStorageManager.record0` → SPI 存储 + 本地监听通知。
4. CQ 监听命中时还会额外记录查询读事件 `EVT_CACHE_QUERY_OBJECT_READ`（`cache/query/continuous/CacheContinuousQueryHandler.java` L1077–1111），`EVT_CACHE_QUERY_EXECUTED` 在 routine 注册完成时记录（L682–709）。

fire 点双通道小结（同一点分叉，复刻时作为集成点）：

```
GridCacheMapEntry.innerUpdate / innerSet（entry 锁内）
 ├─ events 通道: cctx.events().addEvent(EVT_CACHE_OBJECT_PUT, ...)
 │    → GridCacheEventManager.addEvent → new CacheEvent
 │    → GridEventStorageManager.record0 → SPI 存储 + 本地监听（§1.4）
 └─ CQ 通道:   cctx.continuousQueries().onEntryUpdated(lsnrs, ...)
      → CacheContinuousQueryManager.onEntryUpdated → CacheContinuousQueryEvent
      → CacheContinuousQueryListener.onEntryUpdated（primary/backup 分叉，§2.4）
```

---

## 2. CacheContinuousQueryManager：JCache 桥与事件转换

类：`internal/processors/cache/query/continuous/CacheContinuousQueryManager.java`（每 cache 一份，`GridCacheManagerAdapter`）。

### 2.1 数据结构与注册面

- `lsnrs: ConcurrentMap<UUID /*routineId*/, CacheContinuousQueryListener>`（L115）+ 内部专用的 `intLsnrs`（L121）+ 计数器（L118/L124，`updateListeners` L300–314 用计数避免无监听时空转）；JCache 配置监听表 `jCacheLsnrs: ConcurrentMap<CacheEntryListenerConfiguration, JCacheQuery>`（L130–131）；topic 前缀 `"CONTINUOUS_QUERY" + "_" + cacheName`（L162）与自增 `seq`（L127）拼出每个 routine 的有序 topic。
- `registerListener`（L964–1010）由 handler 在远端注册时回调：内部监听直插 `intLsnrs`；用户监听要拿 `CacheGroupContext.listenerLock()` 写锁（L983–1001），首个监听到达时把 cache 挂进 shared group（L993–996）。

### 2.2 JCache `CacheEntryListenerConfiguration` 桥

- cache 启动时（`onKernalStart0`）遍历 `cctx.config().getCacheEntryListenerConfigurations()` 逐个 `executeJCacheQuery(cfg, onStart=true, ...)`（L187–192）。
- `JCacheQuery.execute()`（L1079–1161）：工厂 `create()` 出 `CacheEntryListener`（L1083），按其实现的子接口拼 types 位标志（CREATED/UPDATED/REMOVED/EXPIRED_FLAG，L100–109、L1090–1096）；包一层 `JCacheQueryLocalListener`（L1100–1102）——它实现 `CacheEntryUpdatedListener`，在 `onUpdated` 里按事件类型分派到 `onCreated/onUpdated/onRemoved/onExpired`（L1203–1243）；有 filter factory 用 `CacheContinuousQueryHandlerV2`，否则把 filter 包进 `JCacheQueryRemoteFilter(filter, types)` 用 V1 handler（L1111–1147，该 filter 的 `evaluate` 先查 types 位再跑用户 filter，L1303–1312）。批量参数取 `ContinuousQuery` 默认值（L1152–1154）。

### 2.3 ContinuousQuery 的执行入口与 initialQuery

- 用户 `cache.query(ContinuousQuery)` 落到 `IgniteCacheProxyImpl.queryContinuous`（`internal/processors/cache/IgniteCacheProxyImpl.java` L629–744）：校验 initialQuery 不能是 CQ（L634–637）、localListener 与 remoteFilter(Factory) 至少其一（L651–659），然后 `ctx.continuousQueries().executeQuery(locLsnr, locTransLsnr, rmtFilter, filterFactory, transFactory, pageSize, timeInterval, autoUnsubscribe, loc, keepBinary, includeExpired)`（L682–693）。
- **initialQuery 时机**：routine 启动成功后**立即同步执行** `query(qry.getInitialQuery())`（L695–697），其游标被包进返回的 `QueryCursorEx`；`close()` 该游标 = `stopRoutine(routineId)`（L708–718）；initialQuery 抛错则启动的 routine 会被停掉（L734–736）。即顺序是 **routine 先生效 → 再跑初始快照**（新事件可能与之重叠，由上层语义容忍）。
- 三种"存量数据"机制不要混淆：用户 `initialQuery`（任意 Scan/SQL 查询，master 本地执行，上段）；内部查询的 `notifyExisting`（启动后直接迭代 offheap 逐条 `locLsnr.onUpdated`，manager L775–845，事件 counter 为 0 且会过 handler filter，L822–839）；以及 `existingEntries`（供平台互操作等直接拉 `CacheEntryEventImpl` 快照，L900–956，`getOldValue()` 恒空、counter 恒 0，L1404–1416）。
- `executeQuery`（manager L515–590）按有无 transformer/filterFactory 选 `CacheContinuousQueryHandlerV3/V2/V1`（L529–577），三者 topic 都是 `TOPIC_CACHE.topic(topicPrefix, localNodeId, seq.getAndIncrement())`。
- `executeQuery0`（L716–848）：节点过滤 `loc ? nodeForNodeId(local) : (group.nodeFilter ∧ 非client节点)`（L743–744）；`startRoutine(...)`（L751–757）成功后若非本地启动则 `hnd.waitTopologyFuture(ctx)`（L759–760）——等待 start 时的 exchange 完成，并**预建全部分区的 recovery 对象**（`CacheContinuousQueryHandler.java` L823–832）。内部查询可 `notifyExisting`：启动后直接迭代 offheap 通知存量条目（L775–845）。

### 2.4 事件流：转换、primary/backup 分叉

- 写入点回调 `onEntryUpdated`（L329–357 → L373–445）：由 newVal/oldVal 推导 `CREATED/UPDATED/REMOVED`（L398）；构造内部 `CacheContinuousQueryEntry`（携带 cacheId、partId、**updateCntr**、topVer，L422–432）再包成 `CacheContinuousQueryEvent`（用户拿到的 `CacheEntryEvent` 视图，L441）交给 `CacheContinuousQueryListener.onEntryUpdated(evt, primary, recordIgniteEvt, fut)`（L443）。`CacheContinuousQueryListener` 是 cache 侧监听接口（`CacheContinuousQueryListener.java` L31–134：onEntryUpdated/cleanupOnAck/flushOnExchangeDone/skipUpdateEvent/onPartitionEvicted 等）。
- handler 内实现（`CacheContinuousQueryHandler.java` L452–492）：`asyncCb`（`@IgniteAsyncCallback`）时丢进 `asyncCallbackPool` 按 partition 哈希执行（L471–479）；否则先跑**远端 filter** `filter(evt)`（L481，用户 filter 不通过则 `entry.markFiltered()` L1014–1015）；然后 **`primary || skipPrimaryCheck ? onEntryUpdate(...) : handleBackupEntry(...)`**（L487–491）——这就是"backup 过滤"的全部：非 primary 事件不直接投递，进 backup 缓冲（§2.6）。`skipPrimaryCheck` 仅用于 REPLICATED cache 本地监听（manager L732）。
- primary 侧 `onEntryUpdate`（L1026–1112）：本地监听（`loc=true`）→ `handleEvent`（经 partition recovery 去重补洞，§2.5）→ `notifyLocalListener`（L1120–1134）；远端 master → `prepareEntry`（序列化准备，L808）→ `handleEntry`（进 EventBuffer 攒批）→ `ctx.continuous().addNotification(nodeId, routineId, entryOrList, topic, sync, true)`（L1064）。`sync=true`（JCache `isSynchronous`）时不攒批、同步等 ack（§3.3）。
- **异步回调的分区保序**：`@IgniteAsyncCallback` 监听在 master 端按 `asyncPool.threadId(partition)` 把同分区事件路由到同一线程（`notifyCallback` 分组逻辑，handler L869–910），否则异步会破坏 recovery 输出的分区内顺序；P2P 部署上下文随 entry 的 `deployInfo` 在通知侧恢复（`p2pContext(...)`，L940–951）。

补充两条边缘路径（复刻时容易漏）：

- **过期事件**走 `onEntryExpired`（manager L453–502）：只在 `cctx.isReplicated() || primary` 时通知，构造的 entry `updateCntr = -1`（L492）——**counter 为 -1 的事件在 handler 两侧都直接放行、不进任何缓冲**（接收端 `handleEvent` 的 `e.updateCounter() == -1L` 短路，handler L986–987；发送端 `handleEntry` 同判，L1231–1232）。
- **counter gap 的显式补齐**：REPLICATED cache 或共享 cache group 下，某些更新只生成了 counter 却没有对应事件（被 filter 或本 cache 不关心）。primary 侧用 `skipUpdateEvent` 把"空事件"（`markFiltered` 后仍带 counter，L218–244）灌给监听器；shared group 里其他 cache 的监听器则由 `closeBackupUpdateCountersGaps(cctx, part, topVer, gaps)`（L273–293）按 `[start, end]` 对批量补发 skip 事件，`skipUpdateCounter`（L253–262）在写入路径上收集 gap 并以延迟闭包把补发动作挪出 entry 锁（handler L568–642：loc 时直接通知本地监听器，否则经 `addNotification` 发出）。这条链保证 backup 缓冲的 counter 序列连续，是 §2.5 recovery 判断"连续可放行"的前提。

### 2.5 partition recovery：updateCounter 补洞

**两级缓冲 + 一个重放器**：

- **发送端（affinity 节点）**：`CacheContinuousQueryEventBuffer`（每 partition 一个，`partitionBuffer` L1244–1255）。批次窗口起点由 lambda 决定：`backup > 0 ? locPart.updateCounter() : locPart.reservedCounter()`——`updateCounter()` 是 LWM、`reservedCounter()` 是 HWM（`internal/processors/cache/distributed/dht/topology/GridDhtLocalPartition.java` L906–927 的 javadoc），即 **primary 从 HWM 起只缓存未来事件，backup 从 LWM 起多缓存一段历史**（注释 "Use HWM for primary, LWM for backup"，handler L1252–1253）。窗口 `Batch` 大小 `BUF_SIZE`（默认 1000，`CacheContinuousQueryEventBuffer.java` L52、L69–70），乱序到达先进 `pending`（ConcurrentSkipListMap，L91），被过滤的事件以占位计数保留使 counter 连续（`filtered` 计数，L228–233 一带）。backup 模式下早于窗口的事件进 `backupQ`（L88、L200–205），等 master 的 ack 清理（`cleanupOnAck` L121–131）。
- **接收端（master 本地或有本地监听的节点）**：`CacheContinuousQueryPartitionRecovery`（每 partition 一个，`rcvs` map，handler L191；`getOrCreatePartitionRecovery` L1158–1198）。状态 = `lastFiredEvt` + `curTop` + `pendingEvts: TreeMap<counter, entry>`（`CacheContinuousQueryPartitionRecovery.java` L56–62）。`collectEntries`（L96–266）规则：
  - 首个事件直接 fire 并记住基线（L122–138）；
  - `curTop` 落后且新事件 counter==1 且非 backup → 判定**分区丢失重置**（rebalance 后 primary 换人），把 pending 全部放行（L140–166）；
  - counter ≤ lastFiredEvt 的是重复，丢弃（L172–179）；
  - 从 lastFiredEvt+1 开始**连续段放行**，被 filter 掉的事件用 `filteredCount` 折算保持连续判断（L221–249）；
  - pending 积压超过 `LSNR_MAX_BUF_SIZE`（默认 10_000，handler L106/L125–127）时放行前 90% 防止卡死（L196–219）；`HOLE` 哨兵标记纯占位（L41–47）。
- **起始无缺口的种子**：routine start 时每个远端节点上报本地分区 counters（V2：`processStartRequestV2` 里 `cache.context().topology().localUpdateCounters(false)` 并随 `ContinuousRoutineStartResultMessage` 回传，`GridContinuousProcessor.java` L1618–1642；V1 随 discovery ack 回传，L1460–1468、L1362–1366）→ master 的 `StartFuture.onAllRemoteRegistered` 把 `cntrsPerNode` 注入 handler（L2638–2659，`updateCounters` 落到 `initUpdCntrsPerNode`，handler L364–371）→ `getOrCreatePartitionRecovery` 按**当前 affinity owner** 选取该分区的 counter 作为 `lastFiredEvt` 初值（L1170–1191）。这保证"start 完成瞬间之前已发生的更新不会既丢失又重放"。
- **与 exchange/rebalance 的联动点**：每次 partition exchange 完成（server 节点且发生拓扑变化）时，`GridDhtPartitionsExchangeFuture` 对每个 affinity cache 调 `cacheCtx.continuousQueries().flushOnExchangeDone(res)`（`internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsExchangeFuture.java` L2400–2407）→ manager L684–687 → handler `flushOnExchangeDone`（L520–551）：把 backup 缓冲中攒的事件全部 `markBackup` 后经 `ctx.continuous().addBackupNotification(...)` 补发（L544）——这覆盖"节点断连期间 backup 顶上"的缺口场景。client 断连时 recovery 缓存拓扑失效，`onClientDisconnected` 清 `curTop`（handler L1144–1150 → recovery `resetTopologyCache` L84–86）。
- **ack 回路**：master 收到事件后由 `CacheContinuousQueryAcknowledgeBuffer` 聚合（`CacheContinuousQueryAcknowledgeBuffer.java` L34–120，累计 per-partition 最大 counter，到 `BACKUP_ACK_THRESHOLD`（默认 100，handler L103/L120–122）或 5 秒超时（manager `BACKUP_ACK_FREQ=5000` L112 + `BackupCleaner` 定时任务 L184、L1353–1373）发 `CacheContinuousQueryBatchAck`；affinity 节点在 `onKernalStart0` 注册该消息的处理器并 `cleanupOnAck`（manager L171–182 → EventBuffer L121–131 清 backupQ）。
- **autoUnsubscribe 语义**：CQ 默认 `true`（`cache/query/AbstractContinuousQuery.java` L47）。master 节点离开/失败时，`GridContinuousProcessor` 的 `DiscoveryListener` 对 `autoUnsubscribe=true` 的 routine 全集群 `unregisterRemote`（L2015–2025）——routine 随 master 消亡；`=false` 时远端把 routine 记进本地 `locInfos` 保留（L1472–1475）。

**三场景对照**（recovery 相关机制各管一段，复刻时可作测试矩阵）：

| 场景 | 缺口来源 | 补齐机制 | 关键位置 |
|---|---|---|---|
| routine 启动瞬间 | 历史更新已发生、未来更新将来 | start 时各节点上报 per-partition counters → recovery 以 owner 的 counter 为 `lastFiredEvt` 种子 | processor L1618–1642、L2638–2659；handler L1158–1198 |
| master 断连/备份期间 | 事件滞留在 backup 节点缓冲 | ack 未到的条目留在 `backupQ`/batch；exchange 完成时 `flushOnExchangeDone` 把 backup 缓冲整体补发（`markBackup`） | handler L520–551、EventBuffer L137–162；exchange 触发 `GridDhtPartitionsExchangeFuture.java` L2400–2407 |
| 分区丢失（primary 换人） | 旧 primary 未同步的 counter 空洞 | 收到新 topology 下 counter==1 的事件即认定重置：pending 全放行、基线重立 | recovery L140–166 |

---

## 3. GridContinuousProcessor：通用 routine 基础设施

**消息清单**（复刻协议面）：IO 消息只有三种——`GridContinuousMessage`（type ∈ `GridContinuousMessageType`：`MSG_EVT_NOTIFICATION`、`MSG_EVT_ACK`，枚举见 `processors/continuous/GridContinuousMessageType.java` L25–30）、`ContinuousRoutineStartResultMessage`（V2 启动结果，单播 TOPIC_CONTINUOUS）、`CacheContinuousQueryBatchAck`（CQ backup ack，走 cache IO topic）。Discovery 自定义事件五种：`StartRoutineDiscoveryMessage`(V1)/`StartRoutineDiscoveryMessageV2`、`StartRoutineAckDiscoveryMessage`(V1)、`StopRoutineDiscoveryMessage`、`StopRoutineAckDiscoveryMessage`(V1)。

类：`internal/processors/continuous/GridContinuousProcessor.java`（2819 行）。三个 handler 都经它部署。

### 3.1 状态与协议版本

- 核心表：`locInfos`（本机发起的 routine，L127）、`rmtInfos`（远端发起、本机持有 handler 的 routine，L133）、`clientInfos`（client 发起的，L130）、`startFuts`/`stopFuts`（L136/L139）、`bufCheckThreads`（per-routine 定时 flush 线程，L142）、`syncMsgFuts`（同步通知 ack，L145）、`routinesInfo`（L172）。
- **discoProtoVer**：`ctx.discovery().mutableCustomMessages() ? 1 : 2`（L191）。V1 = 老 discovery 自定义消息协议（`StartRoutineDiscoveryMessage` 携带**未序列化**的 handler 对象，`StartRequestData`，L983–1007）；V2 = 2.9+ 默认（`StartRoutineDiscoveryMessageV2` 携带 `handlerBytes`/`nodeFilterBytes` 纯字节数组，L1008–1029）。两者并存是版本兼容，行为等价。

### 3.2 startRoutine 生命周期

1. `startRoutine(hnd, locOnly, bufSize, interval, autoUnsubscribe, prjPred)`（L856–940）：生成 `UUID routineId`（L867）；P2P 开启时先 `hnd.p2pMarshal`（L869–873）；写入 `locInfos`（L876–877）；`locOnly` 直接本地 `registerHandler` 返回（L879–890）。
2. `registerMessageListener(hnd)`：若 handler 有 `orderedTopic()`，在 IO 层为该 topic 注册通知监听（L1035–1059）——**CQ 的通知走专用有序 topic**，events/messaging 的 handler `orderedTopic()` 返回 null（`GridEventConsumeHandler`/`GridMessageListenHandler` 中均为 null），走公共 `TOPIC_CONTINUOUS`。
3. 本机在 prj 内或 `hnd.isQuery()` → 本地也注册 handler（L907–915）；`createStartMessage` 后 `ctx.discovery().sendCustomEvent(msg)` **广播 discovery 自定义事件**（L917–919）。
4. V2 远端处理 `processStartRequestV2`（L1513–1645）：先 `routinesInfo.addRoutineInfo`（L1526，供 join 的新节点重放），再丢进 system pool（discovery 线程不能做 marshal/IO，L1531）：unmarshal nodeFilter（P2P 时用对端部署，L1550–1590）→ 命中本机则 unmarshal handler 并 `registerHandler`（L1597–1616）→ 查询类 handler 附带 `localUpdateCounters`（L1618–1630）→ `ContinuousRoutineStartResultMessage` 回 master（L1642、L1653–1684）。master 的 `StartFuture` 收齐所有结果后完成、注入 counters（§2.5），有错误则自动 `stopRoutine`（L2663–2669）。
5. **handler 延迟注册**：cache 尚未启动时 `register` 返回 `DELAYED`，`RemoteRoutineInfo.markDelayedRegister`（L1831–1837），`onCacheStart` 时补注册（L727–740）。

### 3.2.5 StartFuture 的完成条件与 V1/V2 对比

- `StartFuture`（L2542 起）有**两个完成条件**：本地 handler 注册完成（`onLocalRegistered`，L2701 起）+ 全部远端结果收齐（V2 由 `RoutineRegisterResults` 收集器驱动，`onResultsCollected` L2622–L2623 / `onResult` L2687–2689 / `onNodeFail` L2694–2696；V1 由 `StartRoutineAckDiscoveryMessage` 驱动，L1357–1368）。master 收齐后注入 counters 并完成 future（L2638–2659）；任一远端报错则整条 routine 自动 `stopRoutine`（L2663–2669）。
- `LocalRoutineInfo`/`RemoteRoutineInfo` 是 routine 的两侧视图：`LocalRoutineInfo`（L2076 起）持源节点 ID、prjPred、handler、bufSize、interval、autoUnsubscribe（Serializable，可随 discovery data 传播）；`RemoteRoutineInfo`（L2170–2386）额外持有批量状态 `batch`、`lastSndTime`、`delayedRegister` 标志（L2195–2196）。`ContinuousRoutineInfo`（V2）则是纯字节形态（handlerBytes、nodeFilterBytes、bufSize、interval、autoUnsubscribe），构造见 L1518–1524 与 L824–839。
- V1/V2 差异一句话：**V1 在 discovery 消息里携带活的 handler 对象引用（依赖各节点类路径一致），V2 全部 marshal 成字节数组、反序列化挪出 discovery 线程**（L1531 的 system pool 提交，注释明言 "Should not use marshaller and send messages from discovery thread"）。
- 另有 `registerStaticRoutine`（L771–811）：不走 startRoutine 的"内部静态 routine"（如 client 节点的内部 CQ），直接登记 `locInfos` 并把 `ContinuousRoutineInfo` 塞进 `routinesInfo` 以便 join 时随 discovery data 传播，P2P 不适用。

### 3.3 stop 与节点 join/left

- `stopRoutine`（L1065–1127）：本地 `locInfos.remove` + `unregisterHandler`，广播 `StopRoutineDiscoveryMessage`（L1112）；各节点 `processStopRequest` → `unregisterRemote`（L1340–1351）；master 的 stop future 由 `StopRoutineAckDiscoveryMessage` 完成（V1，L1329–1334；V2 直接由 discovery 顺序保证）。`unregisterHandler`（L1850–1863）调 `hnd.unregister` 并停掉 buffer-checker 线程。
- **节点 join**：processor 实现 discovery 数据交换（`discoveryDataType() = CONTINUOUS_PROC`，L405–407）。joining node 通过 `ContinuousRoutinesJoiningNodeDiscoveryData` 带上自己的 routine（L514–538），老节点通过 `ContinuousRoutinesCommonDiscoveryData` 提供全量 routine（L541–565）→ `registerHandlerOnJoin`（L669–719）：nodeFilter 不匹配跳过（L676），P2P 部署推迟到 `localJoinFuture`（L685–701）。**新节点因此自动获得既有 routine 的 handler**——这就是 remoteListen/CQ "全组生效"的机制。
- **节点 left/failed**：`DiscoveryListener`（高优先级 order=1，L1997–2047）：`routinesInfo.onNodeFail`（L2006）；master 是离开节点的 routine：`autoUnsubscribe` → `unregisterRemote`，否则仅 `hnd.flushOnNodeLeft()`（L2015–2025）；挂起的同步通知 future 以拓扑异常完成（L2027–2040）。client 断连 `onDisconnected`（L1264–1295）：撤销远端 routine、本地 handler `onClientDisconnected`、保留 `locInfos` 以便重连后重启。

### 3.4 消息协议：批量、ack、有序/无序

- **通知消息**：`GridContinuousMessage(MSG_EVT_NOTIFICATION, routineId, futId, data, msg)`（L1308–1324）。发送端先攒批：`RemoteRoutineInfo.add(obj)` 攒到 `bufSize-1` 触发整批发送（L2313–2347，批容器由 `hnd.createBatch()` 提供——CQ 是 `GridContinuousQueryBatch`，普通是 `GridContinuousBatchAdapter` 的 `FastSizeDeque`，`GridContinuousQueryBatch.java` L28–56）；`interval > 0` 时由 per-routine 的 `continuous-buffer-checker` 线程按周期 flush（L1772–1829 + `checkInterval` L2354–2380）。`bufSize`/`interval` 即 API 的 pageSize/timeInterval（默认 1 / 0，`AbstractContinuousQuery.java` L38/L41——默认**不攒批**立即发）。
- **同步通知**：`sync=true`（JCache synchronous 监听 / CQ internal sync）绕过攒批，用 `SyncMessageAckFuture` 阻塞等待对端 `MSG_EVT_ACK`（L1192–1245；ack 处理 `processMessageAck` L1689–1696，接收方回 ack L1715–1726）。
- **有序 vs 无序**：`hnd.orderedTopic() != null` 时用 `ctx.io().sendOrderedMessage(...)`（L1961–1969）——**per-topic（即 per-routine）保序**，CQ handler 的 `orderedTopic()` 返回自己的 topic（`CacheContinuousQueryHandler.java` L1399–1401），故同一 CQ 的事件通知有序；events/messaging 走 `TOPIC_CONTINUOUS` 普通消息无全局序。投递失败按 `retryCnt/retryDelay` 重试，节点已死则抛 `ClusterTopologyCheckedException`（L1954–1990）。
- **master 端分发**：`TOPIC_CONTINUOUS` 监听（L276–310）→ `processNotification` → `routine.hnd.notifyCallback(nodeId, routineId, data, ctx)`（L1702–1713）。CQ 的 `notifyCallback` 再按 partition 哈希到 `asyncCallbackPool` 保持分区内有序（handler L857–917），最终经 partition recovery（§2.5）调用用户 localListener。
- `onBatchAcknowledged` 回调在消息确认送达后触发（L1250–1257），CQ 用它驱动 backup ack（handler L1331–1334）。
- 三种投递模式小结：

| 模式 | 触发条件 | 攒批 | 通道 | 等待 ack |
|---|---|---|---|---|
| 普通（CQ/事件默认） | `sync=false` | bufSize/interval（默认 1/0 即不攒） | CQ 走专属有序 topic；events 走 TOPIC_CONTINUOUS | 否（ack 回调异步驱动 backup 清理） |
| 同步（JCache `isSynchronous`） | `sync=true` | 不攒，单条发 | 专属 topic | **阻塞等待** `MSG_EVT_ACK`（L1192–1245） |
| backup 补发 | exchange/node left | `addBackupNotification` 整段发 | 专属 topic | 否 |

---

## 4. IgniteMessaging：topic 消息

类：`internal/IgniteMessagingImpl.java` + `internal/GridMessageListenHandler.java` + `GridIoManager` 的用户消息部分。

### 4.1 发送与本地订阅：直接走 IO 层

- `send(topic, msg)` → `send0` → `ctx.io().sendUserMessage(nodes, msg, topic, false, 0, async)`（L92–111）；`sendOrdered(topic, msg, timeout)` 同路但 `ordered=true`（L151–173）。**发送路径完全不经过 GridContinuousProcessor**。
- `GridIoManager.sendUserMessage`（`internal/managers/communication/GridIoManager.java` L2336–2424）：跨节点时 marshal 消息与 topic（L2348–2353）、P2P 部署消息类（L2359–2371）、包成 `GridIoUserMessage`（L2373–2382）发到 `TOPIC_COMM_USER`（ordered 用 `sendOrderedMessageToGridTopic`，L2384–2385；本地监听最后调用，L2410–2421）。
- `localListen(topic, p)` → `ctx.io().addUserMessageListener(topic, p)`（`IgniteMessagingImpl.java` L176–187）→ 包装成 `GridUserMessageListener` 挂到 `TOPIC_COMM_USER`（`GridIoManager.java` L2435–2454）。消息到达：topic 匹配（L3622）→ 按部署类加载器 unmarshal + 资源注入（L3585–3641）→ `predLsnr.apply(nodeId, msgBody)`，**返回 false 即注销该监听**（L3643–3646，apply 包在 `withContext(initNodeId)` 的安全上下文里）。
- **有序消息**：`sendOrdered` 走 IO 层 per-topic 有序队列，超时后丢弃乱序滞留消息（接口 javadoc：`IgniteMessaging.java` L63–68）；这不是 routine 的 ordered topic，而是 `GridIoManager` 的 ordered 投递语义。

### 4.2 remoteListen：借 routine 做部署

- `IgniteMessagingImpl.remoteListen(topic, p)`（L204–225）：`new GridMessageListenHandler(topic, p)` 后 `ctx.continuous().startRoutine(hnd, false, 1, 0, false, prj.predicate())`——**bufSize=1、interval=0、autoUnsubscribe=false**（与 events 的默认 true 相反，消息订阅不随 master 消亡）。
- `GridMessageListenHandler.register`（`GridMessageListenHandler.java` L131–138）：在远端节点 `ctx.io().addUserMessageListener(topic, pred, nodeId)`——把**用户谓词本体**部署到远端并在远端本地执行（`GridUserMessageListener` 的第三参 initNodeId 用于安全上下文切换，`GridIoManager.java` L3643）；其 `notifyCallback` 是 `assert false`（L146–148）——**continuous 的通知通道对 messaging 不使用**，routine 只承担部署/生命周期/新节点重部署。`orderedTopic()` 返回 null（L225–227）。P2P：`p2pMarshal` 部署谓词类（L151–173）、`p2pUnmarshal` 在远端用对端部署还原（L176–207）。

### 4.3 三种 remoteListen 的 routine 参数对比

| 维度 | events remoteListen | CQ（ContinuousQuery/JCache） | messaging remoteListen |
|---|---|---|---|
| handler | `GridEventConsumeHandler`（isEvents=true） | `CacheContinuousQueryHandler[V2/V3]`（isQuery=true） | `GridMessageListenHandler`（isMessaging=true） |
| 默认 bufSize/interval | 1 / 0（`IgniteEventsImpl.java` L115） | `DFLT_PAGE_SIZE=1` / `DFLT_TIME_INTERVAL=0`（`AbstractContinuousQuery.java` L38、L41） | 1 / 0（`IgniteMessagingImpl.java` L212–217） |
| 默认 autoUnsubscribe | **true**（L115） | **true**（L47） | **false**（L217） |
| orderedTopic | null（公共 TOPIC_CONTINUOUS） | 专属 topic（`TOPIC_CACHE.topic("CONTINUOUS_QUERY_<cache>", ...)`） | null |
| 通知通道是否使用 | 使用（事件回流 master） | 使用（事件回流 + ack） | **不使用**（谓词远端本地执行，`notifyCallback` assert false） |
| start 时上报 updateCounters | 否 | 是（`isQuery()` 分支） | 否 |

这行对比是切课的直接依据：E1 的 routine 地基对三者是同一套代码，差异全部压在 handler 内部。

---

## 5. 对复刻课的切课建议

按"一课一个概念簇、单课 2–4 小时、宁多切课不一课双簇"的纪律，结合上述源码实际耦合（`GridContinuousProcessor` 是三者共用地基；CQ 的 recovery 又依赖 exchange/counter 语义）：

| # | 课名（概念簇） | 核心内容（tracer） | 关键文件（相对 `internal/`） | 依赖 | 规模估计 |
|---|---|---|---|---|---|
| E1 | **routine 基础设施**：GridContinuousProcessor + GridContinuousHandler | startRoutine → `StartRoutineDiscoveryMessageV2` discovery 广播 → 远端 registerHandler → `ContinuousRoutineStartResultMessage` 回 ack → StartFuture 完成；stop/节点 join（discovery data 重放）/left（autoUnsubscribe）；`GridContinuousMessage` 通知 + bufSize/interval 批量 + `MSG_EVT_ACK`；orderedTopic 保序。tracer：用一个空壳自定义 handler 走完 routine 全生命周期，观察 join 新节点自动获得 handler | `processors/continuous/`（全包 20 个类） | 无（仅依赖已完成的 discovery custom event 与 IO 层） | ~2.5–3.5h |
| E2 | **cache 事件桥**：CacheContinuousQueryManager + Handler + EventBuffer | 写入点 `onEntryUpdated` 双 fire（events + CQ）；JCache listener 配置桥（`JCacheQuery`/types 位标志）；ContinuousQuery 的 localListener/remoteFilter(Factory) 分离与 initialQuery 时机；primary/backup 分叉（`handleBackupEntry`）、EventBuffer 的 HWM/LWM 窗口与 backupQ、ack 回路（`CacheContinuousQueryBatchAck` + `BACKUP_ACK_THRESHOLD`/5s 超时）；counter gap 补齐（`skipUpdateEvent`/`closeBackupUpdateCountersGaps`）。tracer：put 一次 → 断开 backup → 观察 ack 清理 | `cache/query/continuous/` 除 Recovery 外 | E1 | ~3–4h |
| E3 | **投递保证**：updateCounter 与 partition recovery | `CacheContinuousQueryPartitionRecovery.collectEntries` 全规则（连续放行/去重/分区丢失 flush/溢出 90%）；start 时 counters 采集回注（`StartFuture.onAllRemoteRegistered` → `initUpdCntrsPerNode` → recovery 种子）；exchange 联动（`GridDhtPartitionsExchangeFuture` → `flushOnExchangeDone` 补发）；`waitTopologyFuture`。tracer：kill primary 期间持续写入 → 验证既不丢也不重 | `CacheContinuousQueryPartitionRecovery.java` + handler 相关段 | E2 + exchange/rebalance 弧（research 10） | ~2.5–3h |
| E4 | **事件与消息消费者**：GridEventStorageManager + IgniteMessaging | EventStorageSpi 两实现（Noop 默认/Memory FIFO+expireCount/expireAge）；本地监听表与 high-priority；includeEventTypes 的 recordable/userRecordable 双数组；`EVT_CACHE_OBJECT_PUT` fire 链（innerUpdate → GridCacheEventManager → record0）；`remoteQuery` 的 `GridEventStorageMessage`/`TOPIC_EVENT` 请求-响应；`GridEventConsumeHandler`（events remoteListen = routine）；IgniteMessaging send/localListen（`TOPIC_COMM_USER`）与 remoteListen（`GridMessageListenHandler` 借 routine 纯部署）。tracer：本地事件全链 + 一次 remoteListen 事件回流 | `managers/eventstorage/`、`spi/eventstorage/**`、`IgniteEventsImpl`、`IgniteMessagingImpl`、`GridEventConsumeHandler`、`GridMessageListenHandler` | E1；`EVT_CACHE_*` fire 链需 data-plane 弧 | ~3–4h |

依赖图：`E1 → E2 → E3`（线性），`E1 → E4`（E4 与 E2/E3 并行可换序；E4 的 fire 链例证引用 put 路径，建议排在 data-plane 课之后，全弧 4 课）。

**弧级验收 tracer（端到端，复刻完四课后应全绿）**：

1. 两个节点、`includeEventTypes(EVT_CACHE_OBJECT_PUT)` + MemoryEventStorageSpi：put 一次，两节点 `localQuery` 各见一条（primary 发、各自记录）。
2. ContinuousQuery（默认参数）：client 起 routine、server 持续写入，master 的 localListener 收到的 counter 序列 per-partition 严格连续（含被 remoteFilter 过滤的更新）。
3. kill 持有 primary 的节点：备份晋升后 CQ 事件流**既不丢也不重**（backup 缓冲 + `flushOnExchangeDone` + recovery 种子三件套生效）。
4. `stopRemoteListen` 后再写入：无事件回流，且远端 handler 注销（`unregisterListener` 被调）。
5. messaging：`remoteListen` 谓词部署到远端（远端可见），master 停机后谓词仍在远端存活（autoUnsubscribe=false）；`localListen` 谓词返回 false 自动注销。

**可简化项判断**（默认全做，仅列真正可争议项）：
1. **discoProtoVer==1 协议族**（`StartRoutineDiscoveryMessage`/`StartRoutineAckDiscoveryMessage`/`DiscoveryData` V1 路径）：仅在与老版本（<2.9）混部时激活（`mutableCustomMessages()`）。若复刻集群声明最低版本即"2.18 语义"、不做滚动升级兼容，V2-only 是可辩护的裁剪；但会偏离 2.18 二进制行为（ADR 0005 下默认全做，V1 约 +150 行）。
2. **`IgniteEvents.remoteQuery` 的事件查询通道**（`GridEventStorageMessage` + P2P filter 部署 + 临时 response topic）：功能冷僻、与 routine 主线正交，可考虑降级为 E4 的选做 tracer；其余（remoteListen 走 routine）必须做。
3. **`ContinuousQueryWithTransformer`（HandlerV3）与 `notifyExisting` 存量通知**：V3 只是 V2 加 transformer 工厂（manager L529–543），实现代价小，默认做；`notifyExisting` 仅内部查询使用，若内部 CQ 消费者（如 near cache 一致性）已在别的弧实现，此处可只留接口。
4. 其余（MemoryEventStorageSpi、ack 缓冲、BackupCleaner、delayed register、client 断连重连路径）无争议，全做。

---

## 引用文件清单（相对 `vendors/ignite/`，均已实际打开验证）

1. `modules/core/src/main/java/org/apache/ignite/internal/managers/eventstorage/GridEventStorageManager.java`
2. `modules/core/src/main/java/org/apache/ignite/spi/eventstorage/NoopEventStorageSpi.java`
3. `modules/core/src/main/java/org/apache/ignite/spi/eventstorage/memory/MemoryEventStorageSpi.java`
4. `modules/core/src/main/java/org/apache/ignite/internal/IgniteEventsImpl.java`
5. `modules/core/src/main/java/org/apache/ignite/internal/IgnitionEx.java`
6. `modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java`
7. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheEventManager.java`
8. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheMapEntry.java`
9. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryManager.java`
10. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryListener.java`
11. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryHandler.java`
12. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryPartitionRecovery.java`
13. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryEventBuffer.java`
14. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/query/continuous/CacheContinuousQueryAcknowledgeBuffer.java`
15. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousProcessor.java`
16. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousHandler.java`
17. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousMessageType.java`
18. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousQueryBatch.java`
19. `modules/core/src/main/java/org/apache/ignite/internal/processors/continuous/GridContinuousBatchAdapter.java`
20. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsExchangeFuture.java`
21. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridDhtLocalPartition.java`
22. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java`
23. `modules/core/src/main/java/org/apache/ignite/cache/query/ContinuousQuery.java`
24. `modules/core/src/main/java/org/apache/ignite/cache/query/AbstractContinuousQuery.java`
25. `modules/core/src/main/java/org/apache/ignite/internal/GridEventConsumeHandler.java`
26. `modules/core/src/main/java/org/apache/ignite/internal/GridMessageListenHandler.java`
27. `modules/core/src/main/java/org/apache/ignite/internal/IgniteMessagingImpl.java`
28. `modules/core/src/main/java/org/apache/ignite/IgniteMessaging.java`
29. `modules/core/src/main/java/org/apache/ignite/internal/managers/communication/GridIoManager.java`

（§4.3 表中 `IgniteMessagingImpl.java` L217 指 `startRoutine(..., false, ...)` 的 autoUnsubscribe 实参，见 L210–217。）
