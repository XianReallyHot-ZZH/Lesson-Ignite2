# 11 · near cache 子系统：结构、读路径、失效与生命周期

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文回答 near cache 子系统"内部怎么实现"：near 侧的类分工与条目形态、get 的 near-first 读路径、primary 更新时 near 条目的更新/失效全路径（ATOMIC + TRANSACTIONAL）、near 淘汰与 clear、client 节点的 near 形态。数据面主路径（put、atomic future、affinity 定位）已在 03 号报告验收，本文直接引用不重查；client 不 rebalance 的结论衔接 10 号报告 §4.3。正文短路径（如 `near/...`、`.../dht/atomic/...`）是 `org/apache/ignite/internal/processors/cache/` 下的缩写，仅为排版省行；**文末"引用文件清单"给出全部完整可定位路径**（均已逐一打开验证），行号以当前 submodule 内容为准。

---

## 0. 全景：near cache 在 2.18 里是什么

- near cache 是**挂在同一 cache 名下的第二套本地条目表**：javadoc 自述 "smaller local cache that stores most recently or most frequently accessed data"（`near/GridNearCacheAdapter.java:67`）。它不是独立的 cache 类型，而是 `GridCacheProcessor` 建缓存时按 `nearConfiguration != null` 在本地加装的**前端**（`cache/GridCacheProcessor.java:1273`、`cache/GridCacheUtils.java:444-457`）。
- 装上 near 后，用户 proxy 持有的 delegate 变成 near 前端；**写操作全部一行转发给 dht 后端**（`near/GridNearAtomicCache.java:454` 的 `put → dht.put`，同文件 L449-476 一整组），**读操作先查 near 再下沉**（§2）。所以 near cache 纯粹是"读加速 + 条目级失效协议"，不参与持久化、查询、WAL（§1.2）。
- 失效协议的**登记点在 primary 的 dht 条目上**：每个 `GridDhtCacheEntry` 持一个 `ReaderId[]` readers 数组（`.../dht/GridDhtCacheEntry.java:443-565`），primary 每次更新把新值**捎带**给 readers——ATOMIC 捎在 `GridDhtAtomicAbstractUpdateRequest`/`GridNearAtomicUpdateResponse` 里，TRANSACTIONAL 捎在 `GridDhtTxPrepareRequest`/`GridDhtTxFinishRequest` 里（§3）。

```
用户 proxy ── delegate ──▶ GridNearAtomicCache / GridNearTransactionalCache   (near 前端)
                               │ map = GridCacheLocalConcurrentMap<GridNearCacheEntry>
                               │ .dht() ▼ 互持 ▲ .near()
                             GridDhtAtomicCache / GridDhtCache               (dht 后端)
                               │ map = GridCacheConcurrentMapImpl<GridDhtCacheEntry>（affinity 节点）
                               │            或 GridNoStorageCacheMap（client）
                               └ readers: ReaderId[] —— near 更新的分发名单
```

---

## 1. near 结构

### 1.1 三个实现类的分工

| 类 | 角色 | 关键引用 |
|---|---|---|
| `GridNearCacheAdapter`（抽象基类） | near 通用逻辑：条目工厂、near 容器、`loadAsync` 读入口、evict/clear 双侧联动 | `near/GridNearCacheAdapter.java:69` |
| `GridNearAtomicCache` | ATOMIC 前端：写转发 dht；atomic 响应的 near 回写；delete 历史环形队列 | `near/GridNearAtomicCache.java:77` |
| `GridNearTransactionalCache` | TRANSACTIONAL 前端：写走微事务（不覆写 `put0`，见 03 号 §5）；读在显式事务内并入 tx；显式锁 `GridNearLockFuture` | `near/GridNearTransactionalCache.java:59` |

- 二者都通过 `dht()` 抽象方法（`GridNearCacheAdapter.java:118`）持有 dht 后端；`GridNearAtomicCache` 存 `GridDhtAtomicCache` 字段并暴露 `dht(dhtCache)` 装配方法（L82/L122-129），`GridNearTransactionalCache` 存 `GridDhtCache`（L64/L102-109）。
- `isNear()` 恒真（L129-131）；`preloader()` 直接委托 dht（L134-136）——**near 不做 rebalance**（衔接 10 号 §4.3）。
- 消息 handler 侧：near 前端只注册 `GridNearGetResponse`（ATOMIC L112-117；TX 同款 + `GridNearLockResponse`，`near/GridNearTransactionalCache.java:86-97`）；**`GridNearGetRequest`/`GridNearSingleGetRequest` 的处理方在 dht 侧**（`.../dht/GridDhtTransactionalCacheAdapter.java:130-134`；ATOMIC 在 `GridDhtAtomicCache.onKernalStart`，`.../dht/atomic/GridDhtAtomicCache.java:249-259`），因为响应必须由 primary/backup 产生。

### 1.2 `GridNearCacheEntry` 与 dht 条目的差异

`GridNearCacheEntry extends GridDistributedCacheEntry`（`near/GridNearCacheEntry.java:51`），与 `GridDhtCacheEntry` 平级（都继承 `GridDistributedCacheEntry`，`.../dht/GridDhtCacheEntry.java:60`），但近侧条目"更薄、更多账本"：

1. **不落盘**：`storeValue/removeValue`（L459-468）、`logUpdate/logTxUpdate`（L471-485）全部 no-op——源码注释 "queries are disabled for near cache"；`unswap` 恒 null（L488-490）。near 条目只有 on-heap 值，没有 off-heap/WAL/索引。
2. **额外两个字段**：`volatile AffinityTopologyVersion topVer`（值初始化自 primary 时的拓扑版本，L56）与 `GridCacheVersion dhtVer`（"DHT version which caused the last update"，L59）——失效与一致性判断都靠它们（§3.4）。构造时缓存 `part`（L71-78），内存开销 `NEAR_SIZE_OVERHEAD = 36+16`（L53/L81-83）。
3. **拓扑有效性**：`valid(topVer)`（L96-131）在 primary 换人（`primaryChanged` L108）或**本节点变成 backup**（`backupByPartition` L115）时把自己判无效并复位 `topVer=NONE`——"reader 变 owner 就不该再有 near 条目"在条目层兜底。
4. **near 专属更新方法**：`loadedValue`（远端 get 回填，L382-457）、`resetFromPrimary`（锁/prepare 响应刷新，L200-231）、`updateOrEvict`（tx 提交回写或淘汰，L243-278）、`initializeFromDht`（从本地 dht 条目初始化，L137-187）、`versionedValue`（给锁协议返回 `(dhtVer, val)`，L301-319）。
5. **eviction 预约**：`evictReservations` 计数（L65/L724-759）——get 在途时 `reserveEviction()` 阻止淘汰管理器删条目，防止"回填时条目已没了"（§4.1）。

### 1.3 near 侧并发容器

`GridNearCacheAdapter.start()`（L91-98）建的是 **`GridCacheLocalConcurrentMap`**：单个 `CacheMapHolder` 包一个 `ConcurrentHashMap`（`cache/GridCacheLocalConcurrentMap.java:30-46`），初始容量来自 `NearCacheConfiguration.nearStartSize`（默认 `DFLT_NEAR_START_SIZE = 1500000/4`，`configuration/NearCacheConfiguration.java:49`、`configuration/CacheConfiguration.java:140`）。对比 dht 侧的 `GridCacheConcurrentMapImpl`（按 cacheId 分 holder，03 号 §2.5 已验收）：**near 表没有分区结构**，因为它不挂 `GridDhtLocalPartition`、不参与分区所有权。`entryEx(key, topVer)` 取/建条目后会调 `entry.initializeFromDht(topVer)`（L140-156）：若本地 dht 也有该 key（本节点是 owner），直接把 dht 的值/版本抄进 near 条目（L137-187），免一次网络读。

### 1.4 创建路径与 near↔dht 互持

- 判定：`boolean nearEnabled = GridCacheUtils.isNearEnabled(cfg)`（`GridCacheProcessor.java:1273`，实现就是 `getNearConfiguration() != null`，`GridCacheUtils.java:455-457`）。
- 选前端（L1335-1377）：`PARTITIONED/REPLICATED + nearEnabled` 时，TX → `GridNearTransactionalCache`（L1341），ATOMIC → `GridNearAtomicCache`（L1346）。
- 建 dht 后端（L1397-1492）：为 dht 单独 new 一个 `GridCacheContext`（复用 near 侧的 version/io/deployment/query 等 manager，注释 L1398-1409），dht 侧淘汰管理器按 `onheapCacheEnabled` 选 `GridCacheEvictionManager`/`CacheOffheapEvictionManager`（L1411）；然后**双向互持**：TX `dhtCache.near(near); near.dht(dhtCache);`（L1460-1462），ATOMIC 同款（L1477-1479）。`GridCacheContext.near()/dht()` 是互访入口（`cache/GridCacheContext.java:746/767`）。
- near 前端的淘汰管理器则无条件用 `GridCacheEvictionManager`（near 条目永远 on-heap，L1277-1279 `(nearEnabled || onheapCacheEnabled)` 分支），策略取 `nearEvictionPolicyFactory`（§4.1）。

### 1.5 client 形态与动态 near

- **client（非 affinity 节点）也能持有这套结构**：dht 后端用 `GridNoStorageCacheMap`——`getEntry` 恒 null、`putEntryIfObsoleteOrAbsent` 造一个不落 map 的 `GridDhtCacheEntry`（`cache/GridNoStorageCacheMap.java:30-47`），所以 client 的"dht 缓存"没有存储，只有消息 handler 与拓扑视图；near 表才是 client 的本地数据（L1456-1458/L1473-1475 的三元选择）。
- **near 与否是节点本地决策**：动态建缓存时 `checkForAffinityNode` 只在非 affinity 节点把请求里的 `reqNearCfg` 写进本地 ccfg（L2118-2129：`ccfg.setNearConfiguration(reqNearCfg)`，调用点 L1988）。server 上通常 `nearConfiguration=null`（colocated 形态），client 想要 near 就带 `reqNearCfg`——这就是"同一个 cache 在不同节点有不同形态"的机制根源。
- 公共 API 只有 `Ignite.createNearCache / getOrCreateNearCache`（`org/apache/ignite/Ignite.java:381/394`，实现 `internal/IgniteKernal.java:2305-2333/2336-2369`：本地 `dynamicStartCache(null, cacheName, nearCfg, ...)`）。**2.18 没有 `withNearCache()` 这个方法**（全 core 模块 grep 无命中）——它是社区讨论里的 3.0 方向；复刻课不要照任务书想当然实现它。

---

## 2. near 读路径

### 2.1 入口分派

`cache.get(key)` → `GridCacheAdapter#get`（L1340）→ `getAsync`（L1761-1795）→ 抽象 `getAllAsync`（L1809-1819）。near 前端的实现：

- **ATOMIC**（`GridNearAtomicCache.java:415-446`）：无条件 `loadAsync(...)`。
- **TRANSACTIONAL**（`GridNearTransactionalCache.java:112-166`）：线程上有显式事务（`tx != null && !tx.implicit() && !skipTx`）则 `tx.getAllAsync(...)`（L137-153，读进 tx、near 条目被 enlist 为 `IgniteTxEntry`）；否则同样 `loadAsync`。tx 中的 read-through 再走 `txLoadAsync`（L179-208），仍创建 `GridNearGetFuture`，只是带上 tx 与其 topology version。

二者殊途同归到 `GridNearCacheAdapter#loadAsync`（L215-252）：new **`GridNearGetFuture`** 并 `fut.init(null)`。

### 2.2 near 命中（含 ttl/expiry）

`GridNearGetFuture.map(key, ...)`（`near/GridNearGetFuture.java:315-458`）逐 key 执行：

1. **near 优先**：`allowLocRead = !forcePrimary || 本地是 primary`（L335），`GridNearCacheEntry entry = near.peekEx(key)`（L338）。
2. 命中读：`entry.innerGet(..., /*metrics*/true, ..., expiryPlc, ...)`（L366-375；要版本则 `innerGetVersioned` L349-363）。过期/TTL 由 `innerGet` 统一处理（访问过期走 expiryPlc.forAccess，见 `cache/GridCacheMapEntry.java` 内部实现）；innerGet 自身按值是否为 null 计 hit/miss（`GridCacheMapEntry.java:702-708`）。
3. 命中即 `addResult(key, v, ver)`（L439/L568-589），key 的读旅程结束。

### 2.3 miss 之后：先本地 dht，再远端

- **fast local get**（L379-395）：`cctx.reserveForFastLocalGet(part, topVer)` 成功才尝试 `localDhtGet`——持 checkpoint 读锁（L478）直接 `dht.entryEx(key).innerGet(...)`（L483/L509-519）。拿到值：`metrics0().onRead(true)`（L527-528，注意**本地 dht 命中也算 hit**）且**不回填 near**（key 本节点就持有，near 化无意义）；topology 稳定且无 store 时直接终止搜索（L535-538）。
- **远端**（L397-436）：`selectAffinityNodeBalanced(affNodes, invalidNodes, ...)`（L399）挑目标（默认 primary，可被 backup 平衡/invalid 节点影响）；发起端**不是** owner 时先 `near.entryExx(key, topVer)` 建空 near 条目并 `reserveEviction()` 存进 `saved`（L414-425）——为回填占位；然后决定 `addRdr`（是否让对端登记 reader）：`tx == null || tx.optimistic()` 为真，READ_COMMITTED 且非写集也真（L427-431）——**悲观锁事务不加 reader**（锁本身保证一致性，避免死锁，见注释）。
- 本地分支（key 的 primary 是本机）则不建 MiniFuture，直接 `dht().getDhtAsync(n.id(), ...)` 读本地 dht（L233-283，`.../dht/GridDhtCacheAdapter.java:1100-1128`，落进 `GridDhtGetFuture`）。invalid partition 触发递归 remap（L249-268）。

### 2.4 primary 侧处理与 reader 登记

发起端 MiniFuture 发 **`GridNearGetRequest`**（L287-294/L736-754），key 表是 `LinkedHashMap<KeyCacheObject, Boolean>`——Boolean 就是每 key 的 addRdr 位（`near/GridNearGetRequest.java:74`；请求级 `addReaders()` 是 flags 位，L317-319）。primary 端：

- `GridDhtCacheAdapter#processNearGetRequest`（L1284-1344）→ `getDhtAsync(nodeId, ..., req.addReaders(), ...)` → **`GridDhtGetFuture`**；单 key 变体 `GridNearSingleGetRequest` 走 `processNearSingleGetRequest`（L1179-1278）→ `getDhtSingleAsync` → `GridDhtGetSingleFuture`（L1143-1173）。
- **reader 登记在 `GridDhtGetFuture.getAsync`**（`.../dht/GridDhtGetFuture.java:367-424`）：`addReaders && !skipVals && reader != 本机` 时逐 key（看 Boolean 值）`e.addReader(reader, msgId, topVer)`（L398）。`GridDhtCacheEntry#addReader`（L443-565）的三条拒绝规则：本机不加（L446）、已离线节点不加（L451-456）、**affinity 节点不加**（L459-465）；登记进 copy-on-write 的 `ReaderId[]`（L482-490）。TRANSACTIONAL 下新 reader 还要等 entry 上的活跃事务收尾（L492-499 起的 `GridCacheMultiTxFuture`）；ATOMIC 无此负担（"No transactions in ATOMIC cache"，L492）。

### 2.5 响应回填 near

`GridNearGetResponse` 携 `GridCacheEntryInfo` 集合回到发起端 → `GridNearCacheAdapter#processGetResponse`（L273-284）→ future 的 `loadEntries`（`GridNearGetFuture.java:613-684`）：

- **本节点是 owner 的 key 不进 near**（"Entries available locally in DHT should not be loaded into near cache"，L633-634 判 `keyLocalNode`）。
- 其余 key 从 `savedEntries` 取占位条目或新建，`entry.loadedValue(tx, nodeId, val, atomic ? info.version() : ver, info.version(), ttl, expireTime, ...)`（L641-650）。`loadedValue`（`GridNearCacheEntry.java:382-457`）持条目锁做版本门槛（§3.4），`update(val, ...)` 写入并记 `dhtVer`（L413-429），发 `EVT_CACHE_OBJECT_READ` 事件（L434-447）。
- future 完成时 `onDone` 里补发 **TTL 同步**：`cache().dht().sendTtlUpdateRequest(expiryPlc)`（`GridNearGetFuture.java:145-157`）——把访问过期的 ttl 变更用 `GridCacheTtlUpdateRequest` 通知 primary/backups/readers（`.../dht/GridDhtCacheAdapter.java:1352-1431`）；MiniFuture `onDone` 释放 eviction 预约（L762-770/L690-704）。

### 2.6 ATOMIC 与 TRANSACTIONAL 的读路径差异

| 维度 | ATOMIC near | TRANSACTIONAL near |
|---|---|---|
| 无 tx 读 | `GridNearAtomicCache#getAllAsync` → `loadAsync`（L415-446） | 同（`GridNearTransactionalCache#getAllAsync` L155-165） |
| 显式 tx 读 | 不适用（无显式 tx） | `tx.getAllAsync` 并入事务（L137-153）；read-through 走 `txLoadAsync` 带 tx 版本（L179-208） |
| 悲观锁 | 无（`lockAllAsync` 直接转 dht，L591-601） | `GridNearLockFuture`（L287-316），锁响应里 `resetFromPrimary` 刷新 near 值（§3.2） |
| reader 登记 | get 时登记（§2.4） | get 登记 + tx prepare 时对 near 发起节点再登记（§3.2）；悲观 tx get 不登记（L427-431） |
| 非 near 对照 | colocated client 用 `GridPartitionedSingleGetFuture` 发 `GridNearSingleGetRequest`，**`add reader=false`**（`.../dht/colocated/GridDhtColocatedCache.java:256-272`、`.../dht/GridPartitionedSingleGetFuture.java:346-361`，L356） | 同左——无 near 就无 reader 协议 |

### 2.7 near hit 的 metrics

`CacheMetricsImpl#onRead(boolean isHit)`（`cache/CacheMetricsImpl.java:942`）。near 读路径的三处打点：near peek 在 `innerGet` 内按值有无计 hit/miss（`GridCacheMapEntry.java:702-708`）；本地 dht 命中计 hit（`GridNearGetFuture.java:527-528`）；**发远端且本地连 near 条目都没有时计 miss**（L408-409 的 `!isNear` 条件防重复），远端值经 `loadedValue` 落地时再计一次 miss（`GridNearCacheEntry.java:402-403`）。即 CacheMetrics 的 hit/miss 是"本地视角"：near 命中与本地 dht 命中都算 hit——这正是"near 命中率可观测"（ROADMAP 章 5 tracer）的落点。

---

## 3. 失效与更新：primary 写了之后，near 怎么跟上

### 3.1 ATOMIC（03 号 §2.5/§3.4 已验主体，此处补全 reader 视角）

primary 在 `updateSingle` 的 near 段（`.../dht/atomic/GridDhtAtomicCache.java:2565-2679`；方法起于 L2527，写主体见 03 号 §2.5）里一次写、三路捎带：

1. **先快照 readers**：`entry.readersLocked()`（L2572）——reader 会在 remove 后被清，必须在 `innerUpdate` 前取。
2. **既有 readers（第三方 near 节点）**：`dhtFut.addNearWriteEntries(nearNode, readers, entry, ...)`（L2630-2639）→ `GridDhtAtomicAbstractUpdateFuture`（L233-292）把每个 reader（发起节点除外，L251）映射进 `mappings`，`updateReq.addNearWriteValue(...)`（L286-290）——**near 更新搭 backup 复制请求 `GridDhtAtomicAbstractUpdateRequest` 的便车**（纯 reader 节点单独发一份请求）。reader 离线则当场 `entry.removeReader(reader.nodeId(), -1L)`（L259-268）。
3. **发起节点自身**（`hasNear`，L2648-2677）：发起端非 owner 时 `res.addNearValue(i, newValue, ttl, expireTime)`（或 TRANSFORM/同值场景 `addNearTtl`，L2650-2659）捎进 `GridNearAtomicUpdateResponse`，并 `entry.addReader(nearNode.id(), ...)`（L2661-2666）——03 号 §3.4 的"捎带回传"即此；发起端变成 owner 则反向 `removeReader`（L2668-2670）。

接收端两个入口：

- **发起端**收响应：`GridNearAtomicSingleUpdateFuture#onPrimaryResponse → updateNear`（L293/L373）→ `GridNearAtomicCache#processNearAtomicUpdateResponse`（L135-217）：跳过 failed/skipped key（L163-171）；**"Reader became backup" 分支**——若本节点现在拥有该分区，直接 `markObsolete + removeEntry`（L172-180）；否则选值（primary 生成的值优先，否则请求原值，L182-194）进私有线程安全版 `processNearAtomicUpdateResponse(ver, key, val, ...)`（L230-302）：`entry.innerUpdate(..., /*primary*/false, /*check version*/true, ...)`（L251-280，L265-266）——**near 条目上的写也过版本检查**，旧响应写不进新值。
- **reader 端**收 `GridDhtAtomicAbstractUpdateRequest`：`GridDhtAtomicCache#processDhtAtomicUpdateRequest`（L3271-3462）在备份写完后处理 near 段：`((GridNearAtomicCache)near()).processDhtAtomicUpdateRequest(nodeId, req, nearRes)`（L3412-3416）。后者（`GridNearAtomicCache.java:310-412`）遍历 `req.nearSize()` 个 near 写，`peekEx` 找不到条目（已被 near 淘汰）就记进 `nearEvicted`（L335-343）；找到则 `innerUpdate(..., checkVersion=!forceTransformBackups)`（L355-384）；`obsoleteNearKeys` 段直接 `markObsolete + removeEntry`（L402-409，delete 场景的失效）。`nearEvicted` 随 `GridDhtAtomicUpdateResponse.nearEvicted` 回 primary（L3424-3431/L3459）→ `onDhtResponse` 里 `entry.removeReader(nodeId, res.messageId())`（`GridDhtAtomicAbstractUpdateFuture.java:491-505`）——**near 条目被淘汰的信息通过这条回报路径清理 reader 名单**。

### 3.2 TRANSACTIONAL：prepare/finish 消息对携带 near 更新

tx 提交后 near 侧的更新**不在 finish 响应里，而是把"给 near 的写"作为 prepare/finish 请求的一部分发给 reader 节点**：

1. **reader 映射**：primary 的 `GridDhtTxPrepareFuture#map` 把 dht 条目的 readers（排除 tx 发起节点与 primary/backup——`canSkipNearReader`，L1664-1681/L1705-1714）映射进 `tx.nearMap()`。
2. **prepare 携带**：`sendPrepareRequests`（L1388-1609）给 backup 的 `GridDhtTxPrepareRequest` 附 `nearWrites`（同为 backup 的 reader 合并进同一请求，L1403-1434）；**纯 near reader 单独收一份 `GridDhtTxPrepareRequest`**（nearMapping 不在 dhtMap 时，L1520-1608，构造 L1529-1545、发送 L1577）。backup 请求还带逐 key `invalidateNearEntry(idx, ...)` 标志——primary 发现该 backup 本身是 reader 时置位（L1444-1446：`cached.readerId(n.id()) != null` 且非发起节点）。
3. **reader 端建 near 微事务**：`processDhtTxPrepareRequest` 里 `startNearRemoteTx` 为 `nearWrites` 建 **`GridNearTxRemote`**（`transactions/IgniteTxHandler.java:1901-1919`，调用点 L1207）；backup 端收到 `invalidateNearEntry` 标志则立即失效自己的 near 条目（L1775-1776 → 私有 `invalidateNearEntry` L1882-1890：`nearEntry.invalidate(ver)`）。
4. **finish 提交**：primary 的 `GridDhtTxFinishFuture` 向 dhtMap 节点发 `GridDhtTxFinishRequest`（L437-510），**纯 near reader 也各收一份**（L512-545）；reader 端 `processDhtTxFinishRequest` 找回 `GridNearTxRemote` 并 `finish(nearTx, req)`（L1348-1415，L1373/L1397-1398）→ 提交时 `GridDistributedTxRemoteAdapter` 对每个写执行 `cached.innerSet/innerRemove`（near 条目本体），并 `nearCached.updateOrEvict(xidVer, val0, ...)` 双保险（L544-547 取 `cacheCtx.dht().near().peekExx`，L667-677 set 场景 / L703-705 delete 场景）。`updateOrEvict`（`GridNearCacheEntry.java:243-278`）先 `cctx.versions().onReceived`，`dhtVer` 匹配则清空，为 null（可淘汰）时 `markObsolete` 否则更新值。
5. **一阶段提交被禁用**：有纯 near reader 映射时 `recheckOnePhaseCommit` 把 `onePhaseCommit` 关掉（`GridDhtTxPrepareFuture.java:1373-1383`）——near reader 必须走完整两阶段。
6. **发起节点自身的 near**：不需要消息——`GridNearTxLocal` 的 `IgniteTxEntry.cached()` 就是 near 条目，本地提交循环直接 `cached.innerSet(..., cached.isNear() ? null : explicitVer, ..., dhtVer, ...)` 写 near（`transactions/IgniteTxLocalAdapter.java:601-609/L696-715`）；提交前先 `evictNearEntry(txEntry, false)` 把本节点已是 owner 的 near 条目 `markObsolete`（L577-579；判定 `isNearLocallyMapped` 在 `transactions/IgniteTxAdapter.java:1841-1874`，实现 L1881-1896）。primary（`GridDhtTxLocal`）侧另有 `updateNearCache` 覆写（`.../dht/GridDhtTxLocal.java:241-243`：near 发起节点非本机时为真）+ `updateNearEntrySafely` 更新 primary 自己的 near 条目（`IgniteTxLocalAdapter.java:723-749/933-956`）；**tx 的 reader 登记**发生在 pessimistic enlist：`GridDhtTxLocalAdapter#addReader` 调用点（L629）→ `GridDhtTxLocal#addReader` 把 near 发起节点登进 dht 条目 readers（L260-280）。悲观锁路径上 prepare 响应的 `ownedValues` 还会 `resetFromPrimary` 刷新 near 条目（`near/GridNearTxPrepareFutureAdapter.java:199-247`，L226-230；`GridNearCacheEntry.java:200-231`）。
7. **reader 淘汰回报（TX 版）**：reader 端 `GridNearTxRemote.evicted()`（near 写落空 = 条目已淘汰）随 `GridDhtTxPrepareResponse.nearEvicted` 回 primary（`IgniteTxHandler.java:1210-1223`）→ `GridDhtTxPrepareFuture$MiniFuture.onResult` 里 `cached.removeReader(nearMapping.primary().id(), res.messageId())`（L1897-1920）。

### 3.3 reader 集合的维护时机汇总

| 时机 | 动作 | 引用 |
|---|---|---|
| get/tx 登记 | `addReader`（三拒绝规则：本机/离线/affinity 节点） | `.../dht/GridDhtCacheEntry.java:443-565` |
| ATOMIC 写时 | 发起端非 owner → `addReader`；变 owner → `removeReader` | `.../dht/atomic/GridDhtAtomicCache.java:2661-2666/2668-2670` |
| near 条目被淘汰 | `nearEvicted` 回报 → `removeReader(nodeId, messageId)` | ATOMIC `.../atomic/GridDhtAtomicAbstractUpdateFuture.java:491-505`；TX `.../dht/GridDhtTxPrepareFuture.java:1897-1920` |
| reader 节点离开 | 分发更新时发现节点不在 → `removeReader(-1)` | `.../atomic/GridDhtAtomicAbstractUpdateFuture.java:259-268`；TX 映射时 `discovery().node(readerId) == null` 跳过（`.../dht/GridDhtTxPrepareFuture.java:1671-1674`） |
| 条目删除（tx） | `clearReader(originatingNodeId)` / `clearReaders()` | `cache/GridCacheMapEntry.java:1290-1322` |
| `removeReader` 的 msgId 门槛 | 老响应（msgId 更小）不能移走新登记 | `.../dht/GridDhtCacheEntry.java:573-610`（L591） |

near cache 关闭（节点离线/缓存销毁）没有显式"注销 reader"消息——靠"节点离开"与"nearEvicted 回报"两条惰性路径收敛；`clearReaders/clearReader`（L615-636）主要在条目失效/分区清空时使用。

### 3.4 版本一致性：near 条目凭什么信一个更新

1. **dhtVer 账本**：`loadedValue` 只在 `this.dhtVer == null || this.dhtVer.compareTo(dhtVer) < 0 || !valid(topVer)` 时接受新值（`GridNearCacheEntry.java:410`）；`recordDhtVersion` 只接受不小于现值的 dhtVer（L337-348）；`resetFromPrimary/updateOrEvict` 收包即 `cctx.versions().onReceived(primaryNodeId, dhtVer)`（L208/L252）喂全局版本管理器。
2. **near 上的 innerUpdate 带 verCheck**：ATOMIC 两处回写都传 `check version=true`（`GridNearAtomicCache.java:266/L370`），复用 primary 写路径同一套版本比较（03 号 §2.5 的 `innerUpdate` 机制），乱序/重复响应自然被拒。
3. **ATOMIC delete 历史**：ATOMIC 的 DELETE 与旧值重放可能同版本竞争，`GridNearAtomicCache` 用 `rmvQueue`（`GridCircularBuffer`，容量 `IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE`（默认 1,000,000）/10，系统 cache 100，L100-103）暂存 `(key, ver)` 已删记录，`onDeferredDelete` 入队、挤出时 `removeVersionedEntry`（L609-624；实现 `.../distributed/GridDistributedCacheAdapter.java:160`）——防"删除后旧值复活"。TRANSACTIONAL 无此队列（`GridNearTransactionalCache.java:583-585` 断言不可达）。
4. **拓扑失效兜底**：`valid(topVer)` 在 primary 变更/本节点变 backup 时判无效（L96-131）；`onInvalidate` 复位 `topVer/dhtVer`（L716-719）；"Reader became backup" 分支在 ATOMIC 回写入口再查一次分区归属（`GridNearAtomicCache.java:172-180`）。

---

## 4. 淘汰、clear 与生命周期

### 4.1 nearEvictionPolicy 与 dht eviction 的关系

- **两套独立策略**：dht 侧用 `CacheConfiguration.evictionPolicy(Factory)`（onheapCacheEnabled 时 `GridCacheEvictionManager`，否则 off-heap 淘汰）；**near 侧用 `NearCacheConfiguration.nearEvictionPolicyFactory`**（弃用的 `nearEvictPlc` 直配仍兼容）——`GridCacheEvictionManager` 构造时按 `cctx.isNear()` 二选一（`cache/GridCacheEvictionManager.java:64-67`；配置字段 `configuration/NearCacheConfiguration.java:41-49/104-118`）。默认 null = near 不淘汰（near 表常驻直到失效协议清它），这也是 near cache 必须"手动配淘汰"的原因。
- **联动是单向的**：near 淘汰只删本地条目 + 经 nearEvicted 回报让 primary 除名（§3.1/§3.3），不影响 dht；dht 淘汰条目时 primary 侧 `markObsolete` 后 readers 随条目一起消失（reader 数组挂在 dht entry 上）。`GridNearCacheAdapter#evict/evictAll` 则显式双侧同删（L341-351：`super.evict(key) & dht().evict(key)`）。
- **get 与淘汰的竞态**：`GridNearGetFuture` 在远端读期间对 near 条目 `reserveEviction()`（L417/L724-735），完成时释放（L690-704）——`evictionDisabled()` 以计数器为准（L755-759），淘汰管理器因此不会删掉"回填在途"的条目。tx 提交的 `evictNearEntry`（`IgniteTxAdapter.java:1881-1896`、`GridNearTransactionalCache.java:334-346`）同样只在"本节点已是 owner"时把 near 条目 obsolete 化。

### 4.2 clear / clearAll 对 near 的传播

- `cache.clear()`（集群语义）：`GridCacheAdapter#clear(keys)` **串行跑两次 compute 广播 `ClearTask`**（L1128-1133）——第一次 `near=false` 目标 affinity 节点清 server 数据（`forCacheNodes(name, !near, near, false)` 选组，L1150-1165），第二次 `near=true` 目标 near 节点。job 分派在 `ClearTask.map`：server 清 `GlobalClearAllJob`（srv=true, near=false，L5181-5202），near 清 `GlobalClearAllNearJob`（srv=false, near=true，L5251-5276；带 key 集的变体 L5207-5246/L5281-5307），选型 L6556-6563。
- **near-enabled server 的特殊性**：这类节点的 cache 实例就是 near 前端，`GridNearCacheAdapter#splitClearLocally` 覆写（L376-394）：affinity 节点把 dht 清理 job 包进 `GridNearCacheClearAllRunnable`（先跑 dhtJob 再清 near，`near/GridNearCacheClearAllRunnable.java:49-62`），client 节点退回 `super` 只清 near。`clearLocally/clearLocallyAll` 也双侧执行（L353-363）。
- `localClear/localClearAll` 只动本节点（`IgniteCacheProxyImpl.java:1661-1669`），不传播。

### 4.3 client near 与 "client 不 rebalance" 的衔接

near cache 是 **client 读加速的标准形态**（10 号 §4.3 已证 client 不 rebalance、put 永远转发 server）：client 带上 `reqNearCfg` 后，本地多了真实的 near 条目表 + reader 协议，get 从"每次 `GridNearSingleGetRequest` 打 primary"变成"near 命中零网络"。10 号报告的结论在 near 侧的对应面：`GridNearCacheAdapter#preloader()` 委托 dht（L134-136）而 client 的 dht 是 `GridNoStorageCacheMap`（§1.5）——near 自身无 preloader、无分区状态，client 的 cache 生命周期完全由 exchange 驱动（CLIENT 型 exchange，10 号 §4.3.2）。`GridNearGetFuture` 里 client 与 server near 的读路径同码，差异只在 `affNodes.contains(cctx.localNode())` 恒假（server near 节点对非本节点分区也走同一分支，L414-425）。

---

## 5. 分级结论：章 5 切课建议与可简化项

### 5.1 切课建议表（near cache 章，预计 3 课；ROADMAP 定位"读加速、reader 登记、primary 捎带回传"）

前提：章 1-4 已有 ATOMIC 数据面（put/get/affinity/IO）。**TRANSACTIONAL near 依赖章 6 事务**，按切课纪律（一课一个新概念簇）必须后置，见课 5.3 说明。

| # | 课 | 概念簇（新东西） | tracer（验证手段） | 依赖 |
|---|---|---|---|---|
| 5.1 | near 结构与读加速 | `GridNearCacheAdapter/Entry`、`GridCacheLocalConcurrentMap`、`near.dht` 互持与创建路径、`GridNearGetFuture` near-first 读、`loadDht/getDhtAsync`、`loadedValue` 回填、`createNearCache` API、hit/miss metrics | 2 server + 1 client：client 开 near 后循环 get，统计指标断言 near 命中率 >0、网络消息数随命中下降；关 near 对照 | 章 4（affinity/IO） |
| 5.2 | reader 登记与 ATOMIC 捎带回写 | `GridDhtCacheEntry.ReaderId[]`、`addReader` 三拒绝规则、`addNearValue` 响应捎带、`addNearWriteEntries` 请求捎带、`checkVer` 版本门、`nearEvicted` 回报除名、"reader became backup" | 3 节点剧本：client 读建立 reader → server put → 断言 client near 值更新且零额外消息（捎带）；client 淘汰 near 条目后再 put 断言 reader 被除名；节点增删触发 owner 变化 | 课 5.1 |
| 5.3 | near×事务与生命周期（或并入章 6） | `nearMap` reader 映射、`GridNearTxRemote`、prepare/finish 携带 near 写、`updateOrEvict/resetFromPrimary`、一阶段提交禁用、nearEvictionPolicy、clear 双广播 | 显式 tx 写后 client near 更新；near 配 LRU 后容量封顶；`cache.clear()` 后 near 与 dht 皆空 | 课 5.2 + 章 6 第一课（tx prepare/finish 消息对） |

课 5.3 若在章 5 开讲，须先讲 tx 最小闭环，等于一课双簇——**建议将 5.3 整体后挂到章 6（事务）末尾**作为"事务 × near"综合课，章 5 以 5.1+5.2 两课毕业（tracer "near 命中率可观测" 仅需 ATOMIC 即达成）。这与 ROADMAP"near cache（独立课）"的里程碑不冲突，且符合"宁多切课不一课双簇"。

### 5.2 可简化项（默认全做，仅列真正可争议项）

1. **ATOMIC delete 历史（`rmvQueue`）**：仅在"同 key 删除与旧值响应乱序重放"下才必要，课程若暂不做 delete/put 混合重放测试可推迟，但接口（`onDeferredDelete`）保留——它与真实 bug（删除后复活）直接相关，建议至少留一课内 TODO。
2. **`GridNearSingleGetRequest` 单 key 精简请求**：near 读统一走 `GridNearGetRequest`（批量）语义不损；单 key 优化是性能项，复刻课可最后做或不做。
3. **`GridNearLockFuture` 显式锁路径**（`CacheEntryProcessor` 之外的 `tryLock` API）：IGNITE-2.x 已弃用显式锁 API，若复刻范围不含 `IgniteCache.lock()`，`GridNearTransactionalCache` 的锁簇（L275-316/L349-580）可整体跳过。
4. **TTL 更新广播（`GridCacheTtlUpdateRequest`）**：expiryPlc 的跨节点同步属于正确性细节，可在"过期"概念课（章 1 已有扫描/过期）回头补，near 章只需留 `sendTtlUpdateRequest` 调用点。
5. **2 节点即可讲清主链**：reader 协议的最小演示拓扑是 1 server + 1 client near；"reader became backup" 需要 topology 变化剧本（3 节点伸缩），可作为课 5.2 的选做验收而非必做。

---

## 引用文件清单

以下路径均相对 `vendors/ignite/modules/core/src/main/java/`（除特别注明），全部实际打开验证：

**near 包（`org/apache/ignite/internal/processors/cache/distributed/near/`）**
- `GridNearCacheAdapter.java`（类声明 L69；`start` L91-98；entryFactory L103-113；`dht()` L118；`isNear` L129-131；`preloader` L134-136；`entryEx→initializeFromDht` L140-156；`loadAsync` L215-252；`processGetResponse` L273-284；size 族 L287-309；`evict/evictAll` L341-351；`clearLocally` 族 L353-363；`splitClearLocally` L376-394）
- `GridNearAtomicCache.java`（L77；dht 字段 L82；rmvQueue L85/L100-103；handler L112-117；`dht(dhtCache)` L122-129；`processNearAtomicUpdateResponse` 外层 L135-217（reader-became-backup L172-180）、内层 L230-302（primary=false/verCheck=true L265-266）；`processDhtAtomicUpdateRequest` L310-412（nearEvicted L335-343；obsolete 段 L402-409）；`getAllAsync` L415-446；put 转发 L449-476；`onDeferredDelete` L609-624）
- `GridNearTransactionalCache.java`（L59；dht 字段 L64；handler L86-97；`getAllAsync` L112-166（tx 分支 L137-153）；`txLoadAsync` L179-208；`lockAllAsync` L287-316；`evictNearEntry` L334-346；`onDeferredDelete` L583-585）
- `GridNearCacheEntry.java`（L51；字段 L53-65；`valid` L96-131；`initializeFromDht` L137-187；`resetFromPrimary` L200-231；`updateOrEvict` L243-278；`versionedValue` L301-319；`recordDhtVersion` L337-348；`readThrough` L350-366；`loadedValue` L382-457（metrics L402-403、版本门 L410）；no-op 族 L459-490；`onInvalidate` L716-719；eviction 预约 L724-759）
- `GridNearGetFuture.java`（L61；`init` L124-142；`onDone→sendTtlUpdateRequest` L145-157；`map` L169-305（本地分支 L233-283、远端分支 L284-303）；单 key `map` L315-458（near peek L338、miss L379、fastLocGet L380-395、miss 指标 L408-409、near 条目预约 L414-425、addRdr L427-436）；`localDhtGet` L467-561（onRead(true) L527-528）；`loadEntries` L613-684（L633-650）；`releaseEvictions` L690-704；MiniFuture L715-777）
- `GridNearGetRequest.java`（keyMap 字段 L74；`addReaders()` L317-319）
- `GridNearTxPrepareFutureAdapter.java`（`onPrepareResponse→resetFromPrimary` L199-247）
- `GridNearCacheClearAllRunnable.java`（L29；`run` 先 dht 后 near L49-62）
- `CacheVersionedValue.java`、`GridNearSingleGetRequest.java`/`GridNearSingleGetResponse.java`、`GridNearLockFuture.java`（存在性与用途，未逐行引）

**dht 侧（`.../distributed/dht/`）**
- `GridDhtCacheAdapter.java`（`getDhtAsync` L1100-1128；`getDhtSingleAsync` L1143-1173；`processNearSingleGetRequest` L1179-1278；`processNearGetRequest` L1284-1344；`sendTtlUpdateRequest` L1352-1431；`GridCacheTtlUpdateRequest` handler 注册 L389）
- `GridDhtGetFuture.java`（reader 登记块 L367-424，`e.addReader` L398）
- `GridDhtCacheEntry.java`（L60；`readers` L415-417；`addReader` L443-565；`removeReader` L573-610；`clearReaders/clearReader` L615-636）
- `GridDhtColocatedCache.java`（单 get → `GridPartitionedSingleGetFuture` L256-272）
- `GridPartitionedSingleGetFuture.java`（`GridNearSingleGetRequest` add reader=false L346-361）
- `atomic/GridDhtAtomicCache.java`（get handler 注册 L249-259；`updateSingle` near 段 L2565-2679（readersLocked L2572、addNearWriteEntries L2630-2639、hasNear 段 L2648-2677）；`processDhtAtomicUpdateRequest` L3271-3462（near 段 L3412-3431））
- `atomic/GridDhtAtomicAbstractUpdateFuture.java`（`addNearWriteEntries` L233-292；`onDhtResponse→removeReader` L491-508）
- `atomic/GridNearAtomicSingleUpdateFuture.java`（`updateNear` L373，调用点 L293/L632-645）
- `GridDhtTxPrepareFuture.java`（`recheckOnePhaseCommit` L1373-1383；`sendPrepareRequests` L1388-1609（invalidateNearEntry L1444-1446；纯 near reader 请求 L1520-1608）；reader 映射 L1664-1681；`canSkipNearReader` L1705-1714；nearEvicted 处理 L1897-1920）
- `GridDhtTxFinishFuture.java`（backup 段 L421-510；纯 near 节点段 L512-557）
- `GridDhtTxLocal.java`（`updateNearCache` L241-243；`addReader` L260-280）
- `GridDhtTxLocalAdapter.java`（nearMap 字段 L85；enlist 与 addReader 调用 L578/L629）
- `GridDhtTxRemote.java`（`updateNearCache` L255-264）
- `GridDhtTransactionalCacheAdapter.java`（`GridNearGetRequest`/`GridNearSingleGetRequest` handler L130-134）

**tx 侧（`.../cache/transactions/` 与 `.../cache/distributed/`）**
- `IgniteTxHandler.java`（`processDhtTxFinishRequest` L1348-1415（L1373/L1397-1398）；backup near 失效 L1775-1776；`invalidateNearEntry` L1882-1890；`startNearRemoteTx` L1901-1919；nearEvicted 回填 L1210-1223）
- `GridDistributedTxRemoteAdapter.java`（nearCached L544-547；`updateOrEvict` 调用 L667-677/L703-705）
- `IgniteTxLocalAdapter.java`（`evictNearEntry` L577-579；`updateNearCache`/metrics L589-591；dhtVer L601-609；innerSet 带 dhtVer L696-715；`updateNearEntrySafely` L723-749/L933-956）
- `IgniteTxAdapter.java`（`updateNearCache` 基类 L464-470；`isNearLocallyMapped` L1841-1874；`evictNearEntry` L1881-1896）

**结构与配置**
- `cache/GridCacheProcessor.java`（nearEnabled L1273；evictMgr L1277-1279；前端选择 L1335-1377；dht 创建与互持 L1397-1492（L1411/L1456-1462/L1473-1479）；`checkForAffinityNode` L2118-2129，调用点 L1988）
- `cache/GridCacheUtils.java`（`isNearEnabled` L444-457）
- `cache/GridCacheAdapter.java`（`splitClearLocally` L1019-1040；`clearLocallyAll` L1048-1053；`clear` L1128-1133；`executeClearTask` L1150-1165；`get` L1340；`getAsync→getAllAsync` L1761-1819；GlobalClear job 族 L5181-5307；ClearTask.map L6541-6569）
- `cache/GridCacheMapEntry.java`（onRead hit/miss L702-708；tx 删除清 reader L1290-1322）
- `cache/GridCacheLocalConcurrentMap.java`（L30-46）；`cache/GridNoStorageCacheMap.java`（L30-47）；`cache/GridCacheContext.java`（`dht()` L746、`near()` L767）；`cache/CacheMetricsImpl.java`（`onRead(isHit)` L942）；`cache/GridCacheEvictionManager.java`（near 策略装配 L64-67）
- `distributed/GridDistributedCacheAdapter.java`（`removeVersionedEntry` L160）
- `org/apache/ignite/configuration/NearCacheConfiguration.java`（字段 L41-49；弃用 API L78-96；factory L104-118）
- `org/apache/ignite/configuration/CacheConfiguration.java`（`DFLT_NEAR_START_SIZE = 1500000/4` L140）
- `org/apache/ignite/Ignite.java`（`createNearCache` L381、`getOrCreateNearCache` L394）；`org/apache/ignite/internal/IgniteKernal.java`（实现 L2305-2333/L2336-2369）
- `org/apache/ignite/internal/cluster/ClusterGroupEx.java`（`forCacheNodes(name, affNodes, nearNodes, clientNodes)` L33）

---

## 对复刻课的启示

1. **near cache 是"读路径插件"而非独立子系统**：复刻顺序上，先有 03 号的双实现类骨架（`GridDhtAtomicCache` 单类）就够，引入 near 时按 2.18 的做法"前端薄壳 + 互持指针"（`GridCacheProcessor.java:1397-1492`），写路径零改动（一行 `dht.put` 转发），只有读路径与失效协议是新增面——这决定了它天然是一个 2-3 课的章而非课弧。
2. **失效协议的分发名单（ReaderId[]）挂在 dht 条目上，不挂在 near 节点上**：这是"primary 驱动失效"的核心设计——near 节点从不主动失效（除本地淘汰），一切由 primary 的写路径捎带。复刻时把 `addReader/removeReader` 做成 `GridDhtCacheEntry` 的内聚能力，near 侧只实现"接受更新 + 版本门 + nearEvicted 回报"，两个模块即可独立测试。
3. **捎带（piggyback）是本章最有教学价值的手法**：ATOMIC 把 near 更新塞进已有的 update request/response（省一轮消息），TX 把 near 写塞进已有的 prepare/finish 消息对。课程应让学习者先测"没有捎带时多出的消息数"，再实现捎带，用消息计数差异做验收——tracer 天然可观测。
4. **"reader 变 owner 就退场"是贯穿性不变量**：`addReader` 拒绝 affinity 节点、`valid()` 检查 backup、`processNearAtomicUpdateResponse` 的 became-backup 分支、`evictNearEntry` 的 locallyMapped——同一规则在四处落地。复刻课应把它写成一条显式断言（任何时刻 near 条目的 key 不属于本节点的 owner 集合，除非条目正被清理），四处实现共享同一判定函数，避免教出四份散装逻辑。
5. **版本门（dhtVer + verCheck）是 near 正确性的全部**：near 没有锁、没有事务（ATOMIC 下），一致性完全依赖"更新必须带版本、条目只接受更新的版本"。复刻时先写乱序回放测试（延迟响应、重复响应、删除后旧值）再实现，能直接检验 §3.4 的四层机制。
6. **client 与 near 的关系要讲清一个反直觉点**：client 不开 near 也是完整 client（`GridNoStorageCacheMap` 的 colocated 形态 + `GridNearSingleGetRequest` 无 reader 读）；near 只是 client 的可选读缓存。课程拓扑建议固定"2 server + 1 client"，用 client 开/关 near 做同屏对照实验，顺带复习 10 号 §4.3。
7. **公共 API 面很小且 2.18 无 `withNearCache()`**：复刻公共层只需 `createNearCache/getOrCreateNearCache` + `NearCacheConfiguration`（两个淘汰配置 + startSize）。任务书或直觉里的 `withNearCache()` 不存在，不要发明。
