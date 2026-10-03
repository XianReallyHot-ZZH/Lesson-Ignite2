# 03 · 数据面主路径：一次 `cache.put(key, value)` 的完整旅程

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文只追踪**无事务的单次 put**，以 **ATOMIC 模式 + PARTITIONED cache（默认，无 near cache）** 为主链路；TRANSACTIONAL 只指出分叉点。正文中的短路径（如 `cache/...`、`.../dht/atomic/...`）是 `org/apache/ignite/internal/processors/cache/` 下的缩写，仅为排版省行；**文末"引用文件清单"给出全部完整可定位路径**（均已逐一验证存在），行号以当前 submodule 内容为准。

---

## 0. 先决事实（影响全篇的两个 2.17/2.18 变化）

1. **ATOMIC 与 TRANSACTIONAL 在 `put0` 处分叉**。ATOMIC cache 的实现类是 `GridDhtAtomicCache`，它覆写了 `put0/putAsync0`，走"atomic future 快速路径"（`GridNearAtomicSingleUpdateFuture`），**完全绕过事务对象**；TRANSACTIONAL（`GridDhtColocatedCache`/`GridNearTransactionalCache`/`GridDhtCache`）不覆写，落入 `GridCacheAdapter` 的通用路径，把单次 put 包装成一个**隐式单写的 `GridNearTxLocal` 微事务**。
2. **`CacheMode.LOCAL` 已被删除**。2.18 的 `org/apache/ignite/cache/CacheMode.java`（L29–50）只剩 `REPLICATED` 与 `PARTITIONED` 两种。因此本文的"本地路径"指的不是 LOCAL cache，而是 **key 的 primary 恰好是发起节点本身**这一情形。

cache 实现类的选择发生在 `org/apache/ignite/internal/processors/cache/GridCacheProcessor.java` L1335–1381、L1448–1492（ATOMIC+无 near → `GridDhtAtomicCache`；ATOMIC+near → `GridNearAtomicCache` 前端 + `GridDhtAtomicCache` 后端，`near.dht(dhtCache)` 互相持有；TRANSACTIONAL 对应 `GridDhtColocatedCache` / `GridNearTransactionalCache`+`GridDhtCache`）。

---

## 1. 调用链总览（ATOMIC 单 key put，10 步）

| # | 组件（类#方法） | 位置 | 一句话 |
|---|---|---|---|
| 1 | `IgniteCacheProxyImpl#put` | `cache/IgniteCacheProxyImpl.java` L1283 | 用户 API 入口，取内部 delegate（`IgniteInternalCache`，即 `GridCacheAdapter` 体系）并同步调用 `delegate.put(key, val)` |
| 2 | `GridCacheAdapter#put` → `put0` | `cache/GridCacheAdapter.java` L1930/L1972 | 计指标后进入 `put0`；**ATOMIC 在此分叉** |
| 3 | `GridDhtAtomicCache#put0` → `update0` | `.../dht/atomic/GridDhtAtomicCache.java` L621/L1144 | 覆写点：构造 `GridNearAtomicSingleUpdateFuture` 并调 `map()`，`.get()` 阻塞等结果 |
| 4 | `GridNearAtomicSingleUpdateFuture#mapOnTopology/map/mapSingleUpdate` | `.../dht/atomic/GridNearAtomicSingleUpdateFuture.java` L385/L424/L513 | key/val 转 `KeyCacheObject/CacheObject`，用 affinity 定位 owner 列表，primary = 第一个节点，打包 `GridNearAtomicSingleUpdateRequest` |
| 5 | `GridNearAtomicAbstractUpdateFuture#sendSingleRequest` | `.../dht/atomic/GridNearAtomicAbstractUpdateFuture.java` L305 | **本地/分布式分叉点**：primary 是本机 → 直接本地调用；否则走 IO 层发送 |
| 6 | `GridCacheIoManager#send` → `GridIoManager#sendToGridTopic` | `cache/GridCacheIoManager.java` L1192；`internal/managers/communication/GridIoManager.java` L2149 | 跨节点消息经 `TOPIC_CACHE` 主题送达 primary |
| 7 | `GridDhtAtomicCache#processNearAtomicUpdateRequest` → `updateAllAsyncInternal(0)` | `GridDhtAtomicCache.java` L3205/L1705/L1779 | primary 端统一入口（本地直调与网络请求汇合）：拿 checkpoint 读锁、`lockEntries` 锁定 on-heap entry |
| 8 | `update` → `updateSingle` → `GridDhtCacheEntry#innerUpdate` | `GridDhtAtomicCache.java` L1977/L2527；`cache/GridCacheMapEntry.java` L1433 | primary 分配写版本 `nextVersion()`，执行真正的写入：on-heap entry + off-heap B+ 树数据页 + WAL 记录 |
| 9 | `GridDhtAtomicAbstractUpdateFuture#map` → `sendDhtRequests` | `.../dht/atomic/GridDhtAtomicAbstractUpdateFuture.java` L366/L439 | primary 把已提交的值按 `AffinityAssignment` 复制给 backups（`GridDhtAtomicSingleUpdateRequest`），backup 端 `processDhtAtomicUpdateRequest`（`GridDhtAtomicCache.java` L3271）以 `primary=false, verCheck=true` 重复第 8 步 |
| 10 | 响应回程：`sendNearUpdateReply`（primary→发起端）/ `sendDhtNearResponse`（backup→发起端） | `GridDhtAtomicCache.java` L3681/L3455；`processNearAtomicUpdateResponse` L3227 | 发起端 future 完成（含 near cache 更新），`put()` 返回 |

---

## 2. 本地路径详解（发起节点即 primary，全程零网络）

以最常见的部署为例：server 节点上、默认 `PARTITIONED + ATOMIC`、未启用 near cache，且 key 落在本节点持有的分区。

### 2.1 API 入口与 delegate

- `IgniteCacheProxyImpl` 是 `IgniteCache` 的 proxy 实现（`IgniteCacheProxy` 在 2.18 已是接口，见 `cache/IgniteCacheProxy.java` L33）。`put(K,V)`（`IgniteCacheProxyImpl.java` L1283）通过 `getDelegateSafe()`（L235，拿 `this.delegate`，类型 `IgniteInternalCache<K,V>`）调用 `delegate.put(key, val)`。
- delegate 就是 `GridCacheProcessor` 在建 cache 时选出的 `GridCacheAdapter` 子类实例（ATOMIC 即 `GridDhtAtomicCache`；proxy 包装见 `GridCacheProcessor.java` L4729）。

### 2.2 ATOMIC 分叉：`put0` 被覆写

- `GridCacheAdapter#put(K,V,filter)`（L1943）做统计后调 `put0`（L1972，同步版）或 `putAsync0`（L2389）。
- **全 core 模块只有 `GridDhtAtomicCache` 覆写了 `put0`**（`GridDhtAtomicCache.java` L621）：

```java
@Override protected boolean put0(K key, V val, CacheEntryPredicate filter) {
    Boolean res = (Boolean)update0(key, val, null, null, false, filter, false).get(); ...
```

- 若启用了 near cache，发起端 delegate 是 `GridNearAtomicCache`，它的 `put` 一行直接转发：`dht.put(key, val, filter)`（`.../near/GridNearAtomicCache.java` L454），因此后续链路完全一致，只是 near 端缓存更新在 future 完成时补做（见 2.6）。

### 2.3 atomic future：映射与 primary 判定

- `update0`（L1144）→ `createSingleUpdateFuture`（L1228）创建 **`GridNearAtomicSingleUpdateFuture`**（单 key 专用；多 key 用 `GridNearAtomicUpdateFuture`，DC 复制冲突场景也退回后者，L1284–1326）。`expiryPlc == null` 时可用"单条目精简请求"`GridNearAtomicSingleUpdateRequest`（`canUseSingleRequest()`，L654）。
- `map()`（基类 `GridNearAtomicAbstractUpdateFuture.java` L252）：先取 `cctx.shared().lockedTopologyVersion(null)`，没有则 `mapOnTopology()`（`GridNearAtomicSingleUpdateFuture.java` L385）等待 `cache.topology().topologyVersionFuture()` 就绪后拿到 `AffinityTopologyVersion topVer`，再进 `map(topVer)`（L424）。
- `mapSingleUpdate`（L513–624）做三件事：
  1. `cctx.toCacheKeyObject(key)` / `cctx.toCacheObject(val)` —— 用户对象在此转为内部 `KeyCacheObject/CacheObject`（binary/字节数组形态），并 `validateKeyAndValue`；
  2. **affinity 定位**：`List<ClusterNode> nodes = cctx.affinity().nodesByKey(cacheKey, topVer)`（L536），`primary = nodes.get(0)`（L542）；
  3. 组装请求并 `new PrimaryRequestState(req, nodes, true)`。
- `map()` 尾部 `sendSingleRequest(req.nodeId(), req)`（L460）。

### 2.4 本地/远端分叉点（`sendSingleRequest`，L305–335）

```java
if (cctx.localNodeId().equals(nodeId)) {
    cache.updateAllAsyncInternal(cctx.localNode(), req, ...completionCb...);   // 本机 primary：方法调用，无消息
} else {
    cctx.io().send(req.nodeId(), wrapWithApplicationAttributes(req), cctx.ioPolicy()); // 远端 primary：走 IO 层
}
```

**本地路径就是左半边**：`GridNearAtomicSingleUpdateRequest` 对象只在内存里传递，不序列化、不进网络。

### 2.5 primary 写入与落盘（本地路径的核心段落）

`updateAllAsyncInternal`（L1705）先经 `ctx.group().preloader().request(...)` 确保分区 rebalance 数据就绪，然后 `updateAllAsyncInternal0`（L1779）：

1. `ctx.shared().database().checkpointReadLock()`（L1803）——写入全程持 checkpoint 读锁；
2. `lockEntries(req, topVer)`（L3003）：`entryExx(key, topVer)` 查/建 on-heap entry 并 `entry.lockEntry()`。`entryExx` 链路：`GridDhtCacheAdapter#entryExx`（`.../dht/GridDhtCacheAdapter.java` L452）→ `GridCacheAdapter#entryEx`（L943）→ `map.putEntryIfObsoleteOrAbsent(...)`。`map` 即 **on-heap 条目表 `GridCacheConcurrentMapImpl`**（`cache/GridCacheConcurrentMapImpl.java` L37，内部按 cacheId 分 `CacheMapHolder.map`，L64 的 `putEntryIfObsoleteOrAbsent` 完成"取到或新建 `GridDhtCacheEntry`"）；
3. `update(node, locked, req, res, ...)`（L1977）→ 单 key 走 `updateSingle`（L2527）：
   - `nextVersion()` 分配本次写的 `GridCacheVersion`（`GridCacheAdapter` L3584 → `ctx.versions().next(topVer)`）；
   - `ctx.affinity().assignment(topVer)`（L2548）取 `AffinityAssignment`（后面复制给 backup 用）；
   - **核心一行**：`entry.innerUpdate(ver, nearNode.id(), locNodeId, op, writeVal, ..., /*primary*/true, /*verCheck*/false, ..., dhtFut, false)`（L2574–2603），返回 `GridCacheUpdateAtomicResult`；
   - `dhtFut.addWriteEntry(affAssignment, entry, updRes.newValue(), ...)`（L2616）：登记要复制给 backups 的写；本地无 backup 时 mappings 为空；
   - near cache 场景下若发起节点不是该分区的 owner，还会 `res.addNearValue(...)` + `entry.addReader(...)`（L2650–2666）把发起节点登记为 reader。

**`GridCacheMapEntry#innerUpdate`（L1433）内部**（这是 2.17+ 重构后的新形态）：

- 组装私有闭包 `AtomicCacheUpdateClosure`（同文件 L4470，实现 `IgniteCacheOffheapManager.OffheapInvokeClosure`）；
- `cctx.offheap().invoke(cctx, key, localPartition(), c)`（L1519）——把"读旧值 → 算新值 → 写新行"整体下推给 **off-heap 存储层**执行：
  - `GridCacheOffheapManager$GridCacheDataStore#invoke`（`.../persistence/GridCacheOffheapManager.java` L2507）→ `CacheDataStoreImpl#invoke`（`cache/IgniteCacheOffheapManagerImpl.java` L1472）→ `dataTree.invoke(row, NO_KEY, c)`（L1497，B+ 树 `BPlusTree#invoke`，`.../persistence/tree/BPlusTree.java` L2128）；
  - 闭包 `call(oldRow)`（L4633）在树内、持锁状态下执行：过期检查 →（read-through 时 `cctx.store().load`）→ filter/冲突/版本检查 → `update(...)`（L4918）产出 `newRow` 与 `treeOp=PUT/REMOVE/NOOP`；
  - `update(...)` 中的落盘三连（关键行）：
    - **write-through**：`cctx.store().put(null, entry.key, updated, newVer)`（L5036–5038，配置了 `CacheStore` 才生效）；
    - **WAL**：`entry.logUpdate(op, updated, newVer, newExpireTime, updateCntr0, primary)`（L5056 → L3459，"只在 ATOMIC cache 逐条记 WAL"，写 `DataRecord/DataEntry`，primary 标志随 `DataEntry.flags(primary)` 落盘）；
    - **数据页**：`entry.localPartition().dataStore().createRow(cctx, key, updated, newVer, newExpireTime, oldRow)`（L5059 → `CacheDataStoreImpl#createRow`，`IgniteCacheOffheapManagerImpl.java` L1528：值序列化进 `DataRow`，经 `CacheDataRowStore/FreeList` 写入 `PageMemory` 数据页，复用或新分配 link）；
  - 树操作完成后 `finishUpdate`（L1674）：分区 size 计数、查询索引 `qryMgr.store(newRow, oldRow, true)`、TTL 过期 pending 树维护；
  - 最后 `entry.update(updated, ...)`（L5073 → `GridCacheMapEntry#update` L2147）把 on-heap entry 的 `val/ver/expires` 更新（on-heap 值是否常驻由 `onheapCacheEnabled` 与 off-heap 淘汰管理器决定——`GridCacheProcessor.java` L1411：`isOnheapCacheEnabled() ? GridCacheEvictionManager : CacheOffheapEvictionManager`）。

4. 回到 `updateAllAsyncInternal0` 的收尾（L1908–1964）：`unlockEntries` 释放 entry 锁；`ctx.shared().wal().flush(null, false)` 刷 WAL；`dhtFut.map(node, res.returnValue(), res, completionCb)`（L1958）——本地无 backup 时 `GridDhtAtomicAbstractUpdateFuture#map`（L366）发现 `mappings` 为空，直接 `completionCb.apply(...)` + `onDone()`（L370–378），**不产生任何网络消息**；
5. completionCb 即 `updateReplyClos`（`init()` 中定义，L226–240）：非 `FULL_ASYNC` 时 `sendNearUpdateReply(res.nodeId(), res)`（L3681）——但 res.nodeId 就是本机，同步 put 的结果经 future 链直接回到第 3 步 `.get()` 处。`put()` 返回。

### 2.6 本地路径小结

```
IgniteCacheProxyImpl#put
  └─ GridCacheAdapter#put → GridDhtAtomicCache#put0
       └─ update0 → GridNearAtomicSingleUpdateFuture.map()
            ├─ mapSingleUpdate: toCacheKeyObject/toCacheObject + affinity.nodesByKey → primary=本机
            └─ sendSingleRequest(本机) → updateAllAsyncInternal(0)
                 ├─ lockEntries → GridCacheConcurrentMapImpl.putEntryIfObsoleteOrAbsent → GridDhtCacheEntry
                 ├─ updateSingle → entry.innerUpdate(ver, primary=true)
                 │    └─ cctx.offheap().invoke(AtomicCacheUpdateClosure)
                 │         ├─ CacheStore.put（write-through，可选）
                 │         ├─ logUpdate → WAL DataRecord
                 │         └─ dataStore().createRow → PageMemory 数据页 + B+ 树 PUT + finishUpdate（索引/计数）
                 ├─ wal.flush + unlockEntries
                 └─ dhtFut.map（无 backup → 零网络，future 完成）
```

---

## 3. 分布式路径详解（primary 在其他节点）

适用情形：client 节点发起、或 server 节点上 key 不归自己。步骤 1–4 与本地路径完全相同（同在发起节点执行），从 `sendSingleRequest` 的 else 分支开始不同。

### 3.1 跨节点通信（发起端 → primary）

- `cctx.io().send(nodeId, req, plc)` = `GridCacheIoManager#send`（L1192）：挂上 `lastAffinityChangedTopologyVersion`（L1195，供对端判断是否需要 remap），重试若干次后 `cctx.gridIO().sendToGridTopic(node, TOPIC_CACHE, msg, plc)`（L1207）。
- `GridIoManager#sendToGridTopic`（`org/apache/ignite/internal/managers/communication/GridIoManager.java` L2149）把消息按 IO policy（public/system pool）交给底层通信（TCP）发送。消息类就是 **`GridNearAtomicSingleUpdateRequest`**（继承 `GridNearAtomicAbstractUpdateRequest`；带 filter 的变体 `GridNearAtomicSingleUpdateFilterRequest`、invoke 变体 `GridNearAtomicSingleUpdateInvokeRequest`、多 key 的 `GridNearAtomicFullUpdateRequest`——注意 **2.18 中没有 `GridCachePutRequest` 这种类**，put 的载体是这组 atomic update 请求/响应）。

### 3.2 primary 端接收

- `GridCacheIoManager` 在 kernal 启动时向 `GridIoManager` 注册了 `TOPIC_CACHE` 监听（`GridCacheIoManager.java` L497），收到后 `handleMessage`（L315）按 **cacheId + 消息 lookupIndex（消息类）** 从 `idxClsHandlers` 找到对应 handler。
- 这些 handler 是各 cache 在 `onKernalStart()` 里注册的。`GridDhtAtomicCache`（L249–410）注册了：`GridNearAtomicAbstractUpdateRequest → processNearAtomicUpdateRequest`（L261–279/L3205）、`GridDhtAtomicAbstractUpdateRequest → processDhtAtomicUpdateRequest`（L322–340/L3271，backup 用）、`GridNearAtomicUpdateResponse → processNearAtomicUpdateResponse`（L302/L3227）、`GridDhtAtomicNearResponse → processDhtAtomicNearResponse`（L382/L388）等。
- `processNearAtomicUpdateRequest`（L3205）查出发起节点后调用 `updateAllAsyncInternal(node, req, updateReplyClos)` —— **与本地路径第 7–8 步完全同一段代码**。区别仅在于：`node` 是远端节点，版本号由本 primary 生成，结果需要回发。

### 3.3 primary/backup 语义与备份复制

- **primary 判定**：affinity owner 列表第一个节点（`GridNearAtomicSingleUpdateFuture` L542 `nodes.get(0)`）。发起端既不知道也不协商，直接把请求发给它。
- **写版本唯一来源是 primary**：`update`（L1990）`nextVersion()` 生成 `GridCacheVersion`；backup 不自行生成版本，靠 `req.writeVersion()` 带来的 `ver` 做版本检查，保证副本间顺序一致。
- **backup 集合确定**：`GridDhtAtomicAbstractUpdateFuture#addWriteEntry`（L146–209）用 `affAssignment.get(entry.partition())`（即该分区的 `primary + backups` 列表）减去 primary 自身（`!nodeId.equals(cctx.localNodeId())`，L180），为每个 backup 累积一个 `GridDhtAtomicAbstractUpdateRequest`（单 key 实现为 `GridDhtAtomicSingleUpdateRequest`）。若 client 已知 rebalance 完成（`req.affinityMapping()`），直接用纯 affinity 节点；否则用 `cctx.dht().topology().nodes(part, affAssignment, affNodes)` 修正（L164–165）。
- **发送**：primary 本地写完后 `dhtFut.map(...)`（L1958）→ `sendDhtRequests`（L439–473）逐 backup `cctx.io().send(req.nodeId(), req, cctx.ioPolicy())`；`FULL_SYNC` 时在请求上附 `nearReplyInfo(nearNode.id(), futId)`（L445），允许 backup 直接向发起端汇报。
- **backup 写入**：`processDhtAtomicUpdateRequest`（L3271）同样拿 checkpoint 读锁，逐 key `entryExx(key)` → `entry.innerUpdate(ver, ..., /*primary*/false, /*verCheck*/!req.forceTransformBackups(), ..., req.transformOperation())`（L3330–3360）——与 primary 同一套 `AtomicCacheUpdateClosure`/off-heap/WAL 代码，只是角色标志不同（`DataEntry.flags(primary)` 区分，恢复时据此判定）；随后 `ctx.shared().wal().flush`（L3438）。

### 3.4 响应回程与 future 完成

- backup 完成后：`FULL_SYNC` → `sendDhtNearResponse(req, nearRes)`（L3455，`GridDhtAtomicNearResponse` 直发发起节点）；否则向 primary 发**延迟确认** `sendDeferredUpdateResponse`（L3461，`GridDhtAtomicDeferredUpdateResponse`，可批量）。
- primary 完成（含本地写与 backup 发送）后由 `updateReplyClos` → `sendNearUpdateReply`（L3681）向发起节点回 **`GridNearAtomicUpdateResponse`**（含返回值、near 值、错误、remap 版本）。
- 发起节点：`processNearAtomicUpdateResponse`（L3227）→ `ctx.mvcc().atomicFuture(futId)` 找回 future → `fut.onPrimaryResponse(...)`；`GridDhtAtomicNearResponse` 则走 `onDhtResponse`（FULL_SYNC 下等 primary + 所有 backup 都应答才算完成，见 `GridDhtAtomicAbstractUpdateFuture.map` L382–414 的 `needReplyToNear/needMapping` 判定与 `updateRes.mapping(...)`）。
- 若发起端启用了 near cache：`finishUpdateFuture → updateNear(req, res)`（`GridNearAtomicSingleUpdateFuture.java` L632–649/L373）→ `GridNearAtomicCache#processNearAtomicUpdateResponse`（L135）更新本地 near 条目（primary 在 `res.addNearValue` 里已把新值捎回）。
- future 完成 → `put0` 的 `.get()` 返回 → `IgniteCacheProxyImpl#put` 返回。若中途 topology 变化，响应带 `remapTopologyVersion`，future 在新 topology 上重映射（`waitAndRemap`，L636；重试次数上限默认 `MAX_RETRIES`）。

### 3.5 时序（文字版，3 节点：发起端 N、primary P、backup B）

```
N            P (primary)                        B (backup)
│ map(): nodesByKey(key, topVer)
│            → owners=[P,B], primary=P
│──GridNearAtomicSingleUpdateRequest──────────▶│
│             lockEntries(P.onHeap)
│             innerUpdate(ver=P.nextVersion())
│               store.put? → WAL DataRecord → dataStore.createRow → B+树PUT
│             addWriteEntry(affAssignment) → backups=[B]
│             ├──GridDhtAtomicSingleUpdateRequest(writeVer=ver)──▶│
│             │                                  innerUpdate(ver, primary=false, verCheck)
│             │                                  WAL → 数据页 → flush
│             │                                  ◀─FULL_SYNC: GridDhtAtomicNearResponse─┤(直发N)
│◀──────────GridNearAtomicUpdateResponse─────────│ (sendNearUpdateReply)
│ onPrimaryResponse(+onDhtResponse) → (updateNear) → future done → put() 返回
```

`writeSynchronizationMode` 的影响：`FULL_SYNC` 要等 backups 确认（N 收 primary 响应 + B 的 `GridDhtAtomicNearResponse`）；`PRIMARY_SYNC` primary 本地完成即返回（`needReplyToNear` 判定含 `PRIMARY_SYNC` 分支，`GridDhtAtomicAbstractUpdateFuture` L382）；`FULL_ASYNC` 发出请求即完成（`GridNearAtomicSingleUpdateFuture#map` L462–466）。

---

## 4. 本地 vs 分布式差异对照表

| 维度 | 本地路径（发起端=primary） | 分布式路径（primary 在远端） |
|---|---|---|
| 分叉代码 | `sendSingleRequest` if 分支（`GridNearAtomicAbstractUpdateFuture.java` L306） | 同方法 else 分支（L316） |
| 进入 primary 的方式 | 进程内方法调用 `cache.updateAllAsyncInternal(...)` | `GridCacheIoManager.send → GridIoManager.sendToGridTopic(TOPIC_CACHE)`，primary 端 `handleMessage → processNearAtomicUpdateRequest` |
| 网络消息数 | 0（无 backup 时）；有 backup 时仅 primary→backup 的下行复制 | ≥2（N→P 请求 + P→N 响应；FULL_SYNC 再加 B→N 的 `GridDhtAtomicNearResponse`） |
| primary 端执行代码 | **完全相同**（`updateAllAsyncInternal0 → updateSingle → innerUpdate`） | 完全相同，只是 `nearNode` 参数为远端节点 |
| 写版本分配 | 本机 `nextVersion()` | primary 节点分配，随 `GridDhtAtomicSingleUpdateRequest.writeVersion` 传给 backups |
| near cache 值回传 | 不需要（本机即数据持有者） | primary 在响应中 `addNearValue` 捎带新值 + `addReader` 登记 |
| remap/重试 | topology 失配时同样 remap（本地也有 `GridDhtInvalidPartitionException` 分支，L1927） | 额外有 `ClusterTopologyServerNotFoundException`、`onNodeLeft`、`checkDhtNodes`（`GridNearAtomicCheckUpdateRequest`）等网络失败路径 |
| 返回时机 | future 在本机写入（及 backup 复制发出）后完成 | 按 syncMode：等 primary（±backups）响应后完成 |

---

## 5. TRANSACTIONAL 的分叉点（不展开）

- **分叉点就是 `GridCacheAdapter#put0`（L1972）**：TRANSACTIONAL cache（`GridDhtColocatedCache`/`GridNearTransactionalCache`/`GridDhtCache`）不覆写它，于是走：
  `put0` → `syncOp`（L3679）→ `ctx.tm().newTx(implicit=true, implicitSingle=true, OPTIMISTIC, READ_COMMITTED, ...)`（`transactions/IgniteTxManager.java` L681，创建**隐式单写微事务 `GridNearTxLocal`**）→ `GridNearTxLocal#putAsync`（`distributed/near/GridNearTxLocal.java` L439）→ `putAsync0`（L570）→ `enlistWrite`（L890）/`enlistWriteEntry`（L1214，在 near 端登记 `IgniteTxEntry`）→ `optimisticPutFuture`（L2615，implicit 时直接 `commitNearTxLocalAsync` L3442）→ `prepareNearTxLocal`（L3363，创建 `GridNearOptimisticTxPrepareFuture`）→ `prepare0/prepareSingle`（`GridNearOptimisticTxPrepareFuture.java` L314/L351）→ `map`（L620，这里用 `cacheCtx.topology().nodes(affinity.partition(key), topVer)` 做 owner 定位）→ `proceedPrepare`（L488）。
- `proceedPrepare` 内的本地/远端分叉与 atomic 同构：primary 是本机 → `cctx.tm().txHandler().prepareColocatedTx`（`transactions/IgniteTxHandler.java` L333，本地直接 `GridNearTxLocal#prepareAsyncLocal` L3700 起 `GridDhtTxPrepareFuture`）；primary 在远端 → `cctx.tm().sendTransactionMessage(n, req, ...)` 发送 **`GridNearTxPrepareRequest`**，primary 端 `processNearTxPrepareRequest`（L144，handler 注册见 L205–238）→ `prepareNearTx`（L421）→ 为该请求 new 一个 **`GridDhtTxLocal`**（L551）→ `tx.prepareAsync(req)`（L616）；备份复制用 `GridDhtTxPrepareRequest`（handler L217–218）。
- 单 key 隐式写通常可"一阶段提交"：`checkOnePhase`（`GridNearTxPrepareFutureAdapter.java` L168–192，单 primary 且 backups ≤ 1 且无 write-through store 时 `onePhaseCommit=true`），此时 DHT prepare 即提交（`GridDhtTxPrepareFuture#onDone` 的 `tx.commitOnPrepare()` 分支，`distributed/dht/GridDhtTxPrepareFuture.java` L764–789 `tx.commitAsync()`）。
- 换言之：**ATOMIC 的"版本+复制"逻辑内联在 `GridDhtAtomicCache.updateSingle` 一趟完成；TRANSACTIONAL 把同样的"near 登记 → primary prepare（`GridDhtTxLocal`）→ backup 复制 → commit"拆进显式的 tx 状态机与 prepare/finish 消息对**。

---

## 6. 引用文件清单

以下路径均相对 `vendors/ignite/modules/core/src/main/java/`（除特别注明）：

**入口与 cache 实现**
- `org/apache/ignite/internal/processors/cache/IgniteCacheProxy.java`（接口，L33）
- `org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java`（`#put` L1283、`getDelegateSafe` L235）
- `org/apache/ignite/internal/processors/cache/GridCacheProcessor.java`（cache 实现选择 L1335–1381、L1448–1492；evictMgr L1411；proxy 包装 L4729）
- `org/apache/ignite/cache/CacheMode.java`（仅 REPLICATED/PARTITIONED，L29–50）
- `org/apache/ignite/internal/processors/cache/GridCacheAdapter.java`（`put` L1930、`put0` L1972、`entryEx` L943、`nextVersion` L3584、`syncOp` L3679）

**ATOMIC 主链路**
- `org/apache/ignite/internal/processors/cache/distributed/dht/atomic/GridDhtAtomicCache.java`（`put0` L621、`update0` L1144、`createSingleUpdateFuture` L1228、handler 注册 L249–410、`updateReplyClos` L226、`updateAllAsyncInternal` L1705、`updateAllAsyncInternal0` L1779、`update` L1977、`updateSingle` L2527、`lockEntries` L3003、`processNearAtomicUpdateRequest` L3205、`processDhtAtomicUpdateRequest` L3271、`sendNearUpdateReply` L3681）
- `org/apache/ignite/internal/processors/cache/distributed/dht/atomic/GridNearAtomicAbstractUpdateFuture.java`（`map` L252、`sendSingleRequest` L305、`addWriteEntry` L146、`map` L366、`sendDhtRequests` L439）
- `org/apache/ignite/internal/processors/cache/distributed/dht/atomic/GridNearAtomicSingleUpdateFuture.java`（`updateNear` L373、`mapOnTopology` L385、`map` L424、`mapSingleUpdate` L513、`finishUpdateFuture` L632、`canUseSingleRequest` L654）
- 消息类（同目录）：`GridNearAtomicAbstractUpdateRequest.java`、`GridNearAtomicSingleUpdateRequest.java`、`GridNearAtomicSingleUpdateFilterRequest.java`、`GridNearAtomicSingleUpdateInvokeRequest.java`、`GridNearAtomicFullUpdateRequest.java`、`GridNearAtomicUpdateFuture.java`、`GridNearAtomicUpdateResponse.java`、`GridDhtAtomicAbstractUpdateRequest.java`、`GridDhtAtomicSingleUpdateRequest.java`、`GridDhtAtomicAbstractUpdateFuture.java`、`GridDhtAtomicUpdateFuture.java`、`GridDhtAtomicNearResponse.java`、`GridDhtAtomicDeferredUpdateResponse.java`、`GridNearAtomicCheckUpdateRequest.java`
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearAtomicCache.java`（`put→dht.put` L454、`processNearAtomicUpdateResponse` L135、`processDhtAtomicUpdateRequest` L310）

**affinity 定位**
- `org/apache/ignite/internal/processors/cache/GridCacheAffinityManager.java`（`partition` L144/L157、`affinityKey` L180、`nodesByKey` L194、`nodesByPartition` L203、`assignment` L218）
- `org/apache/ignite/internal/processors/affinity/GridAffinityAssignmentCache.java`（`nodes` L684、`cachedAffinity` L779）
- `org/apache/ignite/cache/affinity/rendezvous/RendezvousAffinityFunction.java`（`hash` L478、`partition` L504、`assignPartitions` L513）
- `org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridDhtPartitionTopologyImpl.java`（`nodes` L1180，"primary is first" L1196；tx 路径与 `addWriteEntry` 的 topology 修正共用）

**on-heap entry 与存储落地**
- `org/apache/ignite/internal/processors/cache/GridCacheConcurrentMapImpl.java`（`getEntry` L55、`putEntryIfObsoleteOrAbsent` L64）
- `org/apache/ignite/internal/processors/cache/GridCacheMapEntry.java`（`value` L269、`update` L2147、`innerUpdate` L1433、`logUpdate` L3459、`AtomicCacheUpdateClosure` L4470、`call` L4633、`update`（闭包内）L4918、store/WAL/createRow 三连 L5036–5073）
- `org/apache/ignite/internal/processors/cache/IgniteCacheOffheapManagerImpl.java`（`CacheDataStoreImpl` L1196、`invoke` L1472、`invoke0` L1493、`createRow` L1528、`finishUpdate` L1674）
- `org/apache/ignite/internal/processors/cache/persistence/GridCacheOffheapManager.java`（`GridCacheDataStore` L1702、`invoke` L2507、数据页/B+树组装 L1893–1979）
- `org/apache/ignite/internal/processors/cache/persistence/tree/BPlusTree.java`（`invoke` L2128）

**跨节点通信**
- `org/apache/ignite/internal/processors/cache/GridCacheIoManager.java`（监听注册 L497、`handleMessage` L315、`send` L1192）
- `org/apache/ignite/internal/managers/communication/GridIoManager.java`（`sendToGridTopic` L2149）

**TRANSACTIONAL 分叉（对照用）**
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxManager.java`（`newTx` L681）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxLocal.java`（`putAsync` L439、`putAsync0` L570、`enlistWrite` L890、`enlistWriteEntry` L1214、`optimisticPutFuture` L2615、`prepareNearTxLocal` L3363、`commitNearTxLocalAsync` L3442、`prepareAsyncLocal` L3700、`entryEx` L3944）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearOptimisticTxPrepareFuture.java`（`prepare0` L314、`prepareSingle` L351、`proceedPrepare` L488、`map` L620）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxPrepareFutureAdapter.java`（`checkOnePhase` L168）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxHandler.java`（handler 注册 L205–238、`prepareColocatedTx` L333、`prepareNearTx` L421、`processNearTxPrepareRequest` L144）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxPrepareFuture.java`（`commitOnPrepare` 分支 L764–789）
