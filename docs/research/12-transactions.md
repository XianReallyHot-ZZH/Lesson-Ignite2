# 12 · 事务子系统：`GridNearTxLocal` 一族的完整生命周期

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文承接 03 号 §5（隐式微事务 put 全链、`checkOnePhase` 部分条件、`IgniteTxHandler` 注册——均不重查，直接引用），把事务子系统全量铺开：类型体系、悲观/乐观全链、消息族全表、恢复与 exchange 交互、CacheStore 集成。正文短路径（`transactions/`、`near/`、`dht/`）是 `org/apache/ignite/internal/processors/cache/` 下的缩写，仅为排版省行；**文末"引用文件清单"给出全部完整可定位路径**（均已逐一验证），行号以当前 submodule 内容为准。与 11 号 §3.2（near×tx）、10 号（ExchangeLatchManager、write-behind flush、`txTimeoutOnPartitionMapExchange`）结论交叉引用处不再重复展开。

---

## 0. 先决事实（本篇核验推翻/确认的五个假设）

1. **`TransactionIsolation` 只有 3 个值，不是 4**：`READ_COMMITTED` / `REPEATABLE_READ` / `SERIALIZABLE`（`org/apache/ignite/transactions/TransactionIsolation.java`，公开 API，L26–34）。矩阵是 **3 隔离 × 2 并发 = 6 组合**（`TransactionConcurrency` 确为 `OPTIMISTIC`/`PESSIMISTIC` 两值，L26–38）。另：`TransactionState` 有 **10** 个值（含 `SUSPENDED`/`UNKNOWN`，`TransactionState.java` L26–54）。
2. **悲观 tx 路径确实使用 `GridNearLockFuture`**——但不是"专门的 tx acquire future"：tx 的每次写操作在 enlist 阶段经 `txLockAsync` 走到与**显式锁 API（`cache.lockAll`）完全相同**的 lock future 族（无 near → `GridDhtColocatedLockFuture`；有 near → `GridNearLockFuture`），`GridNearPessimisticTxPrepareFuture` 这个名字才是 2.18 悲观 prepare 的真身（不存在 `GridNearPessimisticTxAcquireFuture`），它**只负责映射与发 prepare 请求，不负责锁**（§2.1）。
3. **savepoint 不存在**：`org/apache/ignite/transactions/Transaction` 全文无 savepoint；JDBC 层 `setSavepoint` 直接抛 `SQLFeatureNotSupportedException`（`internal/jdbc/thin/JdbcThinConnection.java` L807–826）。`TransactionChanges`（`transactions/TransactionChanges.java` L29–31）是 SQL"事务感知查询"的变更载体（changedKeys + newAndUpdatedEntries），与 savepoint 无关。**复刻无需实现**。
4. **tx mapping 不存 utility cache**：映射全在内存对象（`IgniteTxMappings`/`GridDhtTxMapping`/`tx.transactionNodes()`）与请求消息字段里；utility cache 是事务子系统的**客户**（以"系统事务"身份运行），不是事务状态的存储（§5.5）。
5. **没有 `GridCacheTxRecoveryThread`**：节点崩溃恢复是 discovery 高优先级监听器 + `TxRecoveryInitRunnable` + `GridCacheTxRecoveryFuture` 的消息共识流程（§5.1）。另两个名字勘误：不存在 `GridTxMapped`（真名 `GridCacheMappedVersion`，near→dht 版本映射接口）；回滚没有独立消息族——`commit=false` 复用 finish 请求对（§2.3）。

---

## 1. 类型体系与三个核心组件

### 1.1 六个 tx 形态（谁在哪端持有什么）

| 类（声明行） | 所在端 | 形态 |
|---|---|---|
| `GridNearTxLocal`（`near/GridNearTxLocal.java` L131，extends `GridDhtTxLocalAdapter`） | **发起节点** | 用户/隐式事务本地对象；持有 near/colocated 条目（`IgniteTxEntry`）、映射（`IgniteTxMappings`）、`transactionNodes`；隐式单写微事务（03 号 §5）也是它（`implicit=true, implicitSingle=true`） |
| `GridDhtTxLocal`（`dht/GridDhtTxLocal.java` L67，extends `GridDhtTxLocalAdapter`，implements `GridCacheMappedVersion`） | **primary 端** | 收到 `GridNearTxPrepareRequest` 后由 `IgniteTxHandler#prepareNearTx` 创建（L551）；负责 primary 侧加锁、版本分配、向 backup 复制；`GridDhtTxLocalAdapter` 是"发起节点 colocated 本地部分"与"primary dht 部分"的公共底座 |
| `GridNearTxRemote`（`near/GridNearTxRemote.java` L49，extends `GridDistributedTxRemoteAdapter`） | **reader/backup 端** | 为 `GridDhtTxPrepareRequest.nearWrites` 建的 near 条目微事务（11 号 §3.2 第 3 步），只做条目 set/remove，不参与决议 |
| `GridDhtTxRemote`（`dht/GridDhtTxRemote.java` L52） | **primary/backup 端** | 远端 dht tx；write-through 时也持 `storeWriteThrough` 标志（L60） |
| `GridDistributedTxRemoteAdapter`（`distributed/`） | 远端公共 | 远端写阶段：对每个条目 `innerSet/innerRemove`，write-to-store-from-dht 时 `batchStoreCommit`（L500） |
| `IgniteTxState` 族（`transactions/`） | 全部 | 条目容器拆分：`IgniteTxStateImpl`（单 `txMap` + read/write 两个过滤视图，L63–71）；`IgniteTxImplicitSingleStateImpl`（隐式单 key 的单例列表优化，L50–55）；`IgniteTxRemoteStateImpl`（远端 readMap/writeMap，L43–47） |

### 1.2 `IgniteTxManager`（`transactions/IgniteTxManager.java`，L161）

- **`newTx` 参数表**（L681–729）：`implicit`、`implicitSingle`、`sysCacheCtx`（系统事务的 utility cache 上下文，L684）、`concurrency`、`isolation`、`timeout`、`storeEnabled`、`txSize`、`lb`（label）、`appAttrs`。构造 `GridNearTxLocal`（L697–712）后：system tx 会**钉住已有系统事务的 topology 版本防死锁**（L714–720），再 `onCreated` 注册 + `initTimeoutHandler`（L726；`timeout>0 && !implicit` 才挂 `GridTimeoutObject`，`GridNearTxLocal.java` L4358–4369）。
- **注册表**（字段区）：`threadMap`（线程→tx，L231）、`sysThreadMap`（`TxThreadKey`=线程+cacheId，系统事务隔离，L234/L3218）、`idMap`（dht tx 按 xid 版本，L237）、`nearIdMap`（near tx，L240）、`completedVersSorted/HashMap`（完成历史，上限默认 2^18，L169/L249/L254）、`mappedVers`（near→dht 版本映射，L275）、`deadlockDetectFuts`（L243）。
- **状态机**：`IgniteTxAdapter#state(TransactionState)` 的合法迁移表（`transactions/IgniteTxAdapter.java` L1110–1187）：

| 目标态 | 合法来源态 | 触发点（本文各节） |
|---|---|---|
| `SUSPENDED` | ACTIVE | 用户 `suspend()`（§4.3） |
| `PREPARING` | ACTIVE | prepare future `prepare()`（§2.2/§3.1） |
| `PREPARED` | PREPARING | prepare future `onDone`（§2.2 L433–435、§3.1 L291–296）；one-phase 由 `chainState(PREPARED)`（§3.2 L1354） |
| `MARKED_ROLLBACK` | ACTIVE/PREPARING/PREPARED/SUSPENDED | `setRollbackOnly`（超时、filter 失败、prepare 失败） |
| `COMMITTING` | PREPARED | finish future `localFinish(true)`（§4.2） |
| `COMMITTED` | COMMITTING | finish future onDone |
| `ROLLING_BACK` | ACTIVE/MARKED_ROLLBACK/PREPARING/PREPARED/SUSPENDED/（COMMITTING 且本地非 dht） | finish future `localFinish(false)`（L1181–1184） |
| `ROLLED_BACK` | ROLLING_BACK | finish future onDone |
| `UNKNOWN` | ROLLING_BACK/COMMITTING | 写阶段失败收尸（L1148–1154） |
| `ACTIVE` | SUSPENDED | 用户 `resume()` |

  **进入 `PREPARED`/`COMMITTED`/`ROLLED_BACK` 时写 WAL `TxRecord`**（L1213–1214 → `logTxRecord`）——这是持久化重启后恢复的依据，`collectTxStates`（`IgniteTxManager` L3586–3591）在 WAL 回放时维护 `uncommitedTx` 集合。
- **commit/rollback 入口链**：`TransactionProxyImpl#commit`（`transactions/TransactionProxyImpl.java` L319）→ `GridCacheSharedContext#commitTxAsync`（`cache/GridCacheSharedContext.java` L1073，先等 last async 操作）→ `GridNearTxLocal#commitNearTxLocalAsync`（L3442：`prepareNearTxLocal` + `NearTxFinishFuture`）→ `finish(true)` → `localFinish(true)`（L3162–3242：`state(COMMITTING)` → `userCommit()` 写阶段 → 终态）。回滚对称：`rollbackNearTxLocalAsync`（L3521/L3533）→ `finish(false,...)` → `localFinish(false)` → `userRollback`。
- **TM 收尾三件套**：`commitTx`（L1519–1618，11 步：校验已记 committed 版本 L1532–1546、`processCompletedEntries`、`collectPendingVersions`（dht tx）、`unlockMultiple` 读写锁、`removeObsolete`、清线程表、映射、metrics）；`rollbackTx`（L1627–1684，先 `addRolledbackTx` 记历史再解锁）；`fastFinishTx`（L1693–1734，无锁只读事务快速路径，`fastFinish()` 条件在 `GridNearTxLocal` L3690–3692：写集空且（乐观非 SER 或读集空））。
- **prepare 的分派点** `prepareTx`（L1140–1166）：**pessimistic 本地事务直接 return（锁早在 enlist 拿了，L1155–1156）**；optimistic/远端才 `lockMultiple`（L1161）——`lockMultiple`（L1893–1975）逐条目 `entry.tmLock`（L1931），SERIALIZABLE 携带 `serOrder=nearXidVersion`（L1904），失败即回滚已锁条目并抛 `IgniteTxOptimisticCheckedException`。

### 1.3 MVCC 真身：`GridCacheMvccManager` ≠ candidate 队列

**candidate 队列在 entry 上，不在 manager 上**：

- `GridCacheMvcc`（`cache/GridCacheMvcc.java`，L50）挂在每个 `GridDistributedCacheEntry` 里，持两个 `LinkedList<GridCacheMvccCandidate>`：本地的 `locs`（L82）与远端的 `rmts`（L86）。API 实名（**没有 `addCandidate`/`removeCandidate` 这两个名字**）：`addLocal`（L583/L624，本地候选入队+重排）、`addRemote`（L707）、`addNearLocal`（L751）、`readyLocal`（L797，锁 ready）、`doneRemote`（L921，远端事务完成释放）、`reassign`（L1015，owner 选举重排）、`releaseLocal`（L1190）、`remove`（L1235）。
- `GridCacheMvccManager`（`cache/GridCacheMvccManager.java`，L83）是**共享管理器**，管的是 future 注册表与全局视图：`futs`（按 `IgniteUuid`，L126）+ `verFuts`（按版本，L117）+ `addFuture`/`removeVersionedFuture`（L577/L692）；`locked`/`nearLocked` 条目注册表（L109/L113，即"lockedEntries"真身，`lockedKeys()` L814）；显式锁跨度 `pendingExplicit` + `addExplicitLock/removeExplicitLock`（L101/L912/L940）；`pending` 线程本地候选队列 + `recheckPendingLocks`（L98/L1218，near 锁排队重查）；`near2dht` 版本映射（L129）；exchange 协作入口 `finishLocks/finishRemoteTxs/finishExplicitLocks/finishAtomicUpdates/finishDataStreamerUpdates`（L1082/L330/L1111/L1132/L1151）。
- entry 的 obsolete 语义与 03 号数据面一致：`markObsoleteIfEmpty` 由 `removeObsolete`（`IgniteTxManager` L1171–1213）在事务收尾时触发，条目空则从 cache map 移除。

### 1.4 死锁检测（`TxDeadlock*` 一族存在，算法=分布式 wait-for-graph）

- **触发**：只在**超时**时——`GridNearLockFuture`（L1429）、`GridDhtColocatedLockFuture`（L1497）与 `GridNearOptimisticTxPrepareFuture#onTimeout`（L718–781，detect 调用 L772，结果包成 `TransactionDeadlockException` L758–760）。开关：系统属性 `IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS`（默认 1000，设 0 关闭；`IgniteTxManager` L175/L221–222/L2395–2397）。
- **算法**（`transactions/TxDeadlockDetection.java`）：`TxDeadlockFuture`（L162）维护 wait-for 图 `wfg`（L200）与 `txLockedKeys/txRequestedKeys`（L194–197）；`map`（L278–298）把 key 映到 primary 或 near 持有者节点、按 `nodesQueue` 轮询发 `TxLocksRequest`（迭代上限 `deadlockMaxIters`，L287）；对端经 `IgniteTxManager#txLocksInfo`（L2418–2448，`TOPIC_TX` 直发）应答 `TxLocksResponse`，本地侧从活动 tx + 各 lock/prepare future 的 `requestedKeys` 收集（L2468–2514）；`detect`（L303–319）merge → `updateWaitForGraph`（L461）→ `findCycle`，成环即 `onDone(new TxDeadlock(...))`（L313–315）。监听器 `DeadlockDetectionListener` 注册在 `TOPIC_TX`（`IgniteTxManager` L3338/L372）。

---

## 2. 悲观路径全链

### 2.0 显式 `PESSIMISTIC` 事务一次 commit 的三端时序（总览）

| # | 发起端（GridNearTxLocal） | primary 端（GridDhtTxLocal） | backup 端（GridDhtTxRemote） |
|---|---|---|---|
| 1 | `txStart`：`newTx(implicit=false)` + `threadMap` 注册 | — | — |
| 2 | 每次 put：`enlistWrite` 登记 `IgniteTxEntry`（near/colocated 条目） | — | — |
| 3 | 每次 put：`txLockAsync` → lock future → `GridNearLockRequest`（per primary，多 key 可多路并行） | `processNearLockRequest`：`tmLock` 加 candidate；需要时向 backup 发 `GridDhtLockRequest` | 收 dht 锁请求，登记远端 candidate |
| 4 | 锁全部到手（`postLockWrite` 取旧值/执行 entryProcessor） | 回 `GridNearLockResponse` | — |
| 5 | `commit()` → `prepareNearTxLocal` → `GridNearPessimisticTxPrepareFuture`（此时才建 prepare future） | — | — |
| 6 | 逐 primary 发 `GridNearTxPrepareRequest`（映射早在 3 就绪） | `prepareNearTx` 建 `GridDhtTxLocal` → `prepareAsync`：pessimistic 的 `prepareTx` 空转，锁已在 3 拿到 → `sendPrepareRequests` 发 `GridDhtTxPrepareRequest` | `startRemoteTx` 建 `GridDhtTxRemote`，预写但未提交 |
| 7 | prepare 响应齐 → `state(PREPARED)`；finish future 启动 | 回 `GridNearTxPrepareResponse`（dhtVer/writeVer） | — |
| 8 | `localFinish(true)`：`userCommit` 写本地条目 + WAL | — | — |
| 9 | 逐 primary 发 `GridNearTxFinishRequest(commit=true)` | `finish` → `commitAsyncLocal`（primary 本地写 + WAL + 解锁）→ 向 backup 发 `GridDhtTxFinishRequest(commit=true)` | `finish()`：`commitRemoteTx` 落盘 + 解锁；回 finish 响应 |
| 10 | finish 响应齐 → `state(COMMITTED)`，事务从 `threadMap` 摘除 | `tm().commitTx` 收尾（completed versions、obsolete） | 同左 |

要点：**步骤 3（锁）与步骤 6（prepare）分离**是悲观路径的辨识特征；回滚时步骤 9 的消息 `commit=false`，其余不动。

### 2.1 锁获取发生在 enlist（每次 cache 操作），不在 prepare

以 put 为例（`near/GridNearTxLocal.java`）：`putAsync0`（L570）→ `enlistWrite`（L890，near/colocated 条目登记 `IgniteTxEntry`，缺值时 `loadMissing` 读 store）→ **pessimistic 分支（L629–688）**：`cacheCtx.cache().txLockAsync(keys, timeout, this, ...)`（L635）。分派链（与显式锁 API 同一条）：

```
GridDistributedCacheAdapter#txLockAsync (distributed/GridDistributedCacheAdapter.java L102–114)
  → lockAllAsync(...)
     ├─ 无 near（默认）: GridDhtColocatedCache#lockAllAsync (dht/colocated/GridDhtColocatedCache.java L633–660)
     │    → new GridDhtColocatedLockFuture —— 注释原文 "This is an entry point to pessimistic locking within transaction"
     └─ 有 near: GridNearTransactionalCache#lockAllAsync (near/GridNearTransactionalCache.java L287–302)
          → new GridNearLockFuture
```

**锁消息族**（存在，且 tx 与显式锁共用）：发起端→primary `GridNearLockRequest`（`GridNearLockFuture` L1052 创建；primary 端处理 `GridDhtTransactionalCacheAdapter#processNearLockRequest`，`dht/GridDhtTransactionalCacheAdapter.java` L136–137/L178）；primary→backup `GridDhtLockRequest`（同文件 L139–140/L375）；响应 `GridNearLockResponse`/`GridDhtLockResponse`。显式解锁：`GridNearUnlockRequest`（`GridDhtColocatedCache` L738/L842、`GridNearTransactionalCache` L407 发送）与 primary→backup `GridDhtUnlockRequest`（`GridDhtTransactionalCacheAdapter` L1679/L1714）。**tx 提交/回滚的解锁不走这对消息**——走 finish 请求（§2.3）由 `unlockMultiple` 完成。

### 2.2 悲观 prepare：`GridNearPessimisticTxPrepareFuture`（`near/`，L61）

- `prepare()`（L180–208）：`state(PREPARING)` → `userPrepare(emptyList)`（L198；因 pessimistic 本地，`IgniteTxManager#prepareTx` 空转，§1.2）→ `preparePessimistic()`。
- `preparePessimistic`（L288–418）：对 `tx.allEntries()` 逐条目按 `top.nodes(affinity.partition(key), topVer)` 定位、primary=`nodes.get(0)`（L307–331），累计 `GridDhtTxMapping` → `tx.transactionNodes`（L334–336）；**`checkOnePhase` 仅当 `!hasNearCache`**（L338–339）；然后**并行**对每个 primary 建 `MiniFuture`：本地 → `prepareLocal` → `txHandler().prepareNearTxLocal/prepareColocatedTx`（L268–270），远端 → `sendTransactionMessage(primary, req, tx, plc)`（L392）。
- `onDone`（L426–445）：除 one-phase 且本地无映射的例外，置 `state(PREPARED)`（L433–435）。锁早在 enlist 拿到手，所以 prepare 请求只是"把映射与写集送上去建 `GridDhtTxLocal` + 备份预写"。

### 2.3 prepare/commit/rollback 消息对全表（方向、关键载荷、处理点）

| 消息（方向） | 关键字段（声明行） | 发送/处理点 |
|---|---|---|
| `GridNearTxPrepareRequest`（发起端→primary） | 基类 `GridDistributedTxPrepareRequest`：threadId/concurrency/isolation/timeout/reads/writes/`transactionNodes`（L77–147）；near 特有 futId/miniId/topVer + 位标志 NEAR/FIRST_CLIENT_REQ/IMPLICIT_SINGLE/EXPLICIT_LOCK/ALLOW_WAIT_TOP_FUT/RECOVERY（`near/GridNearTxPrepareRequest.java` L42–79） | 悲观 L379–392、乐观 L517–580 构造发送；primary 端 `processNearTxPrepareRequest`（`transactions/IgniteTxHandler.java` L144，注册 L205–238） |
| `GridNearTxPrepareResponse`（primary→发起端） | `dhtVer`/`writeVer`（L61–65）、`ownedVals`（L69）、`retVal`（L81）、`filterFailedKeys`（L85）、**`clientRemapVer`**（L89）、`onePhaseCommit`（L93）、`pending`（L49） | 发起端 `onPrepareResponse`（`near/GridNearTxPrepareFutureAdapter.java` L199–288：ownedValues 刷 near 条目 L221–238、版本对齐 `onReceived` L268、`readyNearLocks` L284） |
| `GridDhtTxPrepareRequest`（primary→backup / 纯 near reader） | `nearWrites`（L71）、`owned`（L75）、`updCntrs`（L87）、`nearXidVer`（L91）、`invalidateNearEntries`（L66）、`preloadKeys`（L99） | primary 构造（`dht/GridDhtTxPrepareFuture.java` L1418–1434）；backup/reader 端 `processDhtTxPrepareRequest`（`IgniteTxHandler` L1179：`startNearRemoteTx` L1207 建 `GridNearTxRemote`、`startRemoteTx` L1208 建 `GridDhtTxRemote`） |
| `GridDhtTxPrepareResponse`（backup→primary） | `nearEvicted` 等 | L819 处理；nearEvicted 回报链见 11 号 §3.2 第 7 步 |
| `GridNearTxFinishRequest`（发起端→primary） | 基类 `GridDistributedTxFinishRequest`：**`commit`（L80）**、`commitVer`（L72）、`invalidate`（L76）、`baseVer`（L84）、`syncMode`（L100）、`txState`（L103）；near 特有 miniId（L37） | `GridNearTxFinishFuture` 构造（`near/GridNearTxFinishFuture.java` L749–765：**回滚 = 同一请求 `commit=false`**）；primary 端 `finish`（`IgniteTxHandler` L929–975：rollback 先 `addRolledbackTx` L940–941，拆 colocated/near 两路 L952–958） |
| `GridNearTxFinishResponse`（primary→发起端） | — | L793 处理 |
| `GridDhtTxFinishRequest`（primary→backup/reader） | `nearNodeId`（L38）、`updCntrs`（L46）、flags：`checkCommitted`（L172–180）、`waitRemoteTxs`（L186–194）、sysInvalidate、needReturnValue | `GridDhtTxFinishFuture` 发送（`dht/GridDhtTxFinishFuture.java` L359/L452/L522）；backup 端 `processDhtTxFinishRequest`（L1348：`checkCommitted` 分支 L1354–1366 是一阶段 primary 崩溃后的确认探针；commit=false 先 `addRolledbackTx` L1369–1370 → `finish()` L1386–1398 → `tx.rollbackRemoteTx()` L1444） |
| `GridDhtTxFinishResponse`（backup→primary） | — | L853 处理 |
| `GridDhtTxOnePhaseCommitAckRequest`（backup→primary） | 版本列表 | **延迟聚合 ack**：`deferredAckMsgSnd`（`IgniteTxManager` L317–335，超时 500ms/缓冲 256，L178–184）；一阶段提交后 `GridNearTxFinishFuture#ackBackup`（L505–）触发；处理 L1329 |
| `GridCacheTxRecoveryRequest/Response`（恢复方→各 tx 节点） | `nearTxCheck` 等 | §5.1；系统事务走 `UTILITY_CACHE_POOL`（L2018） |

---

## 3. 乐观路径全链

### 3.1 future 分派与多 primary 映射

分派点：`GridNearTxLocal#prepareNearTxLocal`（L3363–3412）——`optimistic + serializable → GridNearOptimisticSerializableTxPrepareFuture`；`optimistic → GridNearOptimisticTxPrepareFuture`；`pessimistic → GridNearPessimisticTxPrepareFuture`（L3375–3381）。

乐观多 key 事务与 §2.0 的差异时序（同表口径，只列不同步）：

| # | 发起端 | primary 端 | backup 端 |
|---|---|---|---|
| 3' | （无锁步骤——enlist 只登记条目；SERIALIZABLE 读额外记 `entryReadVersion`） | — | — |
| 5' | `commit()` 即 `prepareNearTxLocal`：**映射此刻才做**（`map()` 按 affinity 定 primary，多 primary 得到映射队列） | — | — |
| 6' | `proceedPrepare` **串行**逐 primary 发 `GridNearTxPrepareRequest` | `prepareAsync` → `userPrepare` → **`lockMultiple` 在此刻拿 primary 锁**（失败/冲突即整体回滚）→ 版本分配 → `GridDhtTxPrepareRequest` 给 backup | 同 §2.0 步骤 6 |
| 6'' | （SERIALIZABLE）primary 先 `checkReadConflict`，读版本不符抛 `IgniteTxOptimisticCheckedException` 回给发起端 | | |
| 9' | prepare 全部成功才发 finish；客户端拓扑变化时 6' 可能被拒（`clientRemapVersion`）触发重映射而非失败 | | |

要点：乐观路径把"锁 + 冲突检查 + 映射"全部推迟到 prepare 一步完成，prepare 成功后 finish 与悲观完全同构——这也是 6.3/6.4 两课可以共享 finish 消息族实现的原因。

`GridNearOptimisticTxPrepareFuture`（`near/`，L79）：

- `prepare0`（L314–344）：单写走 `prepareSingle`（L351：单条目映射 + `checkOnePhase`（L387–388，同 `!isNear` 条件））；多写走 `prepare`（L400–481）：逐 write `map()`（L620–713：`top.nodes(partition, topVer)`、primary=`F.first(nodes)` L650；near 条目在拓扑锁下重取 entryExx L661–664；本地 near/colocated 用 `MappingKey` 拆开 L442–443；near key 先建 `KeyLockFuture` 占位 L666–676）→ `tx.addEntryMapping(mappings)` + `transactionNodes` 设置 → `checkOnePhase(txMapping)`（L477–478）→ `proceedPrepare`。
- `proceedPrepare`（L503–610）**串行**消费映射队列：构造 `GridNearTxPrepareRequest`（L517–535，多 primary 时只有 `m.last()` 的请求标 last）；near 条目先在本地 `tm().prepareTx` 锁住（L543–551）；本地分派 `prepareNearTxLocal/prepareColocatedTx`（L560–576），远端 `sendTransactionMessage`（L580）。
- 响应 `MiniFuture#onResult`（L965–1008）：error → 整体 fail；`clientRemapVersion != null` → remap（§3.4）；正常 → `onPrepareResponse` + 继续 `proceedPrepare` 下一个映射（L996–1005）。

### 3.2 primary 端：`GridDhtTxPrepareFuture`（`dht/`）

链路：`IgniteTxHandler#prepareNearTx`（L421）`new GridDhtTxLocal`（L551，参数含 `req.onePhaseCommit()`）→ `tx.prepareAsync(req)`（`dht/GridDhtTxLocal.java` L313–402：`addEntry` 登记读写 L372–379 → **`userPrepare(null)` L382 → `tm().prepareTx` → `lockMultiple`（乐观锁在 primary 此刻才拿）** → `fut.prepare(req)` L390）：

- `prepare`（L1075–1130）：topology 校验（L1093）、缺数据先 `forceRebalanceKeys`（L1102–1114）、`readyLocks`（L644–700，`entry.readyLock(xidVer)` L682）→ `mapIfLocked`（L707，锁齐后进 `prepare0`）。
- `prepare0`（L1278–1368）：**SERIALIZABLE 先 `checkReadConflict`（L1282–1312，详见 §3.3）**；锁在手分配写版本 `tx.writeVersion(nextVersion())`（L1318）；逐条目 `map(entry)`（L1336）→ backup 映射 + near reader 映射（`tx.dhtMap/nearMap`，细节 11 号 §3.2）；`last` 时算分区计数器、`recheckOnePhaseCommit`（L1348–1356）、`sendPrepareRequests`（L1356 → L1388–1434 构造 `GridDhtTxPrepareRequest` 给 backup/纯 reader）。
- `onDone`（L752–855）：**`tx.commitOnPrepare()`（= one-phase）分支 L764–826：先造响应（L770）→ `tx.commitAsync()`（L789）即"DHT prepare 完成 = 提交"**；失败则 `rollbackAsync`（L797/L808）。两阶段时只回 prepare 响应，提交等发起端的 finish 请求。

### 3.3 SERIALIZABLE 的特殊处理

- 发起端有**专属 prepare future** `GridNearOptimisticSerializableTxPrepareFuture`（`near/`，L69，断言 `optimistic && serializable`）：差异点是读集也参与映射/冲突（`prepare0` L273）、client 首请求的 `ClientRemapFuture` 复合（L72/L705）、`ignoreFailure`（L89——重复失败不再重试以避免多次锁定协调者）。
- 读版本采集在 enlist：`needReadVer = needVal && serializable && optimistic`（`GridNearTxLocal` L918），锁时携带 `serOrder`（§1.2 `lockMultiple` L1904）。
- 冲突判定在 primary 的 `GridDhtTxPrepareFuture#checkReadConflict`（L1209–1229）：逐条目 `entry.cached().checkSerializableReadVersion(serReadVer)`（L1218），不符即 `versionCheckError` → `IgniteTxOptimisticCheckedException`（L1235–1273，消息含 key/val 摘要）→ prepare 失败、事务回滚。**prepare 何时失败**：锁冲突（`lockMultiple` 失败）、SER 读版本冲突、topology 校验失败（L1093–1097）、超时（`onTimeout` §1.4）。

### 3.4 remap（topology 变化时 in-flight 事务的重新映射）

两层配合，全部围绕 client 首请求（`firstClientRequest`，client 节点在 topology ready 前"盲发"）：

1. **primary 端拒绝**：`prepareNearTx`（`IgniteTxHandler` L456–546）对 `firstClientRequest` 且 topology 未 ready/版本不符（`needRemap` L748）时，直接回带 `lastTopologyChangeVersion` 的 prepare 响应（L510–545），不建 tx。
2. **发起端 remap**：`MiniFuture#onResult` 见 `clientRemapVersion != null`（L982）→ 等 `exchange().affinityReadyFuture` 就绪（L986–994）→ `remap()`（L1013–1027）→ `prepareOnTopology(true, ...)`（`near/GridNearOptimisticTxPrepareFutureAdapter.java` L153–231）→ **`tx.onRemap(topVer, reset=true)`**（L172–173；`GridNearTxLocal#onRemap` L4078–4095：清 mapped/txNodes/onePhaseCommit/mappings）→ `prepare0(remap=true)`（状态允许 PREPARING/PREPARED 再入，L316）。显式锁参与的事务会把 topology 版本钉死（adapter `prepare` L103–131：`lastExplicitLockTopologyVersion` L109）。

---

## 4. 隔离 × 并发矩阵、配置默认值

### 4.1 6 组合的实现差异表（`serializable()`=isolation==SERIALIZABLE，`IgniteTxAdapter` L1042）

| 组合 | 锁时机 | prepare future | 读集合处理 | 失败方式 |
|---|---|---|---|---|
| PESSIMISTIC × RC | enlist 即锁（`txLockAsync`） | `GridNearPessimisticTxPrepareFuture`（`prepareTx` 空转 L1155–1156） | 读不加锁；每次读新值 | 锁等待超时（可触发死锁检测） |
| PESSIMISTIC × RR | 同上 | 同上 | 读不加锁；重复读靠 tx 条目缓存（`IgniteTxStateImpl` readView） | 同上 |
| PESSIMISTIC × SER | 同上 | 同上 | 锁请求携带 `serOrder`（`lockMultiple` L1904），primary 侧串行化排序 | 同上 |
| OPTIMISTIC × RC | primary prepare 时 `lockMultiple` | `GridNearOptimisticTxPrepareFuture`（03 号 §5 主链；one-phase 可用） | 无读版本记录 | 锁冲突/prepare 失败整体回滚（`IgniteTxOptimisticCheckedException`） |
| OPTIMISTIC × RR | 同上 | 同上 | 写冲突经 completedVersions 排序判定（`versions()` L1220） | 同上 |
| OPTIMISTIC × SER | 同上 | **专属 future**（§3.3） | `needReadVer` 逐 key 记读版本（L918）+ `checkReadConflict`（L1209）；`fastFinish` 被排除（L3691） | 读版本冲突在 prepare 即失败 |

**`TransactionConfiguration` 默认值**（`configuration/TransactionConfiguration.java`）：`PESSIMISTIC + REPEATABLE_READ`（L47–50）、`dfltTxTimeout=0` 即无限（L53）、`txTimeoutOnPartitionMapExchange=0` 即不强制回滚（L56）。两个遗留开关：`txSerEnabled`（L44/L65）runtime 不 gate SERIALIZABLE future 选择、只在节点加入校验比对（`cache/ValidationOnNodeJoinUtils.java` L545–549）；`pessimisticTxLogLinger`（L62/L88）在 core runtime **零使用**（仅配置拷贝到平台层）。

### 4.2 rollback 全路径（本地 + 远端）

用户 `tx.rollback()`（`TransactionProxyImpl` L405）→ `rollbackNearTxLocalAsync`（`GridNearTxLocal` L3521/L3533–3596：超时 handler 先摘 L3543–3544；未完成 prepare 触发 `onNearTxLocalTimeout` L3548–3549）→ `NearTxFinishFuture.finish(false,...)` → **安全回滚**：若锁 future 还在飞，先 cancel/等它（`GridNearTxFinishFuture#rollbackAsyncSafe` L415–443）→ `doFinish(false)`（L451–495）→ `localFinish(false)`（`state(ROLLING_BACK)` → `userRollback`（`IgniteTxLocalAdapter` L1022–1056：evict near 条目 → `tm().rollbackTx` → store `sessionEnd(stores,false)` L1052））→ 逐 primary 发 `GridNearTxFinishRequest(commit=false)`（L749–787）→ primary `finish`（`IgniteTxHandler` L929：`addRolledbackTx` L941 → `finishColocatedLocal`/`finishDhtLocal`）→ primary 的 `GridDhtTxFinishFuture` 向 backup 发 `GridDhtTxFinishRequest(commit=false)`（L452）→ backup `tx.rollbackRemoteTx()`（L1444）。另外三个批量回滚入口：节点 stop（`onKernalStop0`）、client 断连（`onDisconnected` L511–522，配合 `IgniteNeedReconnectException`，见 11 号）、PME 超时（§5.2）。

### 4.3 suspend/resume（存在，`Transaction` API 的 `suspend()/resume()`）

`GridNearTxLocal#suspend`（L2980–2991，仅发起线程可挂起，`tm().suspendTx` L2641）/ `resume`（L2999–3068，可在**另一线程**恢复，状态走 `SUSPENDED↔ACTIVE`，迁移表 L1123–1134）。

---

## 5. 恢复与周边

### 5.1 节点崩溃：tx recovery 消息族与共识

- **感知**：`TransactionRecoveryListener`（`IgniteTxManager` L3563–3579，`DiscoveryEventListener` + `HighPriorityListener`）在节点 leave 事件上起 `TxRecoveryInitRunnable`（L3098–3213）：扫活动 tx——originator 是死节点且带 write-through store 的直接 `salvageTx`（L3139–3144，RECOVERY_FINISH）；**originator 死但本地（primary/backup）已 PREPARED 的走 `processPrepared` → `commitIfPrepared`**（L3187–3194）。
- **共识**：`commitIfPrepared`（L2367–2390）建 `GridCacheTxRecoveryFuture`（`distributed/GridCacheTxRecoveryFuture.java` L49）：若发起节点还活着，先问它"tx 是否已提交"（`nearTxCheck` L129，`txCommitted` L2117）；否则向 `tx.transactionNodes()` 全体发 `GridCacheTxRecoveryRequest`（L156/L274/L312）；对端 `processCheckPreparedTxRequest`（`IgniteTxHandler` L1961–2001）查 `txsPreparedOrCommitted`（L2196–2318：等 prepare future 完成、全部 PREPARED/COMMITTED 才 true，未 prepared 即本地回滚 L2244–2256）。
- **执行**：多数 true → `finishTxOnRecovery(tx, true)`（L2326–2358：`tx.commitAsync()` + `CommitListener` L2343）；false → rollbackAsync（L2356）。一阶段提交的主备失联场景：发起端 `GridNearTxFinishFuture#checkBackup`（L389–399，`tx.onNeedCheckBackup()`）用 `GridDhtTxFinishRequest(checkCommitted=true)` 向 backup 探测（L1354–1366）。

### 5.2 tx × exchange 完整交互

- **等什么**：`GridCacheSharedContext#partitionReleaseFuture`（L938–953）聚合：显式锁（`finishExplicitLocks`）、atomic 更新、DataStreamer、**`finishLocalTxs`**（`IgniteTxManager` L807–830，语义：等所有 topology 版本更旧的事务结束——`needWaitTransaction` = `txTopVer < topVer`，L898–902）与 **`finishAllTxs`**（L881–890：本地事务收尾后，再等 primary→backup 的远端更新 `finishRemoteTxs`）。baseline 节点故障走 `partitionRecoveryFuture`（L966–968 → `recoverLocalTxs` L839–868）。
- **怎么等（两阶段）**：`GridDhtPartitionsExchangeFuture` L1633–1648——第一阶段 `waitPartitionRelease(EXCHANGE_LATCH_ID, distributed=true, doRollback=true)` 用 `ExchangeLatchManager` 的分布式 latch（L1841–1842；countDown L1981、await L2003，机制详见 10 号）；第二阶段只做本地等待（L1647）。exchange-free 开关的跳过条件 L1630–1635。
- **何时回滚**：`txTimeoutOnPartitionMapExchange > 0` 且等待超时 → `tm().rollbackOnTopologyChange`（L1893–1897；实现 L481–490，逐个 `rollbackNearTxLocalAsync(false,false)`）。该配置值经 discovery custom event 全网传播（`setTxTimeoutOnPartitionMapExchange` L2733–2746 / `onTxTimeoutOnPartitionMapExchangeChange` L2753–2769）。write-behind flush 的 exchange 调用点：分区释放完成后 `cacheCtx.store().forceFlush()`（`GridDhtPartitionsExchangeFuture` L1666，见 10 号）。

### 5.3 CacheStore two-phase 集成（store 会话与协调点）

- **写阶段前置**：`userCommit`（`IgniteTxLocalAdapter` L522）先 `addCommittedTx`（L545）再 **`batchStoreCommit`**（L548 → `IgniteTxAdapter` L1388–1591：注释明确 "must be called before cache update... if there is a DB failure, cache transaction can still be rolled back"；write-through 判定 L1530，跨 cache 聚批 putAll/removeAll L1541–1555，**持锁状态下** `sessionEnd(stores, true)` L1559；失败则 `removeCommittedTx` 回收 L1567）。远端（write-from-dht）在 `GridDistributedTxRemoteAdapter` L500 调 `batchStoreCommit`。
- **session 协调点**：`IgniteTxAdapter#sessionEnd`（L1370–1380）逐 store 调 `CacheStoreManager#sessionEnd(tx, commit, last, ...)`（`store/CacheStoreManager.java` L175；实现 `GridCacheStoreManagerAdapter` L784–797：触发 `CacheStoreSessionListener` L792 与 `store.sessionEnd(commit)` L797）。回滚路径 `userRollback` → `sessionEnd(stores,false)`（`IgniteTxLocalAdapter` L1042–1053）。**store 与 one-phase 互斥**：`storeWriteThrough()` = `storeEnabled && txState().storeWriteThrough`（`IgniteTxAdapter` L503–504），置位即禁 one-phase（§4 的条件表）。

### 5.4 write-behind 模式（`store/GridCacheWriteBehindStore.java`）

- **flusher 线程**：N 个 `Flusher`（`GridWorker`，L951–1000；`start` 建 `flusher-N` 线程 L1000–1003；N 默认 1——`CacheConfiguration.DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT` L174）。key 按 hash 路由到固定 flusher（`resolveFlusherByKeyHash` L673–689），保证同 key 顺序。
- **coalescing**：开（默认）时每 flusher 一张 `flusherWriteMap`（按 key 合并旧值，L956 区）；关时 queue + map 双结构（`putToFlusherWriteCache` L1007–1058，队列超 `flusherCacheCriticalSize` 会**阻塞写线程背压**）。
- **参数**（`configuration/CacheConfiguration.java` L165–177）：`writeBehindFlushSize` 10240、critical 16384、`writeBehindFlushFrequency` 5000ms、`writeBehindBatchSize` 512（`GridCacheWriteBehindStore` L109）。flush 时机 = size 阈值或定时（`awaitOperationsAvailable*` L1105–1127）。`forceFlush()`（L409）由 exchange 调（§5.2）。

### 5.5 utility cache 在 tx 子系统的真实用途

**事务子系统为 utility/系统 cache 提供专门通道，而非把 tx 状态存进 utility cache**：

- **系统事务机制**：`newTx` 的 `sysCacheCtx` 参数（L684）→ `tx.system=true` + `ioPolicy=UTILITY_CACHE_POOL`（L701–702）；系统 tx 注册进 `sysThreadMap`（按 `TxThreadKey` 线程+cacheId，L234）与用户线程表隔离；**拓扑版本钉死**防系统事务互锁（L714–720）；恢复消息对 system tx 也走 `UTILITY_CACHE_POOL`（`IgniteTxHandler` L2018）。
- **谁消费**：入口 `IgniteTransactionsImpl#txStartEx(ctx, ...)`——`ctx.systemTx()` 时把 cache 上下文传入（`transactions/IgniteTransactionsImpl.java` L121–158）。实际客户：**数据结构处理器**在 `ignite-sys-atomic-cache@group`（TRANSACTIONAL 模式，`datastructures/DataStructuresProcessor.java` L146/L1080–1097）上以 `txStartEx(PESSIMISTIC, REPEATABLE_READ)` 跑系统事务（L739/L841）；security 开启时任务元数据 cache（`GridTaskProcessor` L217）。utility cache 访问器 `GridCacheProcessor#utilityCache`（L4460–4461）。
- **tx mapping 存哪、谁消费（正面回答）**：映射只存在于内存（`GridNearTxLocal.mappings()`（L2921，`IgniteTxMappings`/`IgniteTxMappingsSingleImpl`）与 `GridDhtTxMapping`）以及 `GridNearTxPrepareRequest.transactionNodes` 消息字段（§2.3），由 primary（建 `GridDhtTxLocal`）与 recovery future（`GridCacheTxRecoveryFuture` 遍历 txNodes）消费；**崩溃恢复依赖 WAL `TxRecord`**（§1.2），不依赖任何 cache。

---

## 6. 分级结论：章 6 切课建议（8 课）

### 6.1 切课建议表（每课一个概念簇；依赖按课号递增）

"验收测试来源"列指可改写的 vendor 测试（相对 `vendors/ignite/modules/core/src/test/java/org/apache/ignite/internal/processors/cache/`，均已核验存在；Apache 2.0 attribution 按 ROADMAP §5.4）：

| 课 | 概念簇 | Tracer bullet | 验收测试来源 | 依赖 |
|---|---|---|---|---|
| 6.1 | **隐式微事务与一阶段提交**★约束①：`newTx(implicit)`、`GridNearTxLocal` 登记、`GridNearOptimisticTxPrepareFuture.prepareSingle`、`checkOnePhase` 三条件、`commitOnPrepare` | TRANSACTIONAL cache 单 key put 跨 2 节点提交成功 | `distributed/dht/GridCacheColocatedTxSingleThreadedSelfTest.java` | 章 3 消息、章 4 affinity（03 号 §5 已铺全链） |
| 6.2 | **锁系统**：`GridCacheMvcc` candidate 队列（locs/rmts、ready/reassign）、`txLockAsync`→`GridDhtColocatedLockFuture`、`GridNear/DhtLockRequest` 消息族、显式锁 API、死锁检测（wait-for-graph） | 显式 `cache.lockAll` 跨节点死锁被检测并抛 `TransactionDeadlockException` | `transactions/TxPessimisticDeadlockDetectionTest.java`、`transactions/AbstractDeadlockDetectionTest.java` | 6.1 |
| 6.3 | **悲观 2PC**：`IgniteTransactionsImpl/TransactionProxyImpl`、`GridNearPessimisticTxPrepareFuture`、`GridDhtTxLocal`+`GridDhtTxPrepareFuture`、finish 消息对全表、**rollback 全路径**、`GridDhtTxRemote` | 多 primary PESSIMISTIC 事务提交 + 中途 rollback 两节点状态一致 | `distributed/dht/GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest.java`（rollback 一致性部分） | 6.2 |
| 6.4 | **乐观事务与隔离矩阵**：prepare 时 `lockMultiple`、版本冲突、RC/RR 差异、timeout handler、`fastFinishTx`、suspend/resume | OPTIMISTIC 事务提交 + 写冲突触发回滚 | `CacheOptimisticTransactionsWithFilterTest.java`、`distributed/IgniteAbstractTxSuspendResumeTest.java` | 6.3 |
| 6.5 | **SERIALIZABLE 与拓扑×事务**：专属 future、`checkReadConflict`、remap（clientRemapVersion/onRemap）、`partitionReleaseFuture`/latch 两阶段等待、`txTimeoutOnPartitionMapExchange` | SERIALIZABLE 冲突检测；kill 一节点触发在飞事务 remap | `CacheSerializableTransactionsTest.java`（选子集）、`transactions/TxOptimisticPrepareOnUnstableTopologyTest.java` | 6.4、章 4 exchange（10 号） |
| 6.6 | **崩溃恢复**：`TransactionRecoveryListener`/`TxRecoveryInitRunnable`、`GridCacheTxRecoveryRequest/Response` 共识、`commitIfPrepared`、一阶段 `checkCommitted` 探针 | 发起节点在 prepare 后被 kill，集群共识后正确提交/回滚 | `transactions/TxRecoveryOnCoordniatorFailTest.java`（注意 vendor 文件名原拼写即 "Coordniator"）、`transactions/TxRecoveryConcurrentTest.java` | 6.3、6.5 |
| 6.7 | **CacheStore × tx**★约束②：`batchStoreCommit` 前置、`sessionEnd`/`CacheStoreSessionListener`、store×one-phase 互斥、write-behind flusher/coalescing/参数 | two-phase store 会话回调按序触发；write-behind 断电窗口可控 | `CacheStoreTxPutAllMultiNodeTest.java` | 6.3（不依赖 6.4–6.6） |
| 6.8 | **事务 × near 收官**★约束④：near 条目入 `IgniteTxEntry`、`nearWrites`/reader 微事务（`GridNearTxRemote`）、prepare 响应 `ownedValues` 刷 near、`recheckOnePhaseCommit` 禁一阶段、**utility cache 系统事务**★约束③（`sysCacheCtx`/`sysThreadMap`，数据结构客户） | near 客户端事务正确失效回源 + 数据结构在系统事务下创建 | `distributed/CacheTxNearUpdateTopologyChangeTest.java` | 6.3、章 5（11 号 §3.2 已铺） |

课时按 2–4h 估：6.2/6.3/6.8 偏 4h，6.4/6.5/6.6/6.7 偏 2–3h。★约束③落在 6.8 是因为"系统事务"概念簇很小（约 2h 的一半），单独成课违反"宁多切课不双簇"的反面（过碎）；若实施时发现 6.8 超预算，优先把系统事务拆成 6.8b 小课而不是塞进 6.6。

### 6.2 可简化项判断（默认全做，仅列真正可争议项）

1. **savepoint**：**不存在于 2.18**（§0.3），无需争论，直接不做；JDBC 接口保留抛 `SQLFeatureNotSupportedException` 的 stub。
2. **SERIALIZABLE 专属 future**：可争议。`GridNearOptimisticSerializableTxPrepareFuture` 与普通 optimistic future 共享 90% 映射逻辑（差异：读集映射、`ClientRemapFuture`、`ignoreFailure`）。可先在普通 future 内加 `if (serializable)` 分支，行为面验收后再拆类。**默认拆类**（保真优先）。
3. **recovery 边角**：共识主线（`GridCacheTxRecoveryFuture` 多数查询）必做；`GridDhtTxOnePhaseCommitAckRequest` 的**延迟聚合 ack**（500ms/256 缓冲）可先实现为逐条直发（`IgniteTxManager` L317–335 只是批量优化），行为不变。
4. **死锁检测的迭代上限/超时参数面**：`deadlockMaxIters`/`deadLockTimeout` 可只留 maxIters 一个开关；wait-for-graph 算法本体不可省（`TransactionDeadlockException` 是公开 API 语义）。
5. **`pessimisticTxLogLinger`/`txSerEnabled`**：core runtime 零使用/仅 join 校验（§4.1）——配置字段保留以对齐 API，行为 stub。
6. **suspend/resume**：状态机两条迁移 + 线程表搬移，量小，默认做。

---

## 引用文件清单

以下路径均相对 `vendors/ignite/modules/core/src/main/java/`：

**管理器与事务基座**
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxManager.java`（`newTx` L681、`onCreated` L736、`finishLocalTxs` L807、`recoverLocalTxs` L839、`finishAllTxs` L881、`needWaitTransaction` L898、`prepareTx` L1140、`removeObsolete` L1171、`commitTx` L1519、`rollbackTx` L1627、`fastFinishTx` L1693、`uncommitTx` L1741、`onDisconnected` L511、`rollbackOnTopologyChange` L481、`salvageTx` L587、`lockMultiple` L1893、`txUnlock` L1981、`unlockMultiple` L2013、`txsPreparedOrCommitted` L2196、`finishTxOnRecovery` L2326、`commitIfPrepared` L2367、`detectDeadlock` L2406、`txLocksInfo` L2418/L2468、`suspendTx/resumeTx` L2641/L2664、`setTxTimeoutOnPartitionMapExchange` L2733、`TxRecoveryInitRunnable` L3098、`DeadlockDetectionListener` L3338、`TransactionRecoveryListener` L3563、`collectTxStates` L3586、`ensureTransactionModeSupported` L3610、字段区 L221–286、`start0` L316/L370/L372）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxAdapter.java`（`state` 迁移表 L1110–1214、`serializable/optimistic/pessimistic` L1032–1044、`storeWriteThrough` L503、`sessionEnd` L1370、`batchStoreCommit` L1388、store 批量+sessionEnd L1541–1590）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxLocalAdapter.java`（`userCommit` L522、`userPrepare` L387、`userRollback` L1022、`tmFinish` L966、one-phase 不解锁 L909–921）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxLocalEx.java`（`localFinish` 接口 L58）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxStateImpl.java`（`txMap`/视图 L57–71）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxImplicitSingleStateImpl.java`（L50–55）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTxRemoteStateImpl.java`（L43–47）
- `org/apache/ignite/internal/processors/cache/transactions/IgniteTransactionsImpl.java`（`txStart0` L170、`txStartEx` L121/L137）
- `org/apache/ignite/internal/processors/cache/transactions/TransactionProxyImpl.java`（`commit` L319、`rollback` L405）
- `org/apache/ignite/internal/processors/cache/transactions/TransactionChanges.java`（SQL 事务感知查询载体，L29–31）
- `org/apache/ignite/internal/processors/cache/transactions/TxDeadlockDetection.java`（类 L53、`TxDeadlockFuture` L162、`map` L278、`detect` L303、`updateWaitForGraph` L461）
- `org/apache/ignite/internal/processors/cache/transactions/TxDeadlock.java`、`TxLock.java`、`TxLockList.java`、`TxLocksRequest.java`、`TxLocksResponse.java`（仅经 `TxDeadlockDetection`/`IgniteTxManager` 内引用间接核验存在，未单开）

**near 侧**
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxLocal.java`（类 L131、`putAsync0` L570（悲观 `txLockAsync` L635）、`enlistWrite` L890、`mappings` L2921、`suspend/resume` L2980/L2999、`localFinish` L3162、`prepareNearTxLocal` L3363（future 分派 L3375–3381）、`commitNearTxLocalAsync` L3442、`rollbackNearTxLocalAsync` L3521/L3533、`finishFuture` L3606、`fastFinish` L3690、`commitAsyncLocal/rollbackAsyncLocal` L3754/L3830、`lockAllAsync` L3884、`onRemap` L4078、`initTimeoutHandler` L4358）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxRemote.java`（类 L49）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearPessimisticTxPrepareFuture.java`（类 L61、`prepare` L180、`createRequest` L218、`prepareLocal` L258、`preparePessimistic` L288、`onDone` L426）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearOptimisticTxPrepareFuture.java`（类 L79、`prepare0` L314、`prepareSingle` L351、`prepare` L400、`proceedPrepare` L503、`map` L620、`onTimeout` L718、MiniFuture `onResult` L965、`remap` L1013）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearOptimisticTxPrepareFutureAdapter.java`（`prepare` L103、`prepareOnTopology` L153、`topologyReadLock` L138）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearOptimisticSerializableTxPrepareFuture.java`（类 L69、`remapFut` L72、`ignoreFailure` L89、`prepare0` L273、`ClientRemapFuture` L705）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxPrepareFutureAdapter.java`（`checkOnePhase` L168–192、`onPrepareResponse` L199）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxPrepareRequest.java`（标志位 L42–79）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxPrepareResponse.java`（字段 L49–93）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxFinishFuture.java`（`finish` L383、`rollbackAsyncSafe` L415、`doFinish` L451、`ackBackup` L505、请求构造 L749–765）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTxFinishRequest.java`（miniId L37）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearLockFuture.java`（`new GridNearLockRequest` L1052、`mapOnTopology(remap)` L841、死锁检测 L1429）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearLockResponse.java`（remap 字段 L60–141）
- `org/apache/ignite/internal/processors/cache/distributed/near/GridNearTransactionalCache.java`（`lockAllAsync` L287、`processLockResponse` L275、unlock 发送 L407）
- `org/apache/ignite/internal/processors/cache/distributed/near/IgniteTxMappings.java`

**dht 侧**
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxLocal.java`（类 L67、`prepareAsync` L313（future 创建 L323、`userPrepare` L382）、`finishTx` L409）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxLocalAdapter.java`（dht 本地公共底座）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxRemote.java`（类 L52、`storeWriteThrough` L60）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxPrepareFuture.java`（`readyLocks` L644、`mapIfLocked` L707、`onDone`（commitOnPrepare 分支）L752–826、`prepare` L1075、`checkReadConflict` L1209、`versionCheckError` L1235、`prepare0` L1278、`recheckOnePhaseCommit` L1373、`sendPrepareRequests` L1388（请求构造 L1418–1434））
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxPrepareRequest.java`（字段 L50–111）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxFinishRequest.java`（字段 L36–46、flags L159–208）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTxFinishFuture.java`（`GridDhtTxFinishRequest` 发送点 L359/L452/L522）
- `org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtTransactionalCacheAdapter.java`（锁消息 handler 注册 L136–148、`processNearLockRequest` L178、`processDhtLockRequest` L375、`GridDhtUnlockRequest` 发送 L1679/L1714）
- `org/apache/ignite/internal/processors/cache/distributed/dht/colocated/GridDhtColocatedCache.java`（`lockAllAsync`（"entry point to pessimistic locking within transaction"）L633–660、`GridNearUnlockRequest` 发送 L738/L842）
- `org/apache/ignite/internal/processors/cache/distributed/dht/colocated/GridDhtColocatedLockFuture.java`（类 L93、死锁检测 L1497）
- `org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsExchangeFuture.java`（两阶段释放 L1633–1648、`waitPartitionRelease` L1829–1907（latch L1841–1842、PME 超时回滚 L1893–1897）、`finishLocks` L1925、write-behind flush L1666）

**distributed 公共与消息**
- `org/apache/ignite/internal/processors/cache/distributed/GridDistributedCacheAdapter.java`（`txLockAsync` L102–114）
- `org/apache/ignite/internal/processors/cache/distributed/GridDistributedTxPrepareRequest.java`（字段 L71–147）
- `org/apache/ignite/internal/processors/cache/distributed/GridDistributedTxFinishRequest.java`（字段 L60–103）
- `org/apache/ignite/internal/processors/cache/distributed/GridDistributedTxRemoteAdapter.java`（`batchStoreCommit` 调用 L500）
- `org/apache/ignite/internal/processors/cache/distributed/GridCacheTxRecoveryFuture.java`（类 L49、`nearTxCheck` L129、`prepare` L135–176、请求发送 L156/L274/L312）
- `org/apache/ignite/internal/processors/cache/distributed/GridCacheTxRecoveryRequest.java`、`GridCacheTxRecoveryResponse.java`、`GridNearUnlockRequest.java`、`dht/GridDhtLockRequest.java`、`dht/GridDhtLockResponse.java`、`dht/GridDhtLockFuture.java`、`dht/GridDhtUnlockRequest.java`（存在性与调用点经上文间接核验）

**MVCC、exchange 上下文、store、配置**
- `org/apache/ignite/internal/processors/cache/GridCacheMvcc.java`（类 L50、`locs/rmts` L82/L86、`addLocal` L583、`addRemote` L707、`readyLocal` L797、`doneRemote` L921、`reassign` L1015、`releaseLocal` L1190、`remove` L1235）
- `org/apache/ignite/internal/processors/cache/GridCacheMvccManager.java`（类 L83、字段 L98–152、`finishRemoteTxs` L330、`addFuture` L577、`removeVersionedFuture` L692、`lockedKeys` L814、`remoteCandidates` L830、`addExplicitLock/removeExplicitLock` L912/L940、`finishLocks` L1082、`finishExplicitLocks` L1111、`finishAtomicUpdates` L1132、`finishDataStreamerUpdates` L1151、`recheckPendingLocks` L1218）
- `org/apache/ignite/internal/processors/cache/GridCacheAdapter.java`（`txLockAsync` 抽象声明 L579）
- `org/apache/ignite/internal/processors/cache/GridCacheSharedContext.java`（`partitionReleaseFuture` L938–953、`partitionRecoveryFuture` L966–968、`commitTxAsync` L1073、`rollbackTxAsync` L1111）
- `org/apache/ignite/internal/processors/cache/GridCacheProcessor.java`（`utilityCache` L4460–4461）
- `org/apache/ignite/internal/processors/cache/GridCacheUtils.java`（`UTILITY_CACHE_NAME` L181）
- `org/apache/ignite/internal/processors/cache/store/GridCacheWriteBehindStore.java`（类 L77、`batchSize` L109、flusher 启动 L337–345、`forceFlush` L409、`resolveFlusherByKeyHash` L673–689、`Flusher` L951–1058）
- `org/apache/ignite/internal/processors/cache/store/GridCacheStoreManagerAdapter.java`（`sessionEnd` L784–797）
- `org/apache/ignite/internal/processors/cache/store/CacheStoreManager.java`（`sessionEnd` 声明 L175）
- `org/apache/ignite/configuration/CacheConfiguration.java`（write-behind 默认值 L165–177）
- `org/apache/ignite/configuration/TransactionConfiguration.java`（默认值与遗留开关 L38–88）
- `org/apache/ignite/transactions/TransactionIsolation.java`（3 值 L26–34）、`TransactionConcurrency.java`（2 值 L26–38）、`TransactionState.java`（10 值 L26–54）
- `org/apache/ignite/internal/processors/datastructures/DataStructuresProcessor.java`（`ATOMICS_CACHE_NAME` L146、系统事务 `txStartEx` L739/L841、TRANSACTIONAL 配置 L1080–1097）
- `org/apache/ignite/internal/processors/task/GridTaskProcessor.java`（tasksMetaCache L217）
- `org/apache/ignite/internal/jdbc/thin/JdbcThinConnection.java`（savepoint 抛不支持 L807–826）

**切课验收候选测试**（相对 `vendors/ignite/modules/core/src/test/java/org/apache/ignite/internal/processors/cache/`，存在性均已核验，行号未单开）
- `distributed/dht/GridCacheColocatedTxSingleThreadedSelfTest.java`（6.1）
- `transactions/AbstractDeadlockDetectionTest.java`、`transactions/TxPessimisticDeadlockDetectionTest.java`（6.2）
- `distributed/dht/GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest.java`（6.3）
- `CacheOptimisticTransactionsWithFilterTest.java`、`distributed/IgniteAbstractTxSuspendResumeTest.java`（6.4）
- `CacheSerializableTransactionsTest.java`、`transactions/TxOptimisticPrepareOnUnstableTopologyTest.java`（6.5）
- `transactions/TxRecoveryOnCoordniatorFailTest.java`、`transactions/TxRecoveryConcurrentTest.java`（6.6）
- `CacheStoreTxPutAllMultiNodeTest.java`（6.7）
- `distributed/CacheTxNearUpdateTopologyChangeTest.java`（6.8）

---

## 对复刻课的启示

1. **先把"锁"与"事务"分开教**：2.18 的悲观事务 = MVCC candidate 队列（entry 级）+ lock future（操作级）+ prepare/finish 消息（tx 级）三层叠出来的。复刻时保持同样的分层：`GridCacheMvcc` 可以在单机课（章 1 的 entry 层）就留出 hook，章 6 只补分布式层——否则 6.2 会膨胀成两课的量。
2. **消息对是事务子系统的"真 API"**：commit/rollback 共用 finish 请求、`commit=false` 是唯一回滚载体、`transactionNodes` 随 prepare 请求全局广播——复刻课的验收测试应该直接断言消息序列（这是 ADR 0005 线协议全保真的落点），而不是只断言最终值。
3. **one-phase commit 是性能主干不是优化**：默认拓扑（backups=0/1）下单 primary 事务几乎全走一阶段，`checkOnePhase` 三条件 + `commitOnPrepare` 是隐式微事务课（6.1）的核心教学内容；near reader 与 write-through store 两个"禁用条件"分别在 6.8/6.7 收口。
4. **拓扑变化是事务的第四种结局**：除 commit/rollback 外，remap（client 盲发被拒后重映射）与 PME 超时强制回滚都是真实生产路径；切课上放在 6.5 一课讲透，复刻实现需要 exchange future 与 tx future 的双向引用（`affinityReadyFuture`/`onRemap`）。
5. **恢复靠共识不靠日志中心**：`GridCacheTxRecoveryFuture` 的"问遍 txNodes"是理解 Ignite 无事务日志设计的好入口；复刻课 6.6 用"kill -9 发起节点"做 tracer 最能暴露实现的隐藏状态。
6. **两个"名字陷阱"提前写进讲义**：`GridCacheMvccManager` 不存 candidate（在 entry 的 `GridCacheMvcc` 里）；utility cache 不存 tx 状态（它是系统事务的客户）。学习者从名字猜职责会猜错，讲义应显式纠偏。
