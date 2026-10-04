# 13 · 持久化内部实现：并发协议、编解码、恢复编排与两大配套子系统（Apache Ignite 2.18.0 源码调研）

> Primary source：本仓库 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码 submodule，只读）。文中源码路径相对 `vendors/ignite/modules/core/src/main/java/`（前缀 `core/` = `org/apache/ignite/internal/`，个别文件给全相对路径），行号以当前 submodule 内容为准，全部经实际打开核验。
> 定位：本文是 `04-persistence-sql.md` 第一部分（持久化四件套的**结构层**：PageMemory segment/页头、PageStore 文件布局、WAL 记录类型、checkpoint 工作流）的**内部实现层**续篇——并发协议、逐字节编解码、恢复编排、baseline/activation 与 snapshot。结构层结论直接引用 04 号文档，不重复论证。
> 阅读顺序建议：§1–§2（数据结构层）→ §3（WAL 编解码）→ §4（恢复编排，把前两者串起来）→ §5（配套子系统）→ §6（切课建议）。

---

## 0. 先决纠偏：四个"想当然的名字"在 2.18 中不存在（grep 全库为零命中）

调研提示里的几个名字经全库检索证伪，复刻时不要按这些名字找代码：

| 想当然名 | 实际情况 |
|---|---|
| `InvokeConfirmationBound` | 不存在。invoke 的"确认"机制由内部类 `BPlusTree.Invoke` 的三态布尔 `closureInvoked`（`FALSE/READY/DONE`）+ 尾锁（`finishOrLockTail`）实现（§1.1） |
| `FreeListImpl` | 不存在。体系是 `AbstractFreeList`（泛型基类）+ `CacheFreeList`（数据行特化）（§2.1） |
| `FileWALReader` | 不存在。恢复侧逐条读是 `FileWriteAheadLogManager` 内部类 `RecordsIterator`（extends `AbstractWalRecordsIterator`）；离线工具侧是 `reader/StandaloneWalRecordsIterator`（§3.4） |
| `PartitionMetaPageRecord` | 不存在。分区元数据 WAL 记录是 `MetaPageUpdatePartitionDataRecord`/`V2`/`V3` 一族（§2.4） |

另三处易错：`BPlusTree` **没有 backward cursor**（只有 forward cursor；"backward/backId"是分裂合并方向的术语，§1.3）；`SplitForwardPageRecord` 与 `MergeRecord` 仍存在于枚举与 serializer（读写编解码齐全，`RecordDataV1Serializer.java:1036,1685`）但**没有任何生产写入调用**——源码留 TODO GG-11640：分裂的 forward 页与合并后的 backward 页都以 `walPlc = TRUE` 走整页记录替代（§1.4/§1.5，`BPlusTree.java:3678-3679, 4972-4973`）。离线工具读旧版本 WAL 时仍需实现这两个类型的解码。

---

## 1. BPlusTree 并发协议（`core/processors/cache/persistence/tree/BPlusTree.java`，约 5950 行）

### 1.1 操作栈总览：Get/Put/Remove/Invoke 与 Result 状态机

类头注释（`BPlusTree.java:96-212`）是协议的权威描述：每页一把锁，**同层从左到右加锁；持高层锁时禁止再取低层锁（自底向上加锁），唯一例外是"分配尚无人可见的新页"**（`BPlusTree.java:146-155`）。所有操作共用一个模式：先从根到叶搜索（`Get`），每层可能命中四种结果，对应 `Result` 枚举 `GO_DOWN / GO_DOWN_X / FOUND / NOT_FOUND / RETRY / RETRY_ROOT`（`BPlusTree.java:5886-5904`）：

- `RETRY`：本页加锁失败（页被回收换 tag）或三角不变量被并发修改破坏，重试本节点；
- `RETRY_ROOT`：结构变化太大（如 `g.rmvId < io.getRemoveId(pageAddr)`，`BPlusTree.java:5833-5834`），从根重开。

类层次（全部为 `BPlusTree` 内部类）：

- `Get`（`BPlusTree.java:3114`，抽象）——搜索基类，子类 `GetCursor`（3324，forward cursor 起点）、`Invoke`（3835）；
- `Update` 抽象栈 —— `Put`（3473）与 `Remove`（4321，还实现 `ReuseBag` 把删空的页回收获用）；
- `GetPageHandler<G>`（5822）——把 `PageHandler` 的结果类型特化为 `Result` 的页处理器基类；
- `Tail<L>`（5168）——自底向上逐层写锁住的"尾巴"链，分裂上浮行/合并时逐层持有。

**invoke 乐观栈**：`invoke(row, x, clo)`（`BPlusTree.java:2128-2176`）构造 `Invoke extends Get`，循环 `invokeDown(x, rootId, 0, 0, rootLvl)` 直到非 RETRY。`Invoke` 自带三态布尔 `closureInvoked`（`Bool` 枚举 `FALSE/TRUE/READY/DONE`，`BPlusTree.java:5909-5921`）：

1. 下行阶段在叶层命中或未命中时置 `READY`（`Invoke.found/notFound`，`BPlusTree.java:3898-3930`），此时**尚未调用闭包**；
2. `invokeDown` 在叶层 `FOUND/NOT_FOUND` 分支调用 `x.invokeClosure()`（`BPlusTree.java:2245-2263`）——`READY→DONE`，执行用户 `InvokeClosure.call(foundRow)`，按 `operationType()` 把 `Invoke` 变身成 `Put` 或 `Remove` 装进 `x.op`（`BPlusTree.java:3935-3952`）；
3. 关键一致性保证：闭包执行时**搜索路径上的页仍被本操作持有（或重新加锁）**——从叶层回溯时 `x.op.finishOrLockTail(pageId, page, backId, fwdId, lvl)`（`BPlusTree.java:2241`）要么在尾锁链上完成写操作，要么发现路径失效返回 RETRY 重来（闭包会重复执行，故闭包必须幂等/以 `clo` 携带新值为准）。每层重试有预算：`IGNITE_BPLUS_TREE_LOCK_RETRIES` 默认 1000（`BPlusTree.java:231-235`），耗尽后 `checkLockRetry` 抛 `IgniteCheckedException` 并上报 `FailureType.CRITICAL_ERROR`（3264-3276；`Invoke` 覆写为持尾锁时不抛、继续重试，3825 起）。

**三角不变量的运行时校验**：搜索右拐（`idx == cnt`）时子页的 forward 未知，需要"问邻居"——`askNeighbor(fwdId, g, back)` 读锁自己的 forward 页拿其最左子页（`BPlusTree.java:451-477`、`3007-3009`）；注释明说这不会死锁因为"锁总是沿 forward 方向取"（`BPlusTree.java:457,464`）。若 remove 场景还需要 backId 且当前是 routing page，返回 `GO_DOWN_X`（`BPlusTree.java:474-477`）——父层在下行前先去邻居页把 backId 问回来（`invokeDown` 的 `GO_DOWN_X` 分支，`BPlusTree.java:2211-2225`）。

**尾锁（Tail）机制**——Put/Remove 自底向上传播的载体：`Update.lockTail`（`BPlusTree.java:4748-4770`）在递归回溯时把当前层页面以 `Tail.EXACT` 挂进 `Tail` 链（`LockTail.run0` 校验 `io.getForward == r.fwdId` 后 `r.addTail(...)`，823-845）；无 backId 时先 `lockForward`（写锁 forward 页作尾巴，4777-4792）或有 backId 时 `lockBackAndTail` 双锁。持有尾链后，上浮行的插入（分裂）、inner 替换（`replaceRowInPage`）、合并都能在"路径仍然有效"的前提下完成；`finishOrLockTail` 的语义就是"能完成就完成，完不成就把页挂进尾链返回 RETRY 语义"（调用点 `invokeDown` 2241、`putDown` 3056）。`Get` 系与 `Update` 系的差异由此清晰：**Get 全程乐观（读锁、失败重试），Update 在叶层确定修改后才逐层升级为写锁尾链**。

### 1.2 页面锁的粒度与 tag 防 ABA

锁粒度 = **单个物理页**（不是 pageId）：`PageHandler.readPage/writePage`（`core/processors/cache/persistence/tree/util/PageHandler.java:109-138, 279-330`）先 `pageMem.acquirePage`（把逻辑页钉在某个堆外槽位，返回绝对指针），再对该槽位头部的 `OffheapReadWriteLock` 加读写锁。tag 校验发生在锁内部：

- 页 ID 的 tag 位（`PageIdUtils.tag(pageId)`，随页回收递增）被写进锁字：`OffheapReadWriteLock.init(lock, tag)` 存 `tag << 16` 于 8 字节锁状态高 16 位（`core/util/OffheapReadWriteLock.java:111-117`）；
- `readLock(lock, tag)` 自旋 CAS 前 `checkTag(state, tag)`，不匹配直接返回 false（`OffheapReadWriteLock.java:122-145`）；
- `PageMemoryImpl.readLock/writeLockPage` 拿 `PageIdUtils.tag(pageId)` 去锁，失败返回 0（`core/processors/cache/persistence/pagemem/PageMemoryImpl.java:1584-1592, 1643-1648`），`PageHandler` 把 0 翻译成 `lockFailed`（即 BPlusTree 的 `RETRY`）。

效果：持有旧 pageId 的线程永远无法锁住已被回收复用的新内容（ABA 防护）；页回收方在 `recyclePage` 时用新 tag 重新 init 锁。恢复模式例外：`writeLock(grpId, pageId, page, restore=true)` 走 `writeLockPage(..., !restore)` 跳过 tag 检查（`PageMemoryImpl.java:513-516`），因为重放可能反复重写同一页（§4.3）。

### 1.3 游标：只有 forward

`AbstractForwardCursor`（`BPlusTree.java:5399`，私有抽象类）利用**叶层单向链表**（每页 `getForward` 指向同层下一页，类注释 `BPlusTree.java:119-124`）做范围扫描：

- 构造时以 lowerBound/upperBound + 开闭性换算 `lowerShift/upperShift`（`BPlusTree.java:5424-5430`），`find()` 用 `GetCursor` 下行定位起始叶页（5558-5562），页内 `findLowerBound/findUpperBound` 二分裁剪（5489-5531）；
- `nextPage(lastRow)`：读完当前缓冲后沿 `nextPageId` 走链（5582-5612）；**若 readLock 返回 0（该页被并发合并回收）则 break 出去用最后返回的行作新 lowerBound `reinitialize()` 重找**（5599-5601, 5568-5575）——这就是游标一致性机制：不持跨页锁，靠"重定位 + 页内原子读"。

降序扫描不是树的职责：索引层（H2/InlineIndex）通过组合 forward cursor 与比较器实现；"backward"在树内只指**合并方向**——合并总是把 forward 页并入 backward 页，first page 永不删除（`BPlusTree.java:1264` 注释、§1.5）。

### 1.4 分裂：中位分裂 + WAL delta 写入顺序

`Put.insert`（`BPlusTree.java:3623-3634`）：`cnt == maxCnt` 则 `insertWithSplit`，否则 `insertSimple`。**分裂点 `mid = cnt >>> 1`（中位分裂）**；若开启了"顺序写优化"（索引构建期 `enableSequentialWriteMode()`，`BPlusTree.java:1080-1083`，允许 appender 单线程预分配）则 `mid = cnt * 0.85` 留给后续插入偏向 forward 页（`splitPage`，`BPlusTree.java:2958-2971`）。

**WAL delta 与页修改的顺序**：都在页写锁临界区内，页**先**改、delta 记录**后** log——`insertSimple`：`io.insert(pageAddr, ...)` 改页，随后 `wal.log(new InsertRecord<>(...))`（`BPlusTree.java:3645-3653`）；`doRemove` 同样先 `io.remove` 再 `RemoveRecord`（4837-4847）；`splitPage` 先 `io.splitForwardPage/splitExistingPage` 再 `SplitExistingPageRecord`（2973-2980）。崩溃时内存页丢失，磁盘页只会被 checkpoint 写入，而 checkpoint 保证先落 WAL（04 §1.3/§1.4），所以内存内的先后不影响持久化正确性——**顺序约束真正落在"WAL 记录序"上**：页在本 checkpoint 周期第一次变脏时先有整页 `PageSnapshot`（04 §1.3 第 3 条），此后 delta 才可重放。`needWalDeltaRecord(pageId, page, walPlc)`（`core/processors/cache/persistence/DataStructure.java:285-287` → `PageHandler.isWalDeltaRecordNeeded`，`PageHandler.java:484-495`）判定：页干净（本周期未脏）→ 不写 delta（改由 writeUnlock 时的整页快照兜底）；页已脏 → 写 delta；`walPlc == TRUE` 强制整页。

`insertWithSplit` 全序（`BPlusTree.java:3665-3760`）：

1. `allocatePage` 新 forward 页并写锁（新页首写无需 tag 校验）；
2. `splitPage`（上移行的确定：`moveUpRow = io.getLookupRow(pageAddr, cnt-1)`，3706）；内节点分裂后 `setCount(cnt-1)` + `FixCountRecord`（3708-3712）；
3. 若插入落在 forward 页或需修 forward 页最左子链接：`FixLeftmostChildRecord`（3691-3697）——注释自认"rare case, 可承担独立 WAL 记录避免复杂化"；
4. **forward 页自己不写 SplitForwardPageRecord，而是 `fwdPageWalPlc = TRUE` 走整页记录**（3678-3679 的 TODO GG-11640 与 3700）；
5. 若分裂的是根：分配新根页 `initNewRoot` + `NewRootInitRecord`（新页已知是新的，永不写整页，3727-3745），再 `write(metaPageId, addRoot, ...)` 更新元页。

**合并的 WAL 故事**（`Remove.doMerge`，`BPlusTree.java:4942-4983`）：`left.io.merge(prnt.io, prnt.buf, prntIdx, left.buf, right.buf, emptyBranch, pageSize())` 内存中把 right 并入 left、父页摘除分裂键；随后 `left.walPlc = Boolean.TRUE`（合并页走整页记录，4972-4973 的 TODO GG-11640 即未实现的 MergeRecord）、父页 `doRemove` 写 `RemoveRecord`（4977）、被并入的 right 页 `freePage → recyclePage` 写 `RecycleRecord`（4980；`DataStructure.java:466`、`PagesList.java:1604`——回收即"页 ID 换新 tag 重新初始化并挂入 REUSE_BUCKET"，这就是 §1.2 tag 必须随回收递增的原因）。合并前 `checkChildren` 复核 `(prnt, left, right)` 的三角关系（4928-4933），失败即整链重试。

### 1.5 合并、MetaPage、routing pages

合并分两类（类注释 `BPlusTree.java:189-212`）：**mandatory merge**（叶页删空必须并入兄/弟页，父链上连续 routing page 会级联）与 **regular merge**（回溯路上顺手把能装进一页的祖先对合并掉，提升树形态；成功则再上一层继续试）。注意本实现**不维护"非根节点至少半满"**这一经典 B 树不变量（`BPlusTree.java:181-187` 明确声明不维护）。删空的页不直接释放：装入 `ReuseBag`（`Remove implements ReuseBag`，4321）交给 `ReuseList` 按 tag 轮换复用（§2.1）。

**MetaPage**（页类型 `T_BPLUS_META`，每棵树一页，永不回收——`AddRoot.run` 注释"we should never recycle meta page until the tree is destroyed"，`BPlusTree.java:859,893`）：`BPlusMetaIO`（`core/processors/cache/persistence/tree/io/BPlusMetaIO.java:30-108`）存**每层的 firstPageId 数组 + levelsCount + rootLevel**（`getRootLevel`/`getFirstPageId(pageAddr, lvl)`，`BPlusTree.java:2941-2942`）、inline 大小、feature flags、创建版本、树引用计数。root 的变更只发生在增根/砍根：`AddRoot`（写 `MetaPageAddRootRecord`，898-901）、`CutRoot`（`MetaPageCutRootRecord`，864-867）、`InitRoot`（`MetaPageInitRootInlineFlagsCreatedVersionRecord`，929-934）。热路径不每次读元页：`volatile TreeMetaData treeMeta` 缓存 `(rootLvl, rootId)`，为 null 时才读锁元页加载（`BPlusTree.java:1123-1170`），增/砍根时原地换新（873,906,939）。

### 1.6 数据树 vs 索引树：同一引擎的两种用法

- **数据树**：`CacheDataTree extends BPlusTree<CacheSearchRow, CacheDataRow>`（`core/processors/cache/tree/CacheDataTree.java:56`），每分区一棵，metaPageId 来自分区页 2 的分配；叶子项只存 `(cacheId, hash, link)` 定长键，行体在数据页里（§2.2）。构造参数把 `grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null` 传入——**WAL 是否记录由"有没有 WAL 管理器"这一事实决定**，树代码不感知开关（94-108）。
- **索引树**：`InlineIndexImpl`（`core/cache/query/index/sorted/inline/InlineIndexImpl.java`）按并行度持 `InlineIndexTree[] segments`（64 行），每个 `InlineIndexTree extends BPlusTree<IndexRow, IndexRow>`（`core/cache/query/index/sorted/inline/InlineIndexTree.java:76`）——行即索引行（含内联键值）。索引树的 rootPageId **不放在自己的元页**，而是登记在 **MetaStorage 的索引名→rootPage 映射树**里：`IndexStorage`（`core/processors/cache/persistence/IndexStorage.java:27`）提供 `allocateCacheIndex/findCacheIndex/dropCacheIndex/renameCacheIndex`——建/删索引 = 在元存储树里插入/移除 root 页登记。差异总结：数据树 root 在元页、索引树 root 在元存储；数据树叶子指向数据页 link、索引树叶子自包含；索引树建树时可开 `enableSequentialWriteMode` 走 0.85 分裂偏移。

---

## 2. FreeList 与数据页布局（`core/processors/cache/persistence/freelist/` + `tree/io/`）

### 2.1 空闲页与空闲槽的分配回收：PagesList 体系

`PagesList`（`freelist/PagesList.java:82`）是"**条带化双向页 ID 链表，可选按 bucket 组织**"的基类（类注释 79-81）：每个 bucket 挂多条 `Stripe`（链表尾页），并发上限 `MAX_STRIPES_PER_BUCKET = max(8, CPU 数)`（86-103）；页内容类型 `T_PAGE_LIST_META/NODE`（04 §1.1）。为减少对链表尾页的锁争用，还有一层 **on-heap `PagesCache`**（`bucketCaches`，`AbstractFreeList.java:87`；可用 `IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING` 关闭，`PagesList.java:106-107`——证明该缓存是纯优化，可整体裁掉）。

`AbstractFreeList<T extends Storable> extends PagesList implements FreeList<T>, ReuseList`（`freelist/AbstractFreeList.java:58`）同时扮演两个角色：**FreeList（有剩余空间的数据页登记簿）+ ReuseList（被回收的空页登记簿）**。空闲槽按页剩余空间分桶：`BUCKETS = 256`（2 的幂），步长按 2 的幂（pageSize 4096 时每桶 16B），`REUSE_BUCKET = 255` 专用放回收空页（58-84）。写入一行 `writeSinglePage`（703-719）：

1. `takePage(rowSize)`：小行从"够大的最小桶"向上找 `takeEmptyPage(b)`；找不到或大行直接进 REUSE_BUCKET——`reuseList == this` 时取本列表，否则 `reuseList.takeRecycledPage()` + `initRecycledPage`（换分区号、重 init 页类型）（730-760；`takeRecycledPage` 在 913-917）；还拿不到就 `allocateDataPage(partId)` 分配全新数据页；
2. `write(pageId, writeRowHnd, initIo, row, ...)`：写锁页，`addRow` 落行（必要时跨页分片，`DataPageInsertFragmentRecord`），**然后 `putPage(freeSpace, pageId, ...)` 把页按新剩余空间放回对应桶**（`WriteRowHandler.run`，159-177）。

删除侧：`removeDataRowByLink(link)`（817 起）释放槽后同样按剩余空间 `putPage` 回桶。页彻底空了由树合并路径经 `ReuseList` 回收。**分配与归还的闭环**：`putPage(freeSpace, pageId, ...)`（312 起）把页 ID 挂进对应 bucket 的 on-heap `PagesCache`（命中即无锁快路径）并穿透写入持久化的链表尾页（`PagesList` 的 Stripe 体系）；`takeEmptyPage(bucket)` 优先从 `PagesCache` CAS 摘取，miss 后 `getPageForPut`/`takeEmptyPage` 沿 Stripe 尾页下链（`PagesList.java:741` 起），尾页耗尽则整条 stripe 移除（`removeStripe`，661-698 的 tailId 失效即并发重取机制）。**TTL/pending 树、eviction tracker 都挂在这层**：`evictionTracker.touchPage(pageId)` 在每次行更新时调用（`UpdateRowHandler.run`，126），页淘汰时 tracker 决定该页是否可安全换出（§6 约束③）。

### 2.2 数据页 slot 布局：direct/indirect item 与碎片整理

`AbstractDataPageIO` 类注释（`core/processors/cache/persistence/tree/io/AbstractDataPageIO.java:39-165`）是权威定义，页内两个相向生长的数组：

- **items 表在页头**，每项 2 字节。**direct item**（ID==下标）存行数据在页内的 offset；**indirect item** 存 `(indirectId 1B, 指向的 direct item 下标 1B)`。`[0, directCnt)` 恒为 direct，`[directCnt, directCnt+indirectCnt)` 恒为 indirect——**行数 == directCnt**；
- indirect item **只会由删除产生**：删 item 07 时，让被删项指向最后一个 direct 项的行，最后一个 direct 项翻转为指向它的 indirect（110-114）；再删时 indirect 区整体左移紧凑（132-141）；插入时新 direct 项插到 direct 区尾、indirect 区右移，若新 direct 的 ID 撞上已有 indirect ID 则该 indirect 就地转正（143-164）。

这个设计的意义：**页内碎片整理（compact，行数据前移）时只有 direct 项的 offset 变化，所有 indirect 引用（即外部 link）永不失效**（56-57 行注释）。外部 link 的编码即 `PageIdUtils.link(pageId, itemId)`（类注释 46-47 直接点出）——树叶子项、`CacheDataRowStore` 的读写、TTL pending 树都用这个二元组定位行。行的物理字段序（`DataPageIO.writeRowData`，`tree/io/DataPageIO.java:51-80`）：`payloadSize(2B) + [cacheId(4B 可选)] + key + value + CacheVersion + expireTime(8B)`——与 WAL `DataEntry` 字段序同源（§3.2）。大行跨页分片为 fragment 链（`writeFragmentData` 的 `CACHE_ID→KEY→EXPIRE_TIME→VALUE→VERSION` 分片序，83-103）。

**页内空间簿记**：页头存 `firstEntryOffset`（数据区右边界）与 `realFreeSpace`（302-323）；对外语义 `getFreeSpace()` 是"保证能放下的最大行净长"——从 realFreeSpace 里预留 `ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE` 开销（337-350），且无空 item 槽时直接报 0（338-339）。

### 2.3 CacheDataRowStore 与分区元数据页

`CacheDataRowStore extends RowStore`（`core/processors/cache/tree/CacheDataRowStore.java:31`）是**每分区的行读写门面**：构造 `(grp, freeList, partId)`（57-61），`dataRow(cacheId, hash, link, rowData)` 按 link 惰性解出行（76-98）；`RowStore` 持 freeList 提供 `insertDataRow/removeDataRowByLink/readRow`。数据树（§1.6）的 rowStore 参数就是它——**树管键→link 映射，RowStore+FreeList 管行体存放**。

**分区元数据页**（页类型 `T_PART_META`，每分区 pageIdx=1）：`PagePartitionMetaIO`（`tree/io/PagePartitionMetaIO.java:30-123`）布局 `SIZE_OFF = PageMetaIO.END_OF_PAGE_META`，随后 `UPDATE_CNTR_OFF(+8)、GLOBAL_RMV_ID_OFF(+8)、PARTITION_STATE_OFF(1B)、NEXT_PART_META_PAGE_OFF(+8)`（32-47）；V2/V3 追加 reserved/加密字段（`PagePartitionMetaIOV2/V3` 同包）。`initNewPage` 全零 + `state=-1`（57-65）。**更新点唯一**：`GridCacheOffheapManager.saveStoreMetadata`（`core/processors/cache/persistence/GridCacheOffheapManager.java:406-481`）在写锁元页内 set size/updateCounter/globalRemoveId/state（含加密进度、共享组 per-cache sizes 指针），任一变化即 `wal.log(new MetaPageUpdatePartitionDataRecordV3(...))`（462-475）。恢复时 store（`CacheDataStore` 委托）从该页重建 `restoreState(size, updCntr, cacheSizes, data)`（`GridCacheOffheapManager.java:2005`）。

### 2.4 每分区装配：root 页的登记与 CacheDataStoreImpl

把 §1.6/§2.1-2.4 的零件装成每分区一套的，是 `GridCacheOffheapManager` 内部的 `init0`（1860-1963，checkpoint 写锁内执行）：

1. 从 `PartitionMetaStorageImpl`（每分区自己的小元存储树，1870-1889）读出三个 root 页登记：`treeRoot`（数据树）、`pendingTreeRoot`（TTL pending 树）、`partMetastoreReuseListRoot`；
2. `new CacheDataRowStore(grp, freeList, partId)`（1893，freeList 为整个 data region 级共享——同 region 的所有分区共用一页池）；
3. `new CacheDataTree(grp, name, freeList, rowStore, treeRoot.pageId(), treeRoot.isAllocated(), ...)`（1897-1913）——**`isAllocated == false` 时树自己走 initTree 建首根页并回写登记**；
4. `new PendingEntriesTree(...)`（1919-1935）同样模式；
5. 组装 `delegate0 = new CacheDataStoreImpl(partId, rowStore, dataTree, () -> pendingTree0, grp, busyLock, log, ...)`（1941-1948）——**这就是存储接缝的持久化侧实现**：上层 `CacheDataStore` 接口（章 1 的接缝）由此获得 `put/get/remove/size/pendingTree` 的页式实现。

三棵树的 `allocatePageNoReuse` 覆写都 `assert ctx.database().checkpointLockIsHeldByThread()`（1860-1863 等）——**页分配必须持 checkpoint 读锁**，这是全持久化层的隐性约定（04 §1.4 的 checkpoint 读写锁在此处被数据结构层直接依赖）。

---

## 3. WAL 编解码（`core/processors/cache/persistence/wal/serializer/`）

### 3.1 帧格式与 CRC

`RecordV1Serializer`（实现 `RecordSerializer`，版本 1）把"帧"职责收进 `RecordIO` 接口（`RecordV1Serializer.java:115-192`）：

- **普通记录帧** = `记录类型 1B（type.index()+1，0 保留为 STOP_ITERATION） + WAL 指针 12B（segmentIdx 8B + fileOffset 4B，putPosition 246-249） + 记录数据 + CRC 4B`（sizeWithHeaders 119-127；putRecordType/readRecordType 336-355）；
- `SWITCH_SEGMENT_RECORD` 特例：**只有 1 字节类型、无指针无 CRC**（125-126, 182-184）——segment 尾部塞不下整帧时的单字节哨兵，读侧遇之抛 `SegmentEofException`（136-137）；
- CRC：写入侧 `writeWithCrc` 对整帧（除 CRC 自身）算 `FastCrc` 后 `putInt`（413-438）；读侧 `readWithCrc` 用 `Crc32CheckingFileInput` 包装流边读边校验（367-384），失败按恢复上下文决定抛错或当段尾（§4.4）；读侧还校验**指针连续性** `ptr == expPtr`，不符按段滚动处理（139-143）。

### 3.2 逐记录类型编码（`RecordDataV1Serializer`）

- **`PAGE_RECORD`（PageSnapshot）**：`cacheId(4) + pageId(8) + 整页字节[]`（页长来自当前配置 pageSize，`RecordDataV1Serializer.java:585-595`）——加密组时页长为 realPageSize。
- **`CHECKPOINT_RECORD`**：`uuidMsb(8)+uuidLsb(8) + hasPtr(1) + [idx(8)+off(4)+len(4)] + 分区状态表 + end 标记(1)`（597-617）。分区状态表布局（写侧 `putCacheStates`，2028-2046）：`grpCnt(2) + [grpId(4) + partCnt(2) + (partId(2) + partitionSize(8) + updateCounter(8))*]`——恢复侧重建分区初始状态与 store 计数器全靠这张表（§4.4）。
- **`DATA_RECORD_V2`（DataRecord/DataEntry）字段序**（读侧 2096-2168，写侧 `putPlainDataEntry` 2002-2023，两者严格互逆）：`cacheId(4) + keySize(4)+keyType(1)+keyBytes + valSize(4)[>=0 时 valType(1)+valBytes，-1 表示无值] + op(1) + nearXidVer(GridCacheVersion) + writeVer + partId(4) + partCntr(8) + expireTime(8) + flags(1，仅 V2/CDC 有)`；`entryCnt(4)` 为 DataRecord 前缀，条目数 1 时走单条目优化（667-682）。cacheId 无对应运行 cache 时退化为 `LazyDataEntry` 惰性反序列化（2152-2167）。
- **B+树 delta 族**（写侧样例）：`DataPageInsertRecord` = `grpId+pageId+payloadLen(2)+payload`（1428-1438）；`InsertRecord` 含 `io(版本)+idx+rowBytes+rightId`；`SplitExistingPageRecord` = `grpId+pageId+mid+fwdId`；`MetaPageAddRootRecord` = `cacheId+metaId+rootPageId`（各 delta 类自带 `applyDelta(pageMem, pageAddr)` 供恢复重放，§4.3）。
- **`MEMORY_RECOVERY`**：仅一个时间戳 `long ts`（652-657）。
- **`HEADER_RECORD`**：`magic(8)+serializerVersion(4)`（1421-1426；REGULAR/COMPACTED 双魔数见 `readSegmentHeader` 285-296）。
- **`TX_RECORD`**：委托 `TxRecordSerializer`（`serializer/TxRecordSerializer.java`，事务参数与状态枚举）。

### 3.3 版本协商

每个 segment 第一条记录是 HEADER_RECORD：类型 + 指针（segmentIdx, 0）+ 魔数 + serializer 版本 int + CRC（写侧 `FileWriteAheadLogManager.prepareSerializerVersionBuffer`，`wal/FileWriteAheadLogManager.java:2737-2768`；读侧 `RecordV1Serializer.readSegmentHeader`，260-305）。读侧打开任一 segment 先读头拿版本，再由 `RecordSerializerFactoryImpl.createSerializer(ver)` 选 `RecordV1Serializer`/`RecordV2Serializer`（`serializer/RecordSerializerFactoryImpl.java:68-94`；**未知版本直接抛错，无前向兼容**，90-93）。写侧版本由 walSerializer 配置决定，rollover 时若 handle 版本与当前 serializer 不一致会先切换（`FileWriteAheadLogManager.java:771-775`）。V2 的差别是把物理记录改成自定义二进制布局（`RecordDataV2Serializer`），逻辑记录编码不变。

### 3.4 恢复侧读取：RecordsIterator（不是 FileWALReader）

在线恢复入口 `FileWriteAheadLogManager.replay(WALPointer start, filter)`（1001-1043）：`end = 当前活跃 handle 的 position()`，返回 `RecordsIterator`（内部类，`FileWriteAheadLogManager.java:2839`，extends `wal/AbstractWalRecordsIterator`）——逐 segment 打开文件、读 HEADER 定 serializer、逐条 `readWithCrc`，跨活跃/归档 segment 路由由 `SegmentRouter`/`SegmentAware` 处理；起始指针定位即从 `start` 所指 segment+offset 开始（`AbstractWalRecordsIterator` 基础设施）。迭代骨架在基类 `AbstractWalRecordsIterator.advance()`（`wal/AbstractWalRecordsIterator.java:176-215`）：`advanceRecord` 逐条读，读到段尾则 `advanceSegment` 换下一段——**每段独立打开 handle、重读 HEADER 选 serializer**（75-89 的 `serializerFactory` 字段与 260 的抽象 `advanceSegment`），同一 WAL 流内段间版本可以不同。尾部语义按位置分档：`WalSegmentTailReachedException` 在**工作目录**段上属正常（活跃段尚未写满）只告警截断，在**归档目录**段上即损坏必抛（`validateTailReachedException`，200-213 与 227-234）。恢复上下文对 CRC 错误同样分档：`throwsCRCError()` 决定坏 CRC 是"段尾"还是"致命"——二进制恢复未过 checkpoint 标记时必抛（数据不完整不可用），已过标记后容忍到段尾（`RestoreBinaryState.throwsCRCError`，`GridCacheDatabaseSharedManager.java:3600-3610`；`throwsError` 分类 3518-3528）。CDC/离线分析工具另用 `wal/reader/StandaloneWalRecordsIterator`。单条读取 API `read(ptr)` 也只是 `replay(ptr)` 取第一条（989-998）。

---

## 4. 恢复编排（`core/processors/cache/persistence/GridCacheDatabaseSharedManager.java`）

### 4.1 入口链：节点启动即恢复（与 cluster state 无关）

持久化节点在 **kernal start 阶段**就做内存恢复：`IgniteKernal.start` 末尾 `ctx.cache().context().database().startMemoryRestore(ctx, startTimer)`（`core/IgniteKernal.java:1131`）——即使集群处于 INACTIVE，本地页内存/WAL 已就绪（这正是持久化模式 INACTIVE 启动的组件行为基线：数据层活、缓存面未激活，§5.1）。全链（`GridCacheDatabaseSharedManager.startMemoryRestore`，1885-1961）：

1. `initAndStartRegions`（lazy 分配的 region 此刻真正 mmap）；
2. **物理恢复** `restoreBinaryMemory(groupsWithEnabledWal(), physicalRecords())` → `restoreBinaryMemory`（1060-1122）→ `performBinaryMemoryRestore`（2071-2382）；
3. **逻辑恢复** `applyLogicalUpdates(status, ..., logicalRecords(), false)`（1928-1935）；
4. `cctx.tm().clearUncommitedStates()` + `cctx.wal().startAutoReleaseSegments()`（1937-1939）；
5. 记下 `walTail`，延迟到 activation 时 `resumeWalLogging`（1958-1960, 1964 起；`finishRecovery` 1000-1027：`resumeWalLogging()` + 重建 metastorage）。

此外还有一条更早的 **metastore 预恢复**：`readMetastore`（838-847）在 kernal start 早期只对 metastore 组做一次 `performBinaryMemoryRestore(status, onlyMetastorageGroup(), physicalRecords(), false)`——元存储必须先活，后续组件（含 BLT 持久化，§5.1）才能读它；`finalizeState=false` 表示不补 checkpoint。分区销毁在恢复期被显式排队串行化：`schedulePartitionDestroy` → `checkpoint/PartitionDestroyQueue`（`GridCacheDatabaseSharedManager.java:2291, 2318-2320`；`checkpoint/PartitionDestroyQueue.java`）。

### 4.2 checkpoint 标记选择与损坏处理

`readCheckpointStatus` → `CheckpointManager.readCheckpointStatus`（`checkpoint/CheckpointManager.java:284-285`）→ `CheckpointMarkersStorage.readCheckpointStatus`（`checkpoint/CheckpointMarkersStorage.java:264-319`）：扫 `cp/` 目录，按文件名 `<ts>-<uuid>-(START|END).bin` 各取**时间戳最大**的 START 与 END，从标记文件读 12B WALPointer（285-313）。核心判定 `CheckpointStatus.needRestoreMemory()`：`cpStartId != cpEndId && cpStartId != NULL` 即"上次 checkpoint 没写完 END"（`checkpoint/CheckpointStatus.java:69-75`）。损坏处理策略：

- START 存在但 WAL 里读不到对应 CheckpointRecord → `NoSuchElementException` 包装成 `StorageException`（"Make sure WAL folder properly mounted"，2129-2134）；
- 恢复中读到 START 指的 CheckpointRecord 前就遇到坏段：`needApplyBinaryUpdates` 仍为 true 时 CRC 错误**必须抛**（`RestoreBinaryState.throwsCRCError`，3600-3610）——因为不完整的物理状态不可用；已过 checkpoint 标记后（只追逻辑记录）则容忍到段尾；
- 标记在、记录缺 → "checkpoint marker is present on disk, but checkpoint record is missed in WAL" 抛错（2368-2372）。

**页文件加载**：不是"整文件载入"，而是 lazy——`PageMemoryImpl` 按需从 PageStore 读页（04 §1.1），恢复只负责把**上次 checkpoint 之后**的变更追上进内存的页。

### 4.3 WAL 重放：快照覆盖 vs delta 应用，tag 防重放

`performBinaryMemoryRestore`（2071-2382）：

- 起点指针：`status.endPtr`（最后完整 checkpoint 的 END）；若 `needRestoreMemory`，则读 START 处 CheckpointRecord 的 `checkpointMark()` 作为物理回放起点 `recPtr`（2086-2124）；
- **可选加速**：`CheckpointRecoveryFile`（checkpoint 期间额外落盘的恢复数据，`DataStorageConfiguration.DFLT_WRITE_RECOVERY_DATA_ON_CP = false` 默认关，`configuration/DataStorageConfiguration.java:191-194`）——开启时先并行应用恢复文件里的整页，再落 WAL 只补 `PART_META_UPDATE_STATE`/`PARTITION_DESTROY`（2144-2226）；配置翻转导致文件有/无与配置不符会直接抛 StorageException（2151-2160）；
- `cctx.wal().replay(recPtr, recordTypePredicate)` 逐条分派（2231-2355）：`PAGE_RECORD` → `applyPageSnapshot`（整页覆盖，2246-2278）；`PART_META_UPDATE_STATE` → 分区销毁调度或取消（2280-2308）；`PARTITION_DESTROY` → `pageMem.invalidate` + 销毁队列（2310-2324）；其余 `PageDeltaRecord` → `applyPageDelta`（2327-2353）。应用按 `(grpId, partId)` 提交到 `CacheStripedExecutor` 保序并行（`stripedApplyPage`，2390-2405）；
- **delta 应用**：`applyPageDelta` 里 `pageMem.writeLock(grpId, pageId, page, restore=true)` 跳过 tag 校验（2461-2477，`PageMemoryImpl.java:513-516`）后 `pageDeltaRecord.applyDelta(pageMem, pageAddr)`——注释明示原因："we may be applying memory changes after several repetitive restarts and the same pages may have changed several times"（2461-2462）。**防重放旧物理记录**的机制不是 tag 而是位置：回放从最后 checkpoint 标记起，且恢复完成立即写一条 `MemoryRecoveryRecord` 并把指针挂到下一个 CheckpointRecord 的 mark 上（§4.5）；
- `RestoreBinaryState.next()` 监视 CheckpointRecord：一旦遇到 `checkpointId == status.cpStartId` 的记录，`needApplyBinaryUpdates = false`（3562-3586）——之后的物理记录只扫描不应用；
- 收尾：`finalizeCheckpointOnRecovery(cpStartTs, cpStartId, startPtr, exec)` 替中断的 checkpoint 补写 END（2378；`checkpoint/Checkpointer.java:1084-1091` → `CheckpointWorkflow`）。

**recovery 后 CacheDataStore 重建**：逻辑恢复中 `CheckpointRecord.cacheGroupStates()` 携带的分区状态写入 `partitionRecoveryStates`（2638-2656），缓存组启动时 `GridCacheOffheapManager` 依分区元数据页重建 store 委托并 `restoreState`（`GridCacheOffheapManager.java:2005`）；`restoreStateOfPartition` 处理 EVICTED 等 recovery state（495-499）。

### 4.4 逻辑恢复与 uncommitted 事务

`applyLogicalUpdates`（2592-2690）从 `status.startPtr` 起过滤逻辑记录：

- `TX_RECORD` → `txMgr.collectTxStates(txRec)`（2630-2637）；
- `CHECKPOINT_RECORD` → 收集初始分区状态（2638-2656）；
- `ROLLBACK_TX_RECORD` → 对应分区 `updateInitialCounter` 回退计数器（2658-2668）；
- `DATA_RECORD*` → 逐 DataEntry 应用到 offheap，**`txMgr.uncommitedTx(dataEntry)` 为 true 的跳过**（2680-2684）。

`collectTxStates`（`core/processors/cache/transactions/IgniteTxManager.java:3586-3591`）：`COMPLETED_TX_STATES`（COMMITTED/ROLLED_BACK）→ 从 `uncommitedTx` 移除 nearXidVersion；`PREPARED_TX_STATES`（PREPARED/PREPARING）→ 加入（谓词定义 190-196；即 research 12 §1.2 所述机制）。

**重放出的 PREPARED 未 COMMIT 事务怎么办**——分两层回答：

- **本地持久层（本文范围）**：物理页保留（PageDelta 已应用——树结构完整性优先），逻辑数据丢弃（`uncommitedTx(dataEntry)` 命中即 `continue`，2683-2684）；恢复收尾 `clearUncommitedStates()` 清空集合（`GridCacheDatabaseSharedManager.java:1937`）。即：**重启后这些事务既不自动提交也不自动回滚，页上留下"孤儿数据"，语义裁决权交给事务恢复协议**。
- **跨节点事务恢复（research 12 已覆盖，此处只给接口）**：primary 失联触发 recovery future 遍历 `transactionNodes`；对仍有 PREPARED TxRecord 的事务，`IgniteTxManager.commitIfPrepared`（`IgniteTxManager.java:2367`）按 `txsPreparedOrCommitted`（2117）查询多数派决定补提交（此后对应的 DataRecord 才真正生效——本地重放时被跳过的条目由恢复流程重新写入）或回滚（孤儿数据被 remove 清理）；`finishTxOnRecovery`/`finishAllTxs` 见 research 12 §6 的锚点清单。

### 4.5 恢复完成 → 正常服务

物理恢复末尾（成功路径）：`cctx.wal().resumeLogging(restored)` 从恢复指针续写 WAL（1098-1099；`FileWriteAheadLogManager.resumeLogging`，745 起），**立即 `wal.log(new MemoryRecoveryRecord(now))` 并把指针交给 `checkpointManager.memoryRecoveryRecordPtr(...)`**（1104-1106）——下一次 checkpoint 开始时 `CheckpointWorkflow.markCheckpointBegin` 用它构造 `new CheckpointRecord(memoryRecoveryRecordPtr)` 作为 checkpointMark（`checkpoint/CheckpointWorkflow.java:234-236`），使**再下一次物理恢复的回放起点跳过全部旧物理记录**（记录时间序防重放）。缓存全建好后 `onStateRestored`：`checkpointManager.start()` 启 checkpointer 线程 + `forceCheckpoint("node started")` 并等 LOCK_RELEASED（2056-2063）——**首个 checkpoint 是恢复收尾的一部分**，此后脏页归零、WAL 可截断归档。activation 侧入口 `onActivate` → `finishRecovery`（899-910, 1000-1027）。

---

## 5. baseline / activation 与 snapshot

### 5.1 GridClusterStateProcessor：activate/deactivate 状态机

`GridClusterStateProcessor extends GridProcessorAdapter`（`core/processors/cluster/GridClusterStateProcessor.java:130`）持有 `volatile DiscoveryDataClusterState globalState`（160）——状态与转换进度都在 discovery 数据里全集群复制。三态 `INACTIVE/ACTIVE/ACTIVE_READ_ONLY`（106-108）。

**发起端** `changeGlobalState(state, forceDeactivation, baselineNodes, forceChangeBaselineTopology, isAutoAdjust)`（1106-1225）：安全检查 → maintenance 拒绝 → `calculateNewBaselineTopology`（1126-1128；activate 且无 BLT 时默认以当前 server 节点集建 baseline，即 **auto-baseline**；纯内存集群另有 `autoAdjustBaseline`：active + `baselineAutoAdjustTimeout()==0` 时拓扑变化即刻用新 server 集合构造 `ChangeGlobalStateMessage` 本地走一遍状态机，1592-1633）→ client 节点转发 compute 到 oldest server（1135-1136）→ server 侧 `stateChangeFut` CAS 保证**同时只有一个转换**（1152-1179）→ activate 时预读 metastore 里的 cache 配置随消息携带（1181-1211）→ `ctx.discovery().sendCustomEvent(new ChangeGlobalStateMessage(...))`（1202-1218）。

**每节点执行端** `onStateChangeMessage`（664-774）：已在转换中则按 `isApplicable` 拒绝或搭车（685-713）；INACTIVE 且非 force 时若存在纯内存 cache 直接拒绝（"DATA_LOST" 保护，715-727）；否则 `ctx.cache().onStateChangeRequest(msg, topVer, state)` 产出 `ExchangeActions`，经 PME 完成真正的启停，`globalState = createTransitionState(...)` 进入转换态（730-771）。**转换的收敛**：全部节点 PME 完成后协调者发 discovery 消息 `ChangeGlobalStateFinishMessage(requestId, targetState, ...)`（603；纯内存 auto-baseline 场景用 `onStateFinishMessage` 本地闭环，1625-1627），各节点把转换态定稿为最终态并唤醒等待者；失败经 `GridChangeGlobalStateMessageResponse`（IO 消息逐节点回执，1499/1526）携带异常回滚；发起端 future 由 `wrapStateChangeFuture`（1347）在"集群定稿或本地出错"先到者上完成。**持久化模式 INACTIVE 启动**：`stateOnStart` 在 persistence 下默认 INACTIVE（483-491），但 §4.1 已示 `startMemoryRestore` 照常执行——数据层恢复在 activation 之前，`finishRecovery`（含 WAL resume 与 metastorage 重建）在 `onActivate` 时完成（`GridCacheDatabaseSharedManager.java:899-910`）。节点在转换中途加入拓扑的竞态也有显式处理：`IgniteKernal` 启动末尾若发现 `transitionWaitFuture` 非空则阻塞等转换完成再决定本地 active 与否（`core/IgniteKernal.java:1179-1198`）。**baseline add/remove 的 core 侧入口**：`IgniteClusterImpl.setBaselineTopology`（`core/cluster/../internal/cluster/IgniteClusterImpl.java:430-444`）校验（必须 ACTIVE、非空、移除的必须离线，460-491）后就是一次 `changeGlobalState(ACTIVE, false, baselineNodes, forceChangeBaselineTopology=true)`（436）——**改 baseline 复用 activate 状态机**。

### 5.2 IgniteSnapshotManager：快照 = 借 checkpoint 拷贝一致性页文件

`IgniteSnapshotManager`（`core/processors/cache/persistence/snapshot/IgniteSnapshotManager.java:242-243`）。**触发链**：control.sh/JMX → `createSnapshot(name, ...)`（1941-2126）：校验（ACTIVE、有 baseline、开了持久化、名字 `[a-zA-Z0-9_]`、无并行快照、增量需 WAL compaction，1962-2056）→ 构造 `SnapshotOperationRequest`（含 baseline 节点集合）→ `startSnpProc.start(...)`（2116）。`startSnpProc` 是 `DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse>`（436-437）——其 discovery 初始化消息 `SnapshotStartDiscoveryMessage implements SnapshotDiscoveryMessage`（4149-4150；`SnapshotDiscoveryMessage` 接口让 exchange/PMEx 把快照当作拓扑事件的一部分协调：防止快照期间 rebalance/PME 并发，多节点在同一拓扑版本上开始）。

**本地阶段** `initLocalSnapshotStartStage`（767-838）：建 `SnapshotFileTree`、查 baseline 节点在线、加密/重加密互斥，然后 `initLocalFullSnapshot`（976-1085）注册 `SnapshotFutureTask` 并返回响应；协调端收齐后 `task.start() == true` 时 **`cctx.database().forceNewCheckpoint("Start snapshot operation: ...")`**（2301-2302）——**快照不自己造一致性，而是排队一次强 制 checkpoint**。

`SnapshotFutureTask` 本身是 **CheckpointListener**（`snapshot/SnapshotFutureTask.java:260-379`）：

- `beforeCheckpointBegin`：挂 `finishedStateFut` 到 checkpoint 完成_future（`cpEndFut`），先 flush metastore（282-308）；
- `onMarkCheckpointBegin`（checkpoint 写锁内）：`wal.log(new ClusterSnapshotRecord(snpName))` 留痕并 `ctx.walFlush(true)`（311-324），收集要拍的分区、给每个分区文件挂 `PageStoreSerialWriter` 监听 checkpoint 写页（326-347）；
- `onCheckpointBegin`（锁释放后）：提交拷贝任务——`saveGroup` 里每个分区 `snpSndr.sendPart(源 part-*.bin → 快照目录)`，并 `runAfterBothAsync(cpEndFut)` 等 checkpoint 落盘完成后收尾 delta writer（382-410）。**因此快照内容 = checkpoint 刚写出的页文件副本 +（并发写页期间被 checkpoint 刷写的增量以 .delta 捕获）+ metastore + cache 配置 + SnapshotMetadata(.smf)**；`PageStoreSerialWriter`（676-726）在 checkpoint 写锁内初始化 `AtomicBitSet(store.pages())`，监听期间被写出的页进 delta 文件、未写的靠拷贝整文件——"快照=checkpoint 页文件的副本"成立，但实现是**边 checkpoint 边挂账**的零阻塞拷贝。

**目录格式**（`filename/SnapshotFileTree.java:48-72` 与 `filename/NodeFileTree.java:184-189`）：`snapshots/<name>/<consistentId>/cache-*/part-N.bin`（压缩加 `.zip`）、增量 `part-N.bin.delta/.idx` 与 `increments/NNNN/`、元数据 `<...>.smf`；快照期间的 WAL 段也按 `U.fixedLengthNumberName(idx, ZIP_WAL_SEG_FILE_EXT)` 收录（370-371）以支撑增量。**多节点协调**：`DistributedProcess` 收集所有 baseline 节点的 start 阶段响应（全成或带警告才继续），end 阶段（`endSnpProc`/`SnapshotOperationEndRequest`）收尾并落 `SnapshotMetadata`（含 BLT consistentId 集合，1054-1070）；节点把进行中快照目录登记进 metastore（`SNP_RUNNING_DIR_KEY`，2549-2567）以便崩溃清理。

**增量快照（可选后置，复刻默认跳过）**：前置条件是 WAL compaction 开启（`createSnapshot` 校验，2028-2032）且基快照同名存在、`incIdx = maxLocalIncrementSnapshot + 1`（2034-2040）。启动时 `handleIncrementalSnapshotId`（846-872）做两件事：给事务消息套 `IncrementalSnapshotAwareMessage` 包装（tx 与快照边界对齐），以及起 `IncrementalSnapshotMarkWalFuture` 在 WAL 里打增量标记；随后 `initLocalIncrementalSnapshot` 确定 WAL 下界指针：首个增量用基快照 `meta.snapshotRecordPointer()`，后续增量用上一增量的 `incrementalSnapshotPointer()`（886-903），`IncrementalSnapshotFutureTask` 只拷 `part-N.bin.delta/.idx`（增量页位图）到 `increments/NNNN/` 并落 `IncrementalSnapshotMetadata`（905-930）。

---

## 6. 章 7 切课建议（分级结论）

按切课纪律（一课一个新概念簇、单课 2–4h、宁多切不一课双簇；ROADMAP §3.2/§4），章 7"持久化"建议 **9 课**。依赖图（`→` 为硬前置）：

```
7.1 PageMemory → 7.2 PageStore → 7.3 接缝替换(c1) → 7.4 WAL → 7.5 checkpoint → 7.6 恢复 → 7.7 baseline(c4) → 7.8 TTL+eviction(c2,c3) → 7.9 snapshot(c5)
                                    ↘ 7.4 BPlusTree/FreeList(树先于 WAL：delta 由树产生)
```

（`c1`–`c5` 即下表"约束落位"列的五个已定约束编号；7.8 若拆两课，7.8a=TTL、7.8b=eviction，依赖关系不变——两者都不依赖对方。）

| 课 | 概念簇 | tracer（验收里程碑） | 关键 vendor 锚点 | 改写测试候选（vendor 测试路径） | 约束落位 |
|---|---|---|---|---|---|
| 7.1 | PageMemory 本体：segment/页头/OffheapReadWriteLock+tag/borrowOrAllocateFreePage | 无盘内存页池：分配/锁/tag 回收单测绿 | `PageMemoryImpl.java:1584-1660`、`OffheapReadWriteLock.java` | `db/pagemem/BPlusTreePageMemoryImplTest`、`ClockPageReplacementFlagsTest` | — |
| 7.2 | PageStore 文件层：FilePageStore 17B 头/偏移/CRC、NodeFileTree、FilePageStoreManager | 重启进程后页文件字节可复读（尚无树） | 04 §1.2 + `FilePageStore.java:69-76,703` | `db/file/IgnitePdsCacheIntegrationTest` | — |
| 7.3 | BPlusTree+FreeList+数据页（结构，无 WAL）：invoke 乐观栈、分裂合并、direct/indirect item、PagesList 桶 | 纯内存（NoStoreImpl 数据区）put/get 走真树 | §1、§2；`BPlusTree.java:96-212` | `internal/processors/database/BPlusTreeSelfTest`、`BPlusTreeReplaceRemoveRaceTest`、`CacheFreeListSelfTest`、`FreeListCutTailDifferentGcTest` | **①存储接缝替换**：CacheDataStore 实现换成 PageMemory+树+FreeList，接缝本身不动 |
| 7.4 | WAL：帧格式/CRC、RecordV1Serializer、FileWriteAheadLogManager 写入与 rollover；树上每个变更点挂 delta（§1.4 顺序） | put 后 `.wal` 文件可被逐帧解析回放（离线 iterator） | §3；`RecordV1Serializer.java:115-192`、`BPlusTree.java:3645-3653` | `db/wal/IgniteWalFormatFileFailoverTest`、`db/wal/crc/` | ①（"真 WAL"在此落地） |
| 7.5 | checkpoint：读写锁、脏页集、START/END 标记、checkpointer 线程、WAL 反压（04 §1.3 第 5 条） | kill -9 后 part-*.bin 含已 checkpoint 页（tag/内容抽查） | 04 §1.4 + `CheckpointMarkersStorage.java:264-319` | `db/IgnitePdsCheckpointRecoveryTest`、`db/CheckpointBufferDeadlockTest` | ①（"真 checkpoint 锁"在此落地） |
| 7.6 | 恢复编排：readCheckpointStatus/物理重放/逻辑重放/MemoryRecoveryRecord/首 checkpoint/uncommitted tx | **重启后数据存活**（章 tracer）+ 断 checkpoint 中途重启可恢复 | §4；`GridCacheDatabaseSharedManager.java:2071-2382` | `db/wal/IgniteWalRecoverySeveralRestartsTest`、`IgniteWalReplayingAfterRestartTest` | ① 收官；衔接 research 12 §1.2 |
| 7.7 | baseline/activation：GridClusterStateProcessor 状态机、discovery custom event、INACTIVE 启动、BLT 语义与 auto-baseline | INACTIVE 起 → activate → 再 deactivate → 数据仍在 | §5.1；`GridClusterStateProcessor.java:1106-1225` | `persistence/standbycluster/IgniteChangeGlobalStateAbstractTest`、`IgniteChangeGlobalStateTest` | **④前半**（historical rebalance 消费的 cp 指针/BLT 概念在此建立；rebalance 本体在章 4 已有，historical 补课回填此课后） |
| 7.8 | TTL 后台清理 + eviction/data region 治理（同一课吗？**不**——TTL 与 eviction 若超预算拆 7.8a/7.8b） | 设 TTL 的条目到期被后台清除；压满 data region 触发页淘汰不丢数据 | `GridCacheSharedTtlCleanupManager.java`、`PendingEntriesTree.java:32`、`evict/RandomLruPageEvictionTracker.java` 等 | `cache/GridCacheTtlManagerSelfTest`、`db/IgnitePdsPageEvictionTest` | **②TTL**（cleanup manager+pending 树）、**③eviction** |
| 7.9 | snapshot：createSnapshot → DistributedProcess → 借 checkpoint 拷贝、.smf、增量（可选后置） | control 命令出快照 → 从快照恢复集群 | §5.2；`SnapshotFutureTask.java:282-410` | `snapshot/AbstractSnapshotSelfTest`（其 847 行即 `forceCheckpoint(String.format(CP_SNAPSHOT_REASON, ...))` 用法） | **⑤snapshot 课位**（章末，复用 7.5+7.7 全部概念） |

注：7.3 内含树+freelist 两个"子簇"，但它们互为生存依赖（树的行体在 freelist 页里），拆开无法独立验收，作为单一"页上数据结构"簇处理；若实测超 4h，优先把 **FreeList 回收/ReuseList 分支**延到 7.4 一起做（回收产生 RecycleRecord delta，天然衔接）。

**五个已定约束的落位核对**：①接缝替换=7.3+7.4+7.5 三课在接缝内换实现（PageMemory→真 WAL→真 checkpoint 锁），7.6 以恢复验证接缝无损；②TTL=7.8（`GridCacheSharedTtlCleanupManager` + pending 树 `PendingEntriesTree extends BPlusTree<PendingRow, PendingRow>`，`core/processors/cache/tree/PendingEntriesTree.java:32`——到期条目登记进独立树供后台扫描清除）；③eviction=7.8（on-heap/offheap 淘汰已有章 1/章 4 基础，本课做 page 级 `PageEvictionTracker` + data region 治理）；④historical rebalance=7.7 建概念后回填补课（WAL 增量供应依赖 `replay`+checkpoint 指针，`IgniteDhtDemandedPartitionsMap` 的 historical 集合即接口）；⑤snapshot=7.9。

**可简化项判断（默认全做，仅列真正可争议项）**：

1. **页替换策略三选一**：RANDOM_LRU/SEGMENTED_LRU/CLOCK（04 §1.1）只实现 RANDOM_LRU，接口与配置项保留——策略是 data region 内部正交件，验收测试不感知；
2. **PagesList on-heap bucket cache**：直接实现为关（`IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING` 在 vendor 中即合法常开路径，§2.1）；
3. **`CheckpointRecoveryFile`**：默认关闭（`DFLT_WRITE_RECOVERY_DATA_ON_CP=false`，§4.3），课程只走 WAL-only 恢复路径——vendor 默认路径一致；
4. **加密/压缩族**（ENCRYPTED_*、COMPACTED WAL、页压缩、TDE）：与 ADR 0001 范围核对后建议排除（编解码与密管自成子系统，且 serializer 侧是旁路包装）；
5. **增量 snapshot（WAL compaction 依赖）**：7.9 只做 full snapshot，增量列为课后可选（vendor 要求 `isWalCompactionEnabled()` 前置，§5.2）；
6. **索引树 sequential write 模式**（0.85 分裂偏移）：跳过，仅注释锚点（§1.4，appender 专用优化）。

其余（invoke 乐观栈、tag ABA 防护、direct/indirect item、checkpoint 标记双写、MemoryRecoveryRecord、BLT 状态机）都是行为保真的骨架，不建议简化。

**7.5/7.6 的验收注入矩阵**（由 §4 已核验的恢复分支反推，每格对应一条改写测试）：

| 注入点 | 期望恢复行为 | 依据 |
|---|---|---|
| checkpoint END 缺失（kill 在标记间） | `needRestoreMemory()=true` → 回放 START 之后全部物理记录 → `finalizeCheckpointOnRecovery` 补 END | §4.2/4.3 |
| kill 在 END 之后、WAL 有后续记录 | 只回放 END 后的逻辑记录，物理页即 checkpoint 状态 | `needRestoreMemory()=false`（`CheckpointStatus.java:73-75`） |
| kill 后二次重启（恢复未落 checkpoint） | 遇 `MemoryRecoveryRecord` 之后不重复应用旧物理记录 | §4.5、`CheckpointWorkflow.java:234-236` |
| WAL 归档段 CRC 损坏（标记前） | 启动失败（CRITICAL_ERROR），不静默截断 | §3.4、`throwsCRCError` |
| kill 于事务 PREPARED 后、COMMIT 前 | 数据页留痕但逻辑恢复跳过；跨节点由 recovery 决定提交/回滚 | §4.4 |
| 快照进行中 kill | metastore `SNP_RUNNING_DIR_KEY` 残留 → 下次启动清理后拒绝/续做 | §5.2 |

**与相邻章的接口**：章 6（事务）的 `TxRecord`/两阶段日志在 7.6 被消费（§4.4）；章 4（rebalance）的 historical 供应在 7.7 之后补课（约束④）；章 8（SQL）的 `InlineIndexTree` 直接复用本章 BPlusTree（§1.6），故 7.3 的验收里应包含一条"树可被第二用户（索引行）复用"的用例，避免把数据树特化写死。

---

## 引用文件清单（全部实际打开核验）

vendors/ignite/ 下（`modules/core/src/main/java/` 前缀省略为 `core/` = `org/apache/ignite/internal/`）：

1. `core/processors/cache/persistence/tree/BPlusTree.java`（类注释 96-212；214；231-235；428-481；820-910；1110-1170；2128-2276；2958-2983；3114；3324；3473；3623-3760；3835-3960；4321；4837-4847；5168；5399-5630；5822-5856；5833-5834；5886-5921）
2. `core/processors/cache/persistence/tree/util/PageHandler.java`（109-138；279-330；484-495）
3. `core/processors/cache/persistence/DataStructure.java`（275-287；443-466）
4. `core/processors/cache/persistence/pagemem/PageMemoryImpl.java`（506-516；1584-1599；1630-1660）
5. `core/util/OffheapReadWriteLock.java`（37-67；111-161）
6. `core/processors/cache/persistence/tree/io/BPlusMetaIO.java`（30-108）
7. `core/processors/cache/tree/CacheDataTree.java`（56-108）
8. `core/processors/cache/tree/CacheDataRowStore.java`（31-98）
9. `core/processors/cache/persistence/IndexStorage.java`（27-90）
10. `core/cache/query/index/sorted/inline/InlineIndexImpl.java`（64-180）
11. `core/cache/query/index/sorted/inline/InlineIndexTree.java`（76）
12. `core/processors/cache/persistence/freelist/PagesList.java`（79-134；661-741；1604）
13. `core/processors/cache/persistence/freelist/AbstractFreeList.java`（58-177；703-760；817-818；913-917）
14. `core/processors/cache/persistence/freelist/CacheFreeList.java`（35）
15. `core/processors/cache/persistence/tree/io/AbstractDataPageIO.java`（39-165；300-350）
16. `core/processors/cache/persistence/tree/io/DataPageIO.java`（51-103）
17. `core/processors/cache/persistence/tree/io/PagePartitionMetaIO.java`（28-123）
18. `core/processors/cache/persistence/wal/serializer/RecordV1Serializer.java`（100-305；308-470）
19. `core/processors/cache/persistence/wal/serializer/RecordDataV1Serializer.java`（580-695；1380-1449；2093-2168）
20. `core/processors/cache/persistence/wal/serializer/RecordSerializerFactoryImpl.java`（40-99）
21. `core/processors/cache/persistence/wal/FileWriteAheadLogManager.java`（745；771-775；988-1094；2737-2790；2839-2908）
21a. `core/processors/cache/persistence/wal/AbstractWalRecordsIterator.java`（75-89；176-234；242-300）
22. `core/processors/cache/persistence/GridCacheDatabaseSharedManager.java`（838-920；1000-1122；1885-1961；2030-2063；2071-2511；2560-2690；3531-3611）
23. `core/processors/cache/persistence/checkpoint/CheckpointManager.java`（267-285；380-388）
24. `core/processors/cache/persistence/checkpoint/CheckpointMarkersStorage.java`（160-376）
25. `core/processors/cache/persistence/checkpoint/CheckpointStatus.java`（28-81）
26. `core/processors/cache/persistence/checkpoint/Checkpointer.java`（1084-1091）
27. `core/processors/cache/persistence/checkpoint/CheckpointWorkflow.java`（223-254；705-708）
28. `core/processors/cache/persistence/GridCacheOffheapManager.java`（380-499；1860-1963；2005）
29. `core/IgniteKernal.java`（1080-1198）
30. `core/processors/cache/transactions/IgniteTxManager.java`（185-199；2326；2367；3586-3616）
31. `core/processors/cluster/GridClusterStateProcessor.java`（106-130；378-401；467-491；532-563；664-774；1024-1030；1106-1225；1580-1677）
32. `core/internal/cluster/IgniteClusterImpl.java`（425-505；531-541）
33. `core/processors/cache/persistence/snapshot/IgniteSnapshotManager.java`（242-321；436-437；767-917；976-1094；1941-2158；2285-2319；2540-2567；4149-4150）
34. `core/processors/cache/persistence/snapshot/SnapshotFutureTask.java`（128；250-410；660-726）
35. `core/processors/cache/persistence/filename/SnapshotFileTree.java`（44-72；169-198；281；370-371）
36. `core/processors/cache/persistence/filename/NodeFileTree.java`（184-189；522-534）
37. `modules/core/src/main/java/org/apache/ignite/configuration/DataStorageConfiguration.java`（188-194；376；1487-1488）
38. `core/processors/cache/GridCacheSharedTtlCleanupManager.java`（存在性核验）；`core/processors/cache/tree/PendingEntriesTree.java`（32）
39. `core/processors/cache/persistence/evict/`（`PageEvictionTracker`/`RandomLruPageEvictionTracker`/`FairFifoPageEvictionTracker`/`Random2LruPageEvictionTracker`/`NoOpPageEvictionTracker` 存在性核验）
40. `core/processors/cache/distributed/dht/preloader/IgniteDhtDemandedPartitionsMap.java`（44-83，historical 集合）

测试侧锚点（`modules/core/src/test/java/org/apache/ignite/internal/` 下，均为 §6 改写测试候选，存在性核验）：

41. `processors/database/BPlusTreeSelfTest.java`、`BPlusTreeReplaceRemoveRaceTest.java`、`CacheFreeListSelfTest.java`、`FreeListCutTailDifferentGcTest.java`
42. `processors/cache/persistence/pagemem/BPlusTreePageMemoryImplTest.java`、`ClockPageReplacementFlagsTest.java`
43. `processors/cache/persistence/db/file/IgnitePdsCacheIntegrationTest.java`
44. `processors/cache/persistence/db/wal/IgniteWalFormatFileFailoverTest.java`、`IgniteWalRecoverySeveralRestartsTest.java`、`IgniteWalReplayingAfterRestartTest.java`、`crc/`（目录）
45. `processors/cache/persistence/db/IgnitePdsCheckpointRecoveryTest.java`、`CheckpointBufferDeadlockTest.java`、`IgnitePdsPageEvictionTest.java`
46. `processors/cache/persistence/standbycluster/IgniteChangeGlobalStateAbstractTest.java`、`IgniteChangeGlobalStateTest.java`
47. `processors/cache/GridCacheTtlManagerSelfTest.java`
48. `processors/cache/persistence/snapshot/AbstractSnapshotSelfTest.java`（847 行：`forceCheckpoint(String.format(CP_SNAPSHOT_REASON, ...))`）

---

## 对复刻课的启示

1. **并发协议是章 7 的真正难点，不是文件 I/O**。BPlusTree 的正确性来自"自底向上加锁 + 三角不变量校验 + tag 防 ABA + RETRY/RETRY_ROOT 重试"四件套的配合（§1.1-1.2）。复刻顺序应先在纯内存下把树打绿（7.3），再挂 WAL——vendor 自己就是把"有没有 WAL"做成构造参数注入（`CacheDataTree.java:99`），这个接缝值得原样保留，它让树可以无盘测试。
2. **WAL delta 的"写页后 log"顺序不违反 WAL 原则**——真正的顺序约束在"页变脏先有整页快照，delta 只补差异；checkpoint 先 WAL 后页文件"这两层（§1.4）。课程讲解时必须把这点与教科书"先写日志后改页"区分开，否则学生会把 InsertRecord 挪到 io.insert 之前反而做错。
3. **恢复的正确性锚点全部是"位置"而非"内容"**：END 指针、checkpointMark、MemoryRecoveryRecord、next() 里遇到的 CheckpointRecord——没有一处靠比对页内容（§4）。复刻恢复器时按这条线串代码，验收用"kill 在 checkpoint 中途/后"两种注入即可覆盖主路径。
4. **快照是 checkpoint 的一个监听者，不是独立子系统**（§5.2）。把它排在 7.9 能最大化复用：学生此刻已同时拥有 checkpoint（7.5）、BLT（7.7）与 metastore 的全部概念。
5. **两处"实现演进留痕"可作教学反例**：SplitForwardPageRecord 弃用改整页记录（GG-11640 TODO）、CheckpointRecoveryFile 与默认关——说明"简单正确"与"复杂快"的取舍在真实代码里如何发生（§1.4、§4.3）。
6. **名字陷阱清单**（§0）直接写进 7.3 讲义：学生拿旧博客/旧版本类名（FreeListImpl、FileWALReader）去 vendor 里对表会扑空，锚点必须以 2.18 源码为准。
7. **作用域错位是持久层最常见的建模错误**：FreeList/ReuseList 是 **data region 级**（同 region 全分区共享一页池），树/元页/分区计数是**分区级**，索引页统一在 **INDEX_PARTITION 保留分区**（04 §1.1）——§2.5 的装配代码是这三层作用域的活教材。复刻时若把 FreeList 做成分区私有，行为几乎全对但空间利用率与回收语义都会偏离，且很难被功能测试发现（需要容量对比测试才能暴露）。
8. **页分配持 checkpoint 读锁是数据结构层的隐形契约**（§2.5 末条 `assert checkpointLockIsHeldByThread`）：树/freelist 的所有公开操作都发生在 PME 与 checkpoint 锁的夹缝内。复刻 7.3 时就应把该断言原样搬过来，让后续课的违规调用在测试期即暴露，而不是等 checkpoint 数据竞争。
