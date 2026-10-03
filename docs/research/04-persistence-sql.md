# 持久化与 SQL 组件构成（Apache Ignite 2.18.0 源码调研）

> Primary source：本仓库 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码 submodule，只读）。
> 文中所有源码路径均相对 `vendors/ignite/`，全部经实际打开/检索验证存在。类名、方法名、常量值以 2.18 源码为准。

---

## 第一部分：持久化（core 模块）

持久化相关代码集中在两个包：

- `modules/core/src/main/java/org/apache/ignite/internal/pagemem/`（page memory 抽象接口 + WAL/PageStore 接口）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/`（实现：`pagemem/`、`file/`、`wal/`、`checkpoint/`、`tree/`、`freelist/` 等子包）

### 1.1 Page Memory

**接口与实现**

- 接口：`org.apache.ignite.internal.pagemem.PageMemory`（`modules/core/src/main/java/org/apache/ignite/internal/pagemem/PageMemory.java`），继承 `PageIdAllocator` 与 `PageSupport`，核心方法：`start()` / `stop()` / `pageSize()` / `systemPageSize()` / `pageBuffer(long pageAddr)` / `loadedPages()` / `checkpointBufferPagesCount()`。
- 持久化实现：`org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl`（实现 `PageMemoryEx`）；纯内存（无 PageStore）实现为 `org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl`。
- 每个 `DataRegion`（`.../persistence/DataRegion.java`，字段 `private final PageMemory pageMem`）持有一个 PageMemory 实例；`GridCacheDatabaseSharedManager.createPageMemory(...)` 在 `regCfg.isPersistenceEnabled()` 时构造 `PageMemoryImpl`（`.../persistence/GridCacheDatabaseSharedManager.java:1205`），构造参数里还有 `flushDirtyPage` 回调（页被淘汰替换时直接写盘）与 throttling 策略。

**与 off-heap 的关系（segment 结构）**

`PageMemoryImpl` 通过 `DirectMemoryProvider` 分配堆外内存。`start()`（`PageMemoryImpl.java:362-424`）：

1. `directMemoryProvider.initialize(sizes)` 分配若干 `DirectMemoryRegion`；
2. **最后一个 region 作为 checkpoint buffer（`PagePool checkpointPool`）**，其余每个 region 对应一个 `Segment`，得到 `Segment[] segments`；
3. 每个 `Segment` 内含：页池（按 `sysPageSize = pageSize + PAGE_OVERHEAD` 切分成页）、`loadedPages`（页 ID → 偏移映射，`LoadedPagesMap`）、`dirtyPages`（`GridConcurrentHashSet<FullPageId>`）+ `dirtyPagesCntr`、页替换策略（`PageReplacementPolicy`，按 `DataRegionConfiguration.getPageReplacementMode()` 选择 RANDOM_LRU / SEGMENTED_LRU / CLOCK，见 `PageMemoryImpl.java:343-358`）。

页在内存中的地址即堆外绝对指针 `absPtr`，用户代码通过 `PageMemoryImpl.pageBuffer(pageAddr)` 拿 `ByteBuffer` 视图直接读写，无 on-heap 拷贝。

**page 布局与页头**

`PageMemoryImpl` 类注释（`PageMemoryImpl.java:106-129`）给出页头结构；已分配页：

```
| 8b Marker/Timestamp | 8b Relative ptr | 8b Page ID | 4b Cache group ID | 4b Pin count | 8b Lock | 8b Temp buffer ptr | PAGE_SIZE 数据 |
```

- `PAGE_OVERHEAD = 48` 字节（`PageMemoryImpl.java:153`），`PAGE_LOCK_OFFSET = 32`——页内读写锁（`OffheapReadWriteLock`，带 tag 校验）存在页头里；
- Temp buffer 指针用于 checkpoint 期间的 copy-on-write（见 1.4）。

**页分配**

`PageMemoryImpl.allocatePage(grpId, partId, flags)`（`PageMemoryImpl.java:550`）：

1. `pmPageMgr.allocatePage(...)`（`PageReadWriteManager` → 底层 `FilePageStore.allocatePage()` 递增已分配页数）拿到 pageId；
2. 从所属 `Segment` 借空闲页（`borrowOrAllocateFreePage`），不够则按替换策略淘汰（`removePageForReplacement`，会触发 `flushDirtyPage` 写盘）；
3. 清零页数据、写入 `FullPageId`、初始化页锁 tag，最后 `setDirty(fullId, absPtr, true, true)` 直接标脏（新分配页必须进下一次 checkpoint）。

**页 ID 与 page 类型**

- 页 ID 由 `PageIdUtils` 编码（partId / flag / pageIndex / tag）；分配 flag 定义在 `org.apache.ignite.internal.pagemem.PageIdAllocator`（`.../internal/pagemem/PageIdAllocator.java`）：`FLAG_DATA = 1`（数据页，也用于分区元数据页、tracking 页）、`FLAG_IDX = 2`（索引页，使用 page ID 轮换机制）、`FLAG_AUX = 4`（内部结构页）；索引页统一放在保留分区 `INDEX_PARTITION = 0xFFFF`。
- 页**内容**类型由 `PageIO` 的 type 字段区分（`.../persistence/tree/io/PageIO.java:149+`）：`T_DATA=1`（数据页）、`T_BPLUS_META=2`（B+树元数据页）、`T_H2_REF_LEAF/INNER=3/4` 与 `T_DATA_REF_LEAF/INNER=6/5`（索引叶子/内节点页）、`T_PAGE_LIST_META/NODE=12/13`（空闲页链表）、`T_PART_META=14`（分区元数据页）、`T_PAGE_UPDATE_TRACKING=15`（页更新跟踪页）、`T_PART_CNTRS=20`（分区计数器页）、`T_DATA_METASTORE=21` 系列（元存储）、`T_DATA_PART=32` 等。每页公共头 40 字节：`type(2)+ver(2)+crc(4)+pageId(8)+rotatedIdPart(1)+reserved`（`PageIO.COMMON_HEADER_END`，`PageIO.java:110-144`）。

### 1.2 PageStore：.bin 文件布局与读写

**文件级布局**

一个 `FilePageStore`（`.../persistence/file/FilePageStore.java`）对应一个分区文件或一个 cache group 的索引文件：

- 文件头 `HEADER_SIZE = 8(SIGNATURE 0xF19AC4FE60C530B8L) + 4(VERSION) + 1(type) + 4(pageSize)` 共 17 字节（`FilePageStore.java:69-76`）；
- 第 N 页偏移：`pageOffset(pageId) = pageIndex(pageId) * pageSize + HEADER_SIZE`（`FilePageStore.java:807-809`）；
- `write(pageId, pageBuf, tag, calculateCrc)`（`FilePageStore.java:703`）：先按 `FastCrc` 算 CRC 写入页头 CRC 字段，再 `fileIO.writeFully(pageBuf, off)` 顺序写；`sync()` 对文件 `force()`（fsync）。

**目录结构与命名**

`FilePageStoreManager`（`.../persistence/file/FilePageStoreManager.java`，实现 `IgnitePageStoreManager`）按 `(grpId, partId)` 解析 store（`getStore`），`read/write/sync` 委托给对应 `FilePageStore`。文件树由 `NodeFileTree`（`.../persistence/filename/NodeFileTree.java`）描述，其类注释（`NodeFileTree.java:87-174`）给出完整布局：

```
db/                                    ← DataStorageConfiguration.getStoragePath() 默认 ${IGNITE_WORK_DIR}/db
├── node00-<consistentId>/             ← 每节点目录（PdsFolderSettings.folderName()）
│   ├── cache-default/                 ← cache 名为 "default" 的 cacheStorage
│   │   ├── cache_data.dat             ← cache 配置文件
│   │   ├── index.bin                  ← 该 cache group 的索引分区文件（TYPE_IDX）
│   │   └── part-0.bin ... part-N.bin  ← 分区文件（TYPE_DATA）
│   ├── cacheGroup-tx-cache/           ← cache group 目录（组内多个 cache 共享 part-*.bin）
│   ├── cp/                            ← checkpoint 标记目录
│   │   ├── <ts>-<uuid>-START.bin
│   │   └── <ts>-<uuid>-END.bin
│   ├── metastorage/part-*.bin         ← 元存储
│   └── wal 相关见下节
└── wal/
    ├── <consistentId>/NNNNNNNNNNNNNNNN.wal      ← WAL 活跃 segment（16 位数字编号）
    └── archive/<consistentId>/...               ← WAL 归档
```

文件名常量：`FILE_SUFFIX=".bin"`、`PART_FILE_PREFIX="part-"`、`INDEX_FILE_NAME="index.bin"`（`NodeFileTree.java:201-245`）。

### 1.3 WAL（Write-Ahead Log）

**管理器**

接口 `org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager`；实现 `FileWriteAheadLogManager`（`.../persistence/wal/FileWriteAheadLogManager.java`）。

**segment 文件结构**

- 默认 10 个 segment、每个 64MB：`DFLT_WAL_SEGMENTS = 10`、`DFLT_WAL_SEGMENT_SIZE = 64 * 1024 * 1024`（`.../configuration/DataStorageConfiguration.java:132-135`）；
- segment 头是一条 `HEADER_RECORD`：record type 字节 + `WALPointer`（segment index）+ 8 字节 magic（`HeaderRecord.REGULAR_MAGIC` / `COMPACTED_MAGIC`）+ serializer 版本 int + CRC（`FileWriteAheadLogManager.prepareSerializerVersionBuffer`，`FileWriteAheadLogManager.java:2737-2768`）；
- 普通记录帧格式（`RecordV1Serializer`，`.../wal/serializer/RecordV1Serializer.java:178-191, 246-249`）：`record type(1B) + WAL 指针(segmentIdx 8B + 文件内偏移 4B) + 记录数据 + CRC`；segment 末尾不足放一条记录时写 `SWITCH_SEGMENT_RECORD` 单字节标记（`WALRecord.RecordType` 注释，`WALRecord.java:165-172`）。

**record 类型（`org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType`，`WALRecord.java:41+`）**

按用途分三类（枚举第二个参数 `LOGICAL` / `PHYSICAL` / `MIXED` / `INTERNAL`）：

- **逻辑记录（LOGICAL，面向缓存条目）**：`TX_RECORD(0)`、`DATA_RECORD_V2(70)`（当前数据写入记录，载体 `DataRecord`/`DataEntry`）、`ROLLBACK_TX_RECORD(57)`、`METASTORE_DATA_RECORD(45)`、`ENCRYPTED_DATA_RECORD_V3(71)` 等；
- **物理 delta 记录（PHYSICAL，面向页变更）**：`PAGE_RECORD(1)`（整页快照 `PageSnapshot`）、`INIT_NEW_PAGE_RECORD(5)`、`DATA_PAGE_INSERT_RECORD(6)` / `DATA_PAGE_REMOVE_RECORD(8)` / `DATA_PAGE_UPDATE_RECORD(41)`、`BTREE_PAGE_INSERT(15)` / `BTREE_FORWARD_PAGE_SPLIT(22)` / `BTREE_EXISTING_PAGE_SPLIT(23)` / `BTREE_PAGE_MERGE(24)` 等 B+树 delta、`PAGES_LIST_*`（25-29，空闲链表）、`PARTITION_META_PAGE_UPDATE_COUNTERS(31)`、`TRACKING_PAGE_DELTA(33)`、`META_PAGE_UPDATE_*` 系列、`CHECKPOINT_RECORD(3)`（checkpoint 标记）；
- **快照/运维类**：`SNAPSHOT(44)`、`CLUSTER_SNAPSHOT(75)`、`PARTITION_DESTROY(43)`、`EXCHANGE(46)`、`MEMORY_RECOVERY(32)` 等。

**写入时机**

1. **缓存条目更新（逻辑）**：`GridCacheMapEntry.logDataUpdate(...)` 调 `cctx.group().wal().log(new DataRecord(new DataEntry(...)))`（`.../processors/cache/GridCacheMapEntry.java:3472`；事务路径在 `GridDistributedTxRemoteAdapter` 中批量 log，同文件模式）。
2. **页内结构修改（物理 delta）**：数据结构修改页时逐条写 delta，例如 `BPlusTree` 分裂页时 `wal.log(new SplitExistingPageRecord(grpId, pageId, mid, fwdId))`（`.../persistence/tree/BPlusTree.java:2980`；同文件还有 `InsertRecord`、`MetaPageAddRootRecord` 等）。
3. **首次变脏整页快照**：`PageMemoryImpl.writeUnlockPage(...)` → `beforeReleaseWrite(...)`：当页在本 checkpoint 周期内第一次被改脏（`pageWalRec = markDirty && !wasDirty`）或配置 `isAlwaysWriteFullPages()` 时，`walMgr.log(new PageSnapshot(pageId, ptr, pageSize, realPageSize))`（`PageMemoryImpl.java:1892-1898`）。
4. **写入机制**：`FileWriteAheadLogManager.log(rec)`（`FileWriteAheadLogManager.java:871`）→ 当前 `FileWriteHandle.addRecord(rec)`（实现 `FileWriteHandleImpl`，`.../wal/filehandle/FileWriteHandleImpl.java:222-286`）：记录先进 `SegmentedRingByteBuffer`，由后台 `WALWriter` 线程刷到 segment 文件（mmap 或 FileIO）；`WALMode`（默认 `LOG_ONLY`，另有 `BACKGROUND`、`FSYNC`）控制 fsync 时机；`BACKGROUND` 模式下 `CheckpointRecord` 强制 `flushOrWait`。
5. **rollover**：segment 写满（或 `walForceArchiveTimeout`）时切换下一个 segment 并归档；若距上次 checkpoint 已跨过太多 segment（`next.getSegmentId() - lastCheckpointPtr.index() >= maxSegCountWithoutCheckpoint`）则 `cctx.database().forceCheckpoint("too big size of WAL without checkpoint")`（`FileWriteAheadLogManager.java:1343-1344`）——这是 WAL 反压 checkpoint 的关键机制。

### 1.4 Checkpoint

**线程模型与触发条件**

- `Checkpointer extends GridWorker`（`.../checkpoint/Checkpointer.java:102`），`body()` 循环：`waitCheckpointEvent()`（按 `scheduledCp.nextCpNanos` 周期唤醒，默认 `DFLT_CHECKPOINT_FREQ = 180000`ms = 3 分钟，`DataStorageConfiguration.java:104`，并带随机偏移防集群同步，`Checkpointer.nextCheckpointInterval()`）→ `doCheckpoint()`（`Checkpointer.java:261-294`）。
- 周期之外的强制触发：`scheduleCheckpoint(delayFromNow, reason)` 被 WAL 段数超限（上节）、节点停止、索引重建（`RebuildIndexAction`）、defragmentation、WAL 状态切换等调用。

**一次 checkpoint 的工作流程（以 `doCheckpoint()` + `CheckpointWorkflow` 为准）**

1. **标记开始**：`CheckpointWorkflow.markCheckpointBegin(...)`（`.../checkpoint/CheckpointWorkflow.java:223-352`）：
   - 先持 checkpoint 读锁跑 `beforeCheckpointBegin` 监听器；
   - **取 checkpoint 写锁**（`checkpointReadWriteLock.writeLock()`，即 `CheckpointTimeoutLock`，短暂阻塞所有页面写者）；状态 `LOCK_TAKEN`；
   - 对每个持久化 `DataRegion` 调 `PageMemoryImpl.beginCheckpoint(allowToReplace)`（`PageMemoryImpl.java:1128-1158`）：把每个 `Segment.dirtyPages` 快照为 `CheckpointPages`，随后 `resetDirtyPages()` 换新集合——**脏页快照**完成；
   - 若有脏页：`wal.log(cpRec)` 写 `CheckpointRecord`（含各 cache group 分区状态 `fillCacheGroupState`），随后**在锁外** `wal.flush(cpPtr, true)` 强制 fsync——**WAL 标记**；
   - 释放写锁，`splitAndSortCpPagesIfNeeded` 按 `groupId + effectivePageId` 排序（`CheckpointWriteOrder.SEQUENTIAL` 时顺序写盘）。
2. **START 标记落盘**：`storeBeginMarker(chp)` → `CheckpointMarkersStorage.writeCheckpointEntry(..., CheckpointEntryType.START, ...)`，以「写 `.tmp` → fsync → 原子 rename」两阶段协议写 `cp/<ts>-<uuid>-START.bin`（`CheckpointMarkersStorage.java:460-494`）。
3. **页落盘**：`Checkpointer.writePages(...)`（`Checkpointer.java:606-687`）用 `checkpointWritePageThreads`（默认 `DFLT_CHECKPOINT_THREADS = 4`，`DataStorageConfiguration.java:120`）个 `CheckpointPagesWriter` 线程消费脏页队列：`pageMem.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, ...)` 拷出页内容，`PageStoreWriter.writePage` → `PageStore.write(...)`（`CheckpointPagesWriter.java:166-258`）；写完对本次触达的每个 store `sync()`（fsync）。
4. **完成标记**：`CheckpointWorkflow.markCheckpointEnd(chp)`（`CheckpointWorkflow.java:559-599`）：各 PageMemory `finishCheckpoint()` 清理 `checkpointPages`；写 `cp/<ts>-<uuid>-END.bin`；`wal.notchLastCheckpointPtr(checkpointMark)`（此后更早的 WAL segment 才可归档删除）；跑 `afterCheckpointEnd` 监听器。

**与 page memory 的并发协作（copy-on-write）**

checkpoint 快照后如果业务线程又改某页：`PageMemoryImpl.postWriteLockPage`（`PageMemoryImpl.java:1655-1697`）发现该页 `isInCheckpoint(fullId)` 且无 temp buffer，就从 `checkpointPool` 借一页把当前内容拷过去，把原页 dirty 标志清掉、记录 temp 指针——checkpoint 写的是冻结的临时副本，业务继续改原页，改动进入下一轮脏页集合。checkpoint buffer 不足时触发写限流（`ThrottlingPolicy`：`CHECKPOINT_BUFFER_ONLY` / `SPEED_BASED` / `TARGET_RATIO_BASED`，`PageMemoryImpl.initWriteThrottle`）。

### 1.5 一次写入的完整交互时序（put → page memory → WAL → checkpoint）

1. 业务线程更新缓存条目，全程持有 `cctx.shared().database().checkpointReadLock()`（`GridCacheMapEntry.java:460/498`）——保证不在 checkpoint 写锁期间改页。
2. **WAL 逻辑记录**：`wal.log(new DataRecord(new DataEntry(...)))` 记录 key/value/op（`GridCacheMapEntry.java:3472`），经 `FileWriteAheadLogManager.log` 进入 ring buffer、后台刷入 segment 文件。
3. **页内存修改 + 物理 WAL**：数据写入 `DataPageIO` 数据页、索引更新走 B+树（索引页在 `INDEX_PARTITION`）；树结构在修改页过程中逐条写物理 delta（如 `BPlusTree.java:2980` 的 `SplitExistingPageRecord`）；`writeUnlockPage` 时 `setDirty` 把 `FullPageId` 加进 `Segment.dirtyPages`，若为本周期首次变脏则补一条整页 `PageSnapshot`（`PageMemoryImpl.java:1720-1723, 1892-1898`）。
4. **checkpoint 触发**（周期到 / WAL 段数超限强制）：取 checkpoint 写锁 → `beginCheckpoint` 快照脏页集合并重置 → `wal.log(CheckpointRecord)` + fsync → 写 `START.bin` 标记 → 释放写锁。
5. **页落盘**：多个 writer 线程把快照页逐页写入 `FilePageStore`（`part-N.bin` / `index.bin`，按 pageOffset 定位、带 CRC），全部写完逐 store fsync，写 `END.bin` 标记并推进 `lastCheckpointPtr`。
6. **重启恢复**（时序闭环）：`GridCacheDatabaseSharedManager` 读最后一次成功 checkpoint 的 `END` 标记 → 页文件加载进 page memory → 从 checkpoint 的 WAL 指针开始 `cctx.wal().replay(...)` 重放记录（`GridCacheDatabaseSharedManager.java:2231` 附近），重放前先写 `MemoryRecoveryRecord` 防止旧物理记录被重复回放（`GridCacheDatabaseSharedManager.java:1104`）。

---

## 第二部分：SQL（modules/indexing）

### 2.1 H2 集成

**版本证据**

`modules/indexing/pom.xml`（第 68-70 行）声明依赖：

```xml
<groupId>com.h2database</groupId>
<artifactId>h2</artifactId>
<version>${h2.version}</version>
```

版本属性定义在根 parent POM：`parent/pom.xml:78` → `<h2.version>1.4.197</h2.version>`。即 2.18 的 indexing 模块基于 H2 **1.4.197**（配合 `org.apache.ignite.internal.processors.query.h2.opt` 等补丁类使用，见 `modules/indexing/pom.xml` 中关于 split package 的说明）。

**组件装配**

`IgniteH2Indexing`（`modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/IgniteH2Indexing.java`，实现 `GridQueryIndexing`）通过可选组件机制加载：`org.apache.ignite.internal.IgniteComponentType.INDEXING`（`modules/core/src/main/java/org/apache/ignite/internal/IgniteComponentType.java:72-78`，classPath 有 `ignite-indexing` 时实例化 `IgniteH2Indexing`）。其 `start()`（`IgniteH2Indexing.java:1532-1622`）创建：`ConnectionManager`（H2 连接/语句缓存）、`QueryParser`、`H2SchemaManager`、`GridMapQueryExecutor` + `GridReduceQueryExecutor`（两阶段执行器，监听 `GridTopic.TOPIC_QUERY`）、`CommandProcessor`（DDL）、`FunctionsManager`。

**schema / 表 / 索引的建立（与 cache 的映射）**

- schema：`H2SchemaManager.onSchemaCreated` 执行 `CREATE SCHEMA IF NOT EXISTS ...`（`H2SchemaManager.java:108-119`）。
- 表：cache 的 SQL 类型（`GridQueryTypeDescriptor`，来自 `QueryEntity`）注册时回调 `onSqlTypeCreated`（`H2SchemaManager.java:134-151`）→ `H2Utils.tableCreateSql` 生成形如 `CREATE TABLE sch.tbl (_KEY <type> NOT NULL, _VAL <type>, f1 <type>, ...)` 的 DDL（`H2Utils.java:166-203`）→ `H2TableEngine.createTable` 在 SQL 末尾附加 `engine "org.apache.ignite.internal.processors.query.h2.H2TableEngine"` 并执行，H2 回调 `createTable(CreateTableData)` 构造 **`GridH2Table`**（`H2TableEngine.java:57-89`）。
- `GridH2Table extends org.h2.table.TableBase`（`.../h2/opt/GridH2Table.java:84`）：持有 `GridH2RowDescriptor`、`H2TableDescriptor` 与 `GridCacheContextInfo`——这就是「H2 表 ↔ cache」的映射层；构造时计算 affinity key 列（`_KEY` 或显式 affinity 字段，用于分区裁剪与分布式 join，`GridH2Table.java:254-274`），创建 hash/text 系统索引，`addSystemIndex` 把 SCAN 索引放 0 位并注册主键索引（`GridH2Table.java:229-247`）。
- 行映射：`GridH2RowDescriptor`（`.../h2/opt/GridH2RowDescriptor.java:31-80`）把 key/value Java 类型映射为 H2 `DataType`，暴露 `GridQueryTypeDescriptor`（字段名、精度、notNull 等）。
- 二级索引：`H2IndexFactory.createIndex`（`.../h2/H2IndexFactory.java:56+`）在服务节点创建 `H2TreeIndex`（客户端为 `H2TreeClientIndex`）；`H2TreeIndex`（`.../h2/database/H2TreeIndex.java:110-160`）包装 core 模块的 `InlineIndexImpl`（`modules/core/src/main/java/org/apache/ignite/internal/cache/query/index/sorted/inline/InlineIndexImpl.java`），后者经 `IndexStorageImpl`（`.../persistence/IndexStorageImpl.java:48`，内部 `MetaTree extends BPlusTree`）把索引数据存为 **PageMemory 里的 B+树页**（`FLAG_IDX` / `INDEX_PARTITION` / `T_H2_REF_*` 页类型）——即 SQL 索引与数据共用同一套 page memory + 持久化 + WAL + checkpoint 体系。

### 2.2 分布式查询（map-reduce 两阶段）

**入口链路**

1. `IgniteCache.query(SqlQuery)` / `query(SqlFieldsQuery)` → `IgniteCacheProxyImpl.query(...)` 分发到 `ctx.kernalContext().query().querySql(...)` / `querySqlFields(...)`（`modules/core/src/main/java/org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java:793-803`）。
2. `GridQueryProcessor.querySql` 先把 `SqlQuery` 转成 `SqlFieldsQuery`（`idx.generateFieldsQuery`，`GridQueryProcessor.java:3383-3416`）；`querySqlFields` 经 `engineForQuery` 选择引擎（见 2.3），H2 引擎路径落到 `IgniteH2Indexing.querySqlFields`（`GridQueryProcessor.java:3056-3130`）。
3. `QueryParser.parse`（`.../h2/QueryParser.java:448-497`）：先用 H2 prepare，再用 `GridSqlQueryParser` 转 AST；若需要切分（`splitNeeded = !loc || locSplit`）则调 `GridSqlQuerySplitter.split(...)` 产出 **`GridCacheTwoStepQuery`**（map 查询列表 + reduce 查询）。
4. 切分算法（`.../h2/sql/GridSqlQuerySplitter.java:1151-1199` 的 `splitSelect`）：把聚合/分组/HAVING 表达式拆成 map 侧与 reduce 侧两半——map 查询保留 `GROUP BY` 与部分聚合列，reduce 查询 `SELECT ... FROM tableN`（`mergeTable(splitId)` 生成的合并表，`GridSqlQuerySplitter.java:170-179`）做最终聚合。
5. `IgniteH2Indexing.executeSelect0`：`splitNeeded` 为真走 `executeSelectDistributed` → `rdcQryExec.query(...)`（`IgniteH2Indexing.java:1255-1269, 1397-1458`）；否则本地直查。

**reduce 阶段（发起节点）：`GridReduceQueryExecutor`**

`GridReduceQueryExecutor.query(...)`（`.../h2/twostep/GridReduceQueryExecutor.java:343-545`）：

1. `createMapping` 按亲和（分区 → 节点）选出参与节点；
2. `createReduceQueryRun`（`GridReduceQueryExecutor.java:774-835`）：为每个 map 查询建 **`ReduceQueryRun`**（`.../h2/twostep/ReduceQueryRun.java:36`，持有 `List<Reducer>`）；非 skipMergeTbl 时 `createMergeTable` 在本地 H2 连接里注册 **`ReduceTable`**（H2 `TableBase`，带 `MERGE_INDEX_SORTED` / `MERGE_INDEX_UNSORTED` 两种 reduce 索引适配器，`GridReduceQueryExecutor.java:1313-1337`），并把它挂到 `table0..N` 假表名下供 reduce SQL 引用；
3. 组装 `GridH2QueryRequest`（map 查询、参数、分区映射、topology 版本）经 `GridTopic.TOPIC_QUERY` 发给所有数据节点，`awaitAllReplies` 等待；
4. 结果到齐后在本地 H2 上执行 reduce 查询 `SELECT ... FROM tableN ...`（`conn.prepareStatementNoCache(rdc.query())` + `h2.executeSqlQueryWithTimer`，`GridReduceQueryExecutor.java:512-542`），游标惰性拉取。

**map 阶段（数据节点）：`GridMapQueryExecutor`**

`GridMapQueryExecutor.onQueryRequest(...)`（`.../h2/twostep/GridMapQueryExecutor.java:206+ → 332-451`）：

1. `partitionReservationManager().reservePartitions(...)` 预约本节点负责的分区；
2. 为每个 `GridCacheSqlQuery` 建本地 H2 连接、设置 `QueryContext`（backup 过滤、分布式 join 上下文），`h2.executeSqlQueryWithTimer(...)` 在**本地 H2 实例**上对 `GridH2Table` 执行 map 查询（`GridMapQueryExecutor.java:488`）；
3. 结果封装为 `MapQueryResult` / `MapQueryResults`，按 `pageSize` 分页以 `GridQueryNextPageResponse` 回发 reducer；reducer 侧 `onNextPage` 把页喂给 `Reducer`（排序归并或无序堆叠，`GridReduceQueryExecutor.java:249-283`），需要下一页时发 `GridQueryNextPageRequest`（`GridMapQueryExecutor.java:868+`）——这就是两阶段之间的流式回传。

### 2.3 calcite 模块与 indexing（H2）的关系（简述）

2.18 引入了基于 Apache Calcite 的新查询引擎，与 H2 引擎并存、按配置/提示选择：

- calcite 模块：`modules/calcite/`，核心类 `CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine`（`modules/calcite/src/main/java/org/apache/ignite/internal/processors/query/calcite/CalciteQueryProcessor.java:135`），同样经 `IgniteComponentType` 可选组件注册（`IgniteComponentType.java:118`）。
- 配置开关：`CalciteQueryEngineConfiguration`（`modules/calcite/src/main/java/org/apache/ignite/calcite/CalciteQueryEngineConfiguration.java:27-66`，`ENGINE_NAME = "calcite"`，`setDefault(true)` 设为默认引擎）通过 `SqlConfiguration.setQueryEnginesConfiguration(...)` 注入；`GridQueryProcessor.initQueryEngines()`（`GridQueryProcessor.java:574-654`）约定：**未配置任何引擎时 H2（indexing）为默认**，配置了则可指定默认引擎，每个引擎至多一个实例。
- 单查询覆盖：`engineForQuery`（`GridQueryProcessor.java:3236-3268`）依次看 SQL hint（`QRY_ENGINE_PATTERN`）、client context 的 `queryEngine()`，否则用默认引擎。H2 引擎对应标记接口 `IndexingQueryEngine`（`modules/core/src/main/java/org/apache/ignite/internal/processors/query/IndexingQueryEngine.java`，`ENGINE_NAME = "h2"` 的配置类为 `modules/indexing/.../ignite/indexing/IndexingQueryEngineConfiguration.java`）。

---

## 第三部分：对复刻课的组件清单小结

持久化主线（自底向上）：

| # | 组件 | 2.18 源码锚点 | 复刻要点 |
|---|------|--------------|---------|
| 1 | PageMemory 抽象 | `internal/pagemem/PageMemory.java` | 页大小、allocate/acquire/release、dirty 语义 |
| 2 | PageMemoryImpl（segment + off-heap + 页头 + 脏页集） | `.../persistence/pagemem/PageMemoryImpl.java` | Segment 分片、48B 页头、CLOCK/LRU 替换、写锁标脏 |
| 3 | 页 ID / 页类型 | `internal/pagemem/PageIdAllocator.java`、`.../tree/io/PageIO.java` | FLAG_DATA/IDX/AUX、T_* 页类型、CRC 页头 |
| 4 | PageStore 文件层 | `.../persistence/file/FilePageStore.java`、`FilePageStoreManager.java`、`filename/NodeFileTree.java` | 17B 文件头、pageOffset 定位、part-N.bin/index.bin 布局 |
| 5 | WAL | `.../persistence/wal/FileWriteAheadLogManager.java`、`WALRecord.java`、`wal/filehandle/FileWriteHandleImpl.java`、`wal/serializer/RecordV1Serializer.java` | segment 64MB、逻辑/物理/CHECKPOINT 三类记录、ring buffer 异步刷盘、rollover |
| 6 | Checkpoint | `.../checkpoint/Checkpointer.java`、`CheckpointWorkflow.java`、`CheckpointPagesWriter.java`、`CheckpointMarkersStorage.java` | 脏页快照 → WAL 标记 + fsync → START 标记 → 并行写页 + fsync → END 标记；copy-on-write 临时页 |
| 7 | 恢复 | `.../persistence/GridCacheDatabaseSharedManager.java` | END 标记定位 + WAL replay + MemoryRecoveryRecord |

SQL 主线：

| # | 组件 | 2.18 源码锚点 | 复刻要点 |
|---|------|--------------|---------|
| 8 | H2 内嵌引擎 | `modules/indexing/pom.xml`、`parent/pom.xml` | H2 1.4.197 |
| 9 | IgniteH2Indexing 装配 | `.../h2/IgniteH2Indexing.java`、`IgniteComponentType.java` | 连接管理、parser、schema 管理、两个 executor |
| 10 | schema/表/索引映射 | `H2SchemaManager.java`、`H2Utils.java`、`H2TableEngine.java`、`opt/GridH2Table.java`、`opt/GridH2RowDescriptor.java` | `CREATE TABLE ... engine`、_KEY/_VAL 列、affinity 列 |
| 11 | 持久化 SQL 索引 | `H2IndexFactory.java`、`database/H2TreeIndex.java`、core 的 `InlineIndexImpl.java`、`IndexStorageImpl.java` | 索引 = PageMemory 上的 B+树，复用持久化栈 |
| 12 | 查询切分 | `QueryParser.java`、`sql/GridSqlQuerySplitter.java` | AST 拆 map/reduce，merge table |
| 13 | 两阶段执行 | `twostep/GridReduceQueryExecutor.java`、`twostep/GridMapQueryExecutor.java`、`twostep/ReduceQueryRun.java`、`ReduceTable` | map 本地 H2 查询 + 分页回传，reduce 端 merge table 归并 |
| 14 | 引擎选择（H2/Calcite） | `GridQueryProcessor.java`、`IndexingQueryEngine.java`、`CalciteQueryEngineConfiguration.java`、`CalciteQueryProcessor.java` | 多引擎配置 + hint 覆盖，默认 H2 |

一句话主干：**写入 = checkpointReadLock 下「WAL 逻辑/物理记录 + page memory 标脏」，checkpoint 周期性以脏页快照 + WAL 标记 + 并行刷盘把 page memory 落成 .bin 页文件；查询 = H2（或 Calcite）解析切分成 map/reduce 两步，map 在各数据节点本地 H2 查 `GridH2Table`（索引本身就是 PageMemory 里的 B+树），reduce 在发起节点用 merge table 归并。**

---

## 引用文件清单（均相对 `vendors/ignite/`）

core 模块：

1. `modules/core/src/main/java/org/apache/ignite/internal/pagemem/PageMemory.java`
2. `modules/core/src/main/java/org/apache/ignite/internal/pagemem/PageIdAllocator.java`
3. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/pagemem/PageMemoryImpl.java`
4. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/DataRegion.java`
5. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/GridCacheDatabaseSharedManager.java`
6. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/tree/io/PageIO.java`
7. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/file/FilePageStore.java`
8. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/file/FilePageStoreManager.java`
9. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/filename/NodeFileTree.java`
10. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/wal/FileWriteAheadLogManager.java`
11. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/wal/filehandle/FileWriteHandleImpl.java`
12. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/wal/serializer/RecordV1Serializer.java`
13. `modules/core/src/main/java/org/apache/ignite/internal/pagemem/wal/record/WALRecord.java`
14. `modules/core/src/main/java/org/apache/ignite/configuration/DataStorageConfiguration.java`
15. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/checkpoint/Checkpointer.java`
16. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/checkpoint/CheckpointWorkflow.java`
17. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/checkpoint/CheckpointPagesWriter.java`
18. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/checkpoint/CheckpointMarkersStorage.java`
19. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheMapEntry.java`
20. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/tree/BPlusTree.java`
21. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/IndexStorageImpl.java`
22. `modules/core/src/main/java/org/apache/ignite/internal/cache/query/index/sorted/inline/InlineIndexImpl.java`
23. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/GridQueryProcessor.java`
24. `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java`
25. `modules/core/src/main/java/org/apache/ignite/internal/processors/query/IndexingQueryEngine.java`
26. `modules/core/src/main/java/org/apache/ignite/internal/IgniteComponentType.java`

indexing 模块：

27. `modules/indexing/pom.xml`
28. `parent/pom.xml`（H2 版本属性）
29. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/IgniteH2Indexing.java`
30. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2SchemaManager.java`
31. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2Utils.java`
32. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2TableEngine.java`
33. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/GridH2Table.java`
34. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/opt/GridH2RowDescriptor.java`
35. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/H2IndexFactory.java`
36. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/database/H2TreeIndex.java`
37. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/QueryParser.java`
38. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/sql/GridSqlQuerySplitter.java`
39. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/GridReduceQueryExecutor.java`
40. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/GridMapQueryExecutor.java`
41. `modules/indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/ReduceQueryRun.java`

calcite 模块：

42. `modules/calcite/src/main/java/org/apache/ignite/calcite/CalciteQueryEngineConfiguration.java`
43. `modules/calcite/src/main/java/org/apache/ignite/internal/processors/query/calcite/CalciteQueryProcessor.java`
