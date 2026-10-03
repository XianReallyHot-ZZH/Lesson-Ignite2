# Ignite 启动路径研究：从 `Ignition.start()` 到 `IgniteKernal` 完成启动

> Primary source：本仓库 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码 submodule，只读）。
> 下文所有路径均相对 `vendors/ignite/`，行号以当前 submodule 检出内容为准。所有引用均经逐文件打开/ grep 验证。

## 1. 流程总览（有序步骤）

1. **`Ignition.start(...)`**：5 个公开重载（无参 / `IgniteConfiguration` / Spring 路径 / URL / InputStream），统一委托给 `IgnitionEx.start(...)`，返回前可能用 `SandboxIgniteComponentProxy` 包装（Ignite Sandbox 开启时）。
2. **`IgnitionEx.start0(GridStartContext, failIfStarted)`**：按 igniteInstanceName 在静态注册表（`grids` map / `dfltGrid`）登记 `IgniteNamedInstance`，处理重名/并发启动/已启动语义，然后调用 `grid.start(startCtx)`。
3. **`IgniteNamedInstance.start` → `initializeConfiguration`**：拷贝配置、解析 IGNITE_HOME/工作目录、**生成 nodeId（`UUID.randomUUID()` 或用户指定）**、初始化日志代理、补默认 SPI（`TcpDiscoverySpi` 等）、补默认 cache 配置（utility cache）。
4. **`new IgniteKernal(springCtx)` → `grid0.start(cfg, errHnd, workerRegistry, oomeHnd, startTimer)`**：核心生命周期，内部依次为：
   a. Gateway 状态机 `STOPPED → STARTING`；
   b. 组装 `GridKernalContextImpl`；
   c. 按固定顺序 `startProcessor(...)` / `startManager(...)` 启动全部 processor 与 manager（每个 manager 启动其管理的 SPI）；
   d. `fillNodeAttributes` 填充节点属性；
   e. 持久化内存恢复（`startMemoryRestore`）；
   f. **最后** `startManager(discoMgr)` 启动 discovery manager → `DiscoverySpi.spiStart` → 加入环形拓扑；
   g. 对所有组件回调 `onKernalStart(active)`；
   h. 生命周期 bean 收到 `AFTER_NODE_START`。
5. **启动完成**：`IgniteKernal` 内 `gw.setState(STARTED)`（在 join 拓扑前）；回到 `IgnitionEx` 后 `state = STARTED`、注册 JVM shutdown hook、`notifyStateChange(name, STARTED)` 通知 `IgnitionListener`；`Ignition.start()` 返回该实例。

主链路一句话：`Ignition.start` → `IgnitionEx.start0`（注册表）→ `IgniteNamedInstance.start0`（配置定稿）→ `IgniteKernal.start`（context 组装 → processor/manager/SPI 顺序启动 → 属性填充 → 持久化恢复 → discovery 最后启动加入环 → `onKernalStart` 回调）→ 状态置 STARTED。

---

## 2. 入口层：`Ignition` 与 `IgnitionEx`

### 2.1 `Ignition.start()` 的重载

`modules/core/src/main/java/org/apache/ignite/Ignition.java:280`（`start()`）、`:298`（`start(IgniteConfiguration)`）、`:323`（`start(String springCfgPath)`）、`:348`（`start(URL)`）、`:373`（`start(InputStream)`）、`:389`（`getOrStart(cfg)`）。全部形如：

```java
return wrapToProxyIfNeeded(IgnitionEx.start(cfg));
```

`wrapToProxyIfNeeded`（`Ignition.java:548`）仅在 Ignite Sandbox 环境内用 `SandboxIgniteComponentProxy.igniteProxy(ignite)` 包装返回值（`Ignition.java:538-542`）；普通场景原样返回 `IgniteKernal` 实例（其实现 `IgniteEx`，`IgniteKernal.java:348`）。

### 2.2 `IgnitionEx` 入口与配置解析

- 直接以配置对象启动：`IgnitionEx.start(IgniteConfiguration, GridSpringResourceContext, boolean)`（`modules/core/src/main/java/org/apache/ignite/internal/IgnitionEx.java:614`）→ `start0(new GridStartContext(...), failIfStarted)`。
- Spring 路径/URL/流启动：如 `IgnitionEx.start(URL, ...)`（`IgnitionEx.java:870`）先 `loadConfigurations(springCfgUrl)` 解析出 `Collection<IgniteConfiguration>`，再进入 `startConfigurations`（`IgnitionEx.java:959`）——对每个配置 bean 调 `start0`；中途失败会停止已启动的所有实例（`IgnitionEx.java:988-999`）。一个 Spring 文件可含多个 grid 定义，返回第一个。

### 2.3 grid 实例注册表与并发语义

`IgnitionEx.start0`（`IgnitionEx.java:1018-1119`）：

- 命名实例存入静态 `grids` map，默认实例存 `dfltGrid`（`IgnitionEx.java:1029-1042`）；
- 已存在且 `failIfStarted` 为真 → 抛 "already been started"；`getOrStart` 语义下返回旧实例 `T2<>(old, false)`（`IgnitionEx.java:1066-1074`）；
- `warmupClosure` 先于启动执行（`IgnitionEx.java:1076-1077`）；
- `grid.start(startCtx)` 成功后 `notifyStateChange(name, STARTED)`（`IgnitionEx.java:1096`），失败则从注册表摘除（`IgnitionEx.java:1100-1113`）。

`notifyStateChange`（`IgnitionEx.java:1393-1401`）更新 `gridStates`/`dfltGridState` 并通知所有 `IgnitionListener`——这是"实例级 started"对外信号。判断 kernal 是否完全启动可用 `IgnitionEx.hasKernalStarted(name)`（`IgnitionEx.java:1426`，检查 `startLatch` 已计数到零）。

### 2.4 `IgniteNamedInstance.start`：配置定稿

`IgniteNamedInstance`（`IgnitionEx.java:1514`）的 `start(GridStartContext)`（`IgnitionEx.java:1632`）用 `startGuard` CAS 防重入，随后：

1. `initializeConfiguration`（`IgnitionEx.java:1795-1940`）：
   - **node id**：`cfg.getNodeId() != null ? cfg.getNodeId() : UUID.randomUUID()`（`IgnitionEx.java:1822-1824`）——node id 在进入 `IgniteKernal` 之前就定稿；
   - `consistentId`（可被 `IGNITE_OVERRIDE_CONSISTENT_ID` 覆盖，`IgnitionEx.java:1826-1829`）、工作目录（`U.workDirectory`）、`GridLoggerProxy` 日志代理（`IgnitionEx.java:1831-1840`）；
   - **默认 SPI 注入** `initializeDefaultSpi`（`IgnitionEx.java:2026-2090`）：Discovery 缺省 `TcpDiscoverySpi`（IP finder 缺省 `TcpDiscoveryMulticastIpFinder`）、Communication 缺省 `TcpCommunicationSpi`、Deployment 缺省 `LocalDeploymentSpi`、EventStorage 缺省 `NoopEventStorageSpi`、Checkpoint 缺省 `NoopCheckpointSpi`、Collision 缺省 `NoopCollisionSpi`、Failover 缺省 `AlwaysFailoverSpi`、LoadBalancing 缺省 `RoundRobinLoadBalancingSpi`（且总是追加一个用于内部 task）、Indexing 缺省 `NoopIndexingSpi`、Encryption 缺省 `NoopEncryptionSpi`、MetricExporter 缺省 `JmxMetricExporterSpi`（MBean 禁用时 `NoopMetricExporterSpi`）、Tracing 缺省 `NoopTracingSpi`；
   - 默认 cache 配置 `initializeDefaultCacheConfiguration`（`IgnitionEx.java:1990`）——自动追加 `UTILITY_CACHE_NAME` 系统缓存（REPLICATED/TRANSACTIONAL，`IgnitionEx.java:2097-2111`）；若配置了用户 cache 且 DiscoverySpi 不支持排序则报错（`IgnitionEx.java:1998-2002`）。
2. `start0(startCtx, myCfg, startTimer)`（`IgnitionEx.java:1661`）：
   - 多实例共存时校验各 SPI 带 `@IgniteSpiMultipleInstancesSupport`（`IgnitionEx.java:1669-1679`）；
   - 创建 `WorkersRegistry`（blocked worker → `FailureProcessor`，`IgnitionEx.java:1688-1706`）与 OOM `UncaughtExceptionHandler`（`IgnitionEx.java:1681-1686`）；
   - `registerFactoryMbean`（`IgnitionEx.java:1709`）；
   - `new IgniteKernal(startCtx.springContext())`（构造器只保存 spring 资源上下文，`IgniteKernal.java:451-453`）→ **`grid0.start(cfg, ...)`**（`IgnitionEx.java:1721-1731`）；
   - 成功后 `state = STARTED`（`IgnitionEx.java:1733`），并（默认）注册 JVM shutdown hook（`IgnitionEx.java:1761-1782`）。
3. 启动耗时按 stage 打日志 "Node started : [...]"（`IgnitionEx.java:1645-1647`）。

---

## 3. `IgniteKernal.start()`：核心生命周期

方法签名：`modules/core/src/main/java/org/apache/ignite/internal/IgniteKernal.java:844`。下面按代码顺序分阶段说明。

### 3.1 Gateway 状态机与公共校验

- `gw.compareAndSet(null, new GridKernalGatewayImpl(...))` 后 `writeLock` 下把 `GridKernalState` 从 `STOPPED` 置为 `STARTING`（`IgniteKernal.java:853-886`）。状态枚举 `STARTED/STARTING/STOPPING/DISCONNECTED/STOPPED` 见 `modules/core/src/main/java/org/apache/ignite/internal/GridKernalState.java`；gateway 用读写锁保护公共 API 访问（`modules/core/src/main/java/org/apache/ignite/internal/GridKernalGateway.java:42-99`）。
- `validateCommon(cfg)`（`IgniteKernal.java:891`，实现于 `:1358-1381`）：校验 nodeId、logger、全部 SPI 非空、网络超时参数为正。
- `LongJVMPauseDetector` 启动（`IgniteKernal.java:900-902`）；用户属性名不得以保留前缀 `ATTR_PREFIX` 开头（`:908-911`）。
- 收集 `PluginProvider`（`U.allPluginProviders`，`:913`）；初始化 marshaller 类名过滤器 `IgniteMarshallerClassFilter`（`:915-917`）。

### 3.2 `GridKernalContextImpl` 组装

`ctx = new GridKernalContextImpl(log, this, cfg, gw, plugins, clsFilter, workerRegistry, hnd, longJVMPauseDetector)`（`IgniteKernal.java:921-930`）。构造器（`modules/core/src/main/java/org/apache/ignite/internal/GridKernalContextImpl.java:431-465`）创建 `MarshallerContextImpl`、`IgniteDefragmentationImpl`，并持有 `BinaryMarshaller`（字段 `marsh`，`GridKernalContextImpl.java:410`）。它实现 `GridKernalContext` 并维护组件列表 `comps`（`GridKernalContextImpl.java:344`）与节点属性 `attrs`（`:348`）。

每个组件通过 `ctx.add(GridComponent)` 注册：`add(comp, addToList)`（`GridKernalContextImpl.java:488-615`）按 `instanceof` 把 manager/processor 赋到对应字段（`discoMgr`、`ioMgr`、`cacheProc`、`cluster`、……），并可选加入 `comps` 列表（启动后 `for (GridComponent comp : ctx)` 迭代即按此列表顺序）。

`startProcessor` / `startManager`（`IgniteKernal.java:1695-1704` / `:1676-1689`）：**先 `ctx.add` 再 `start()`**（先注册后启动，避免"已启动但注册表查不到"）。组件生命周期契约 `start()/stop()/onKernalStart(boolean)` 定义在 `modules/core/src/main/java/org/apache/ignite/internal/GridComponent.java:93-112`。

### 3.3 SPI 的注入与启动机制（`GridManagerAdapter`）

所有 manager 继承 `modules/core/src/main/java/org/apache/ignite/internal/managers/GridManagerAdapter.java`：

- 构造时传入该 manager 管理的 SPI 数组（来自 `IgniteConfiguration` 的 getter，见 §5 表）；
- `startSpi()`（`GridManagerAdapter.java:240-291`）对每个 SPI 依次：`onBeforeStart()` → **资源注入**（`ctx.resource().inject(spi)` + 内部对象注入）→ 校验 SPI 名唯一 → `onBeforeSpiStart()` → **`spi.spiStart(ctx.igniteInstanceName())`** → `onAfterSpiStart()` → `parseNodeAttributes(spi)`；
- `onKernalStart(active)`（`GridManagerAdapter.java:346-632`）对每个 SPI 调 `spi.onContextInitialized(IgniteSpiContext)`（把 discovery/timeout/metric 等内核能力以匿名 `IgniteSpiContext` 形式喂给 SPI，`:349-624`），再调 `onKernalStart0()` 钩子；
- `stopSpi()`（`:298-329`）调用 `spi.spiStop()` 并清理注入资源。

### 3.4 生命周期回调

- lifecycle beans 在 `GridResourceProcessor` 就绪后先做资源注入（`IgniteKernal.java:957-962`），再收到 `BEFORE_NODE_START`（`notifyLifecycleBeans`，`:965`；实现 `:804-816`）；
- `U.startLifecycleAware(lifecycleAwares(cfg))`（`:968`）启动实现了 `LifecycleAware` 的配置组件——列表见 `IgniteKernal.java:1945-1967`：lifecycle beans、segmentation resolvers、connector 的 interceptor/SSL 工厂、marshaller、logger、MBeanServer、communicationFailureResolver；helper 实现于 `modules/core/src/main/java/org/apache/ignite/internal/util/IgniteUtils.java:5970-5980`；
- 全部启动完成后收到 `AFTER_NODE_START`（`:1255`）。

### 3.5 `fillNodeAttributes`：节点属性里程碑

`fillNodeAttributes(clusterProc.updateNotifierEnabled())`（`IgniteKernal.java:1123`，实现 `:1457-1637`）把以下内容写入 `ctx` 节点属性：环境变量与系统属性（按 `includeProperties` 过滤）、本机 IP/MAC（loopback 告警）、`ATTR_BUILD_VER`（版本）、**`ATTR_MARSHALLER`（marshaller 类名）**、`ATTR_LATE_AFFINITY_ASSIGNMENT`、`ATTR_IGNITE_INSTANCE_NAME`、`ATTR_CLIENT_MODE`、`ATTR_NODE_CONSISTENT_ID`、JVM 参数/PID、data storage 与事务配置摘要，以及每个 SPI 的类名属性（`addSpiAttributes`，`:1664-1670`）。这些属性之后随 join 请求发给集群。

marshaller 本体在更早的 `initializeMarshaller()`（`IgniteKernal.java:936`，实现 `:1408-1422`）初始化：`ctx.marshaller().setContext(ctx.marshallerContext())` 并绑定实例名——2.18 中 `ctx.marshaller()` 固定是 `BinaryMarshaller`（`GridKernalContextImpl.java:410`）。

### 3.6 持久化恢复与 maintenance

- `ctx.cache().context().database().startMemoryRestore(ctx, startTimer)`（`IgniteKernal.java:1131`）——持久化集群的 page memory/WAL 恢复在 discovery join **之前**完成；随后 `ctx.recoveryMode(false)`（`:1133`）。
- `MaintenanceProcessor`（`:1026-1028`）若处于 maintenance 模式，会把 `DiscoverySpi` 换成 `IsolatedDiscoverySpi`、强制 `clusterStateOnStart=INACTIVE` 并重建 discovery manager（`:1030-1051`）；全部组件（除 discovery）启动完后再 `mntcProc.prepareAndExecuteMaintenance()`（`:1148`）。

### 3.7 Discovery 最后启动：加入环形拓扑

`IgniteKernal.java:1150-1160`：gateway `writeLock` 下置 `gw.setState(STARTED)`，**紧接着** `startManager(discoMgr)`（注释明言"最后启动以确保 grid 完全初始化"）。注意 `GridDiscoveryManager` 在前面（`:1016-1018`）就已 `ctx.add(discoMgr, false)` 注册到 context 字段（供其他组件注册监听器），但此时才真正 `start()`。

`GridDiscoveryManager.start()`（`modules/core/src/main/java/org/apache/ignite/internal/managers/discovery/GridDiscoveryManager.java:483`）：

1. 写 offheap 相关节点属性（`:484-485`）；
2. `spi.setMetricsProvider(...)`（`:514`）、安全模式下 `spi.setAuthenticator(...)`（`:520`）、`spi.setListener(new DiscoverySpiListener(){...})`（`:542` 起）——监听 `onLocalNodeInitialized` 与 `onDiscovery`（拓扑事件入口）；
3. 启动 `DiscoveryMessageNotifierThread`（`:1064`）；
4. **`startSpi()`**（`:1066`）→ `TcpDiscoverySpi.spiStart`（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java:2116-2125`）：`initializeImpl()` 依据 client/server 选择 `ClientImpl`/`ServerImpl`（`TcpDiscoverySpi.java:2130-2157`），再委托 `impl.spiStart(...)`；
5. 服务端路径 `ServerImpl.spiStart`（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ServerImpl.java:407-495`）：启动 `RingMessageWorker` 线程与 `TcpServer`，**`spi.initLocalNode(tcpSrvr.port, true)`**（`:445`；`TcpDiscoverySpi.initLocalNode` 在 `TcpDiscoverySpi.java:1187`——用配置的 nodeId 构造 `TcpDiscoveryNode`、`locNode.setAttributes(locNodeAttrs)`、`locNode.local(true)`，并回调 listener 的 `onLocalNodeInitialized`），注册地址到 IP finder，最后 **`joinTopology()`**（`:484`）；
6. `joinTopology()`（`ServerImpl.java:1101` 起）构造 `TcpDiscoveryJoinRequestMessage`（携带节点属性与 `collectExchangeData` 收集的各组件 discovery 数据）并循环 `sendJoinRequestMessage`：联系上任意地址则走环形握手；全部失败且自己是第一个节点则本地成环（`ServerImpl.java:1136-1155`）；join 完成后本节点获得 `node order`（order==1 即 coordinator，`ServerImpl.java:486-487`）；
7. 回到 `GridDiscoveryManager.start()`：**`U.await(startLatch)`**（`:1071`）——`startLatch` 由 `DiscoverySpiListener.onDiscovery` 收到**本地 join 事件**（`locJoinEvt`）时 `countDown()`（`GridDiscoveryManager.java:768-774`），同时完成 `locJoin` future（`:808-811`）并就地通知 security/versions/exchange/service/encryption/cluster 的 `onLocalJoin`（`:789-804`）；
8. `locNode = spi.getLocalNode()`、跨节点属性一致性检查 `checkAttributes`、启动 disco worker 线程（`:1088-1093`）。

### 3.8 `onKernalStart` 回调与完成标志

- `ctx.discovery().localJoin()` 取 `DiscoveryLocalJoinData`（`IgniteKernal.java:1179`）；
- **`ctx.discovery().onKernalStart(true)` 最先**（`:1185`，保证拓扑就绪），**`ctx.io().onKernalStart(true)` 第二**（`:1189`，保证可收发消息；`GridIoManager.onKernalStart0` 注册本地事件监听器，见 `modules/core/src/main/java/org/apache/ignite/internal/managers/communication/GridIoManager.java:918`）；
- 若 join 时集群正处于 state transition，先等待其结束拿 `active`（`:1191-1204`）；
- 注册 metrics/system view（`:1206-1212`）、`mBeansMgr.registerMBeansAfterNodeStarted()`（`:1215`；实现见 `modules/core/src/main/java/org/apache/ignite/internal/managers/IgniteMBeansManager.java:106-189`，注册 Kernal/Metrics/Transactions/Query/Compute/Service/DataStorage 等 MXBean）；
- 遍历 `ctx` 中所有组件依序 `comp.onKernalStart(active)`（显式跳过 GridDiscoveryManager、GridIoManager 与 GridPluginComponent，`:1220-1245`；客户端可能抛 `IgniteNeedReconnectException` 进入 reconnect 流程）；
- plugins 收到 `provider.onIgniteStart()`（`:1248-1249`）；lifecycle beans 收到 `AFTER_NODE_START`（`:1255`）；
- **完成标志**：`startTime = U.currentTimeMillis()`（`:1280`）、可选 metrics 定时日志任务（`:1285-1296`）、`info.ackKernalStarted(log, this)`（`:1298`；打印节点信息/性能建议等，`modules/core/src/main/java/org/apache/ignite/internal/plugin/IgniteLogInfoProviderImpl.java:140-147`）、`ctx.discovery().ackTopology(joinTopologyVersion, EVT_NODE_JOINED, localNode())`（`:1300`；实现 `GridDiscoveryManager.java:1438-1502` 输出拓扑快照）。

**"什么算 started"的分层答案**：

| 层 | 信号 | 源码 |
|---|---|---|
| 组件级 | 全部 manager/processor 的 `start()` 与 `onKernalStart()` 已跑完 | `IgniteKernal.java:932-1245` |
| kernal 级 | `GridKernalGateway` state = `STARTED`（join 拓扑前置位） | `IgniteKernal.java:1150-1156` |
| 实例级 | `IgniteNamedInstance.state = STARTED`、`startLatch` 归零、shutdown hook 挂上 | `IgnitionEx.java:1733`、`1650`、`1761-1782` |
| 工厂级 | `notifyStateChange(name, STARTED)` → `IgnitionListener`、`Ignition.state()` 可见 | `IgnitionEx.java:1096`、`:1393-1401` |
| 集群级 | 本地 join 完成（`locJoin` future、`EVT_NODE_JOINED` 拓扑事件） | `GridDiscoveryManager.java:768-813` |

对外 API（如 `ignite.events()`、`ignite.cluster()`）在 `IgniteKernal` 上直接委托 `ctx` 对应组件（`ignite.events()` → `ctx.cluster().get().events()`，`IgniteKernal.java:481-483`；`ClusterGroupAdapter.events()` 惰性构造 `IgniteEventsImpl`，`modules/core/src/main/java/org/apache/ignite/internal/cluster/ClusterGroupAdapter.java:241`）——即公共 API 的可用性由 `ctx` 各组件就绪 + gateway 读锁保证，而非某个单独标志位。

---

## 4. Manager 一节一责（含 SPI 绑定）

每个 manager 的启动入口 `start()` 都调用 `GridManagerAdapter.startSpi()`（行号见下表"startSpi 行"），SPI 数组来自构造器（"构造器行"）。

| Manager | 管理的 SPI（默认实现见 §2.4） | 职责 | 源码引用 |
|---|---|---|---|
| `GridTracingManager` | `TracingSpi`（`NoopTracingSpi` 降级重试） | 分布式 tracing 的创建/传播（SPI 失败时降级为 Noop 二次启动） | `managers/tracing/GridTracingManager.java:122`（super）、`:134`（startSpi）；启动点 `IgniteKernal.java:997-1001` |
| `GridMetricManager` | `MetricExporterSpi[]` + 自动追加 `SqlViewMetricExporterSpi` | 内部 metrics 注册表；为每个 exporter 提供只读视图 | `processors/metric/GridMetricManager.java:79`、`:210-221`、`:269-273`；启动点 `IgniteKernal.java:1002` |
| `GridSystemViewManager` | `SystemViewExporterSpi[]`（追加 `SqlViewExporterSpi`，`JmxSystemViewExporterSpi` 缺省） | 系统 view（sys view）注册中心，供 JMX/SQL 导出 | `managers/systemview/GridSystemViewManager.java:51`、`:63`、`:231`；启动点 `IgniteKernal.java:1003` |
| `GridIoManager` | `CommunicationSpi`（`TcpCommunicationSpi`） | 节点间通信中枢：消息工厂组装、`CommunicationListener` 挂接、消息按 topic 分发、IO metrics | `managers/communication/GridIoManager.java:379`（super）、`:423-507`（start：formatter/msgFactory/`startSpi()` at 471/commLsnr）；启动点 `IgniteKernal.java:1004` |
| `GridCheckpointManager` | `CheckpointSpi` | 计算 job 的 checkpoint 保存/加载 | `managers/checkpoint/GridCheckpointManager.java:82`（super）、`:112`（startSpi） |
| `GridEventStorageManager` | `EventStorageSpi` | 事件存储与本地/远程事件查询、监听器管理（`ignite.events()` 的后端） | `managers/eventstorage/GridEventStorageManager.java:139`（super）、`:281-301`（start） |
| `GridDeploymentManager` | `DeploymentSpi` | 类部署（peer class loading 模式下加载用户任务类） | `managers/deployment/GridDeploymentManager.java:72`（super）、`:102`（startSpi） |
| `GridLoadBalancerManager` | `LoadBalancingSpi[]` | compute task 的负载均衡策略 | `managers/loadbalancer/GridLoadBalancerManager.java:44`（super）、`:49`（startSpi） |
| `GridFailoverManager` | `FailoverSpi[]` | compute job 失败后重路由 | `managers/failover/GridFailoverManager.java:39`（super）、`:44`（startSpi） |
| `GridCollisionManager` | `CollisionSpi` | job 并发仲裁（缺省 Noop） | `managers/collision/GridCollisionManager.java:44`（super）、`:49`（startSpi） |
| `GridIndexingManager` | `IndexingSpi` | 外部自定义索引 SPI 接入 | `managers/indexing/GridIndexingManager.java:42`（super）、`:49`（startSpi） |
| `GridDiscoveryManager` | `DiscoverySpi`（`TcpDiscoverySpi`） | 拓扑管理：join/离开事件、`DiscoCache` 拓扑快照、节点可见性、segmentation 检查 | `managers/discovery/GridDiscoveryManager.java:314`（super）、`:483`（start）、`:1066`（startSpi）；启动点 `IgniteKernal.java:1156`（最后） |
| `GridEncryptionManager` | `EncryptionSpi` | 加密密钥管理（TDE 场景） | `managers/encryption/GridEncryptionManager.java:230`（super）、`:237`（startSpi） |

（上表未注明的 manager 目录前缀均为 `modules/core/src/main/java/org/apache/ignite/internal/`。）

---

## 5. Processor 一节一责（按启动顺序）

启动顺序全部来自 `IgniteKernal#start` 的 `startProcessor(...)` 调用序列（`IgniteKernal.java:932-1102`）。文件路径省略公共前缀 `modules/core/src/main/java/org/apache/ignite/internal/`。

| # | Processor | 职责（经源码 javadoc/start 体核实） | 引用 |
|---|---|---|---|
| 1 | `DiagnosticProcessor` | 诊断辅助（dump page lock 等） | `processors/diagnostic/DiagnosticProcessor.java:52`；`IgniteKernal.java:932` |
| 2 | `GridInternalSubscriptionProcessor` | 本地组件间内部事件订阅（无网络）；注释明确"启动得最早以便任何组件可用" | `processors/subscription/GridInternalSubscriptionProcessor.java:40`；`IgniteKernal.java:938` |
| 3 | `ClusterProcessor` | `ignite.cluster()` 的后端（`IgniteClusterImpl`）、cluster id/tag、更新通知器 | `processors/cluster/ClusterProcessor.java:182-205`；`IgniteKernal.java:940-942` |
| 4 | `GridResourceProcessor` | 资源注入（`@IgniteInstanceResource` 等注解）+ spring 上下文；注释："先于其他所有 manager/processor 启动" | `IgniteKernal.java:946-954` |
| 5 | `IgnitePluginProcessor` | 插件 provider 注册、`PluginContext`、扩展点（`createComponent` 会走它） | `processors/plugin/IgnitePluginProcessor.java:52`；`IgniteKernal.java:970` |
| 6 | `FailureProcessor` | 统一 failure 处理 API（FailureHandler 触发） | `processors/failure/FailureProcessor.java:48`；`IgniteKernal.java:972` |
| 7 | security processor | 认证开启时 `IgniteAuthenticationProcessor`（包在 `IgniteSecurityProcessor` 内），否则 `NoOpIgniteSecurityProcessor` | `IgniteKernal.java:975`、`:1309-1320`（`securityProcessor()`） |
| 8 | `PoolProcessor` | 全部线程池创建（public/mgmt/… executor） | `processors/pool/PoolProcessor.java:78`、`:290`（start 创建 pub 池等）；`IgniteKernal.java:977` |
| 9 | `GridClosureProcessor` | compute 闭包/call/run 执行器（注释：依赖者众多，须早启动） | `processors/closure/GridClosureProcessor.java:85`；`IgniteKernal.java:984` |
| 10 | `GridPortProcessor` | 端口使用登记/注销 | `processors/port/GridPortProcessor.java:36`；`IgniteKernal.java:987` |
| 11 | `GridJobMetricsProcessor` | job 指标（已废弃，改用 metric registry） | `processors/jobmetrics/GridJobMetricsProcessor.java:47`；`IgniteKernal.java:988` |
| 12 | `GridTimeoutProcessor` | 全局超时对象调度（注释：manager 们依赖它，须先于 manager 启动） | `processors/timeout/GridTimeoutProcessor.java:45`；`IgniteKernal.java:992` |
| 13 | `PdsConsistentIdProcessor` | PDS 存储目录解析 + 持久化场景生成 consistentId | `processors/cache/persistence/filename/PdsConsistentIdProcessor.java:32`；`IgniteKernal.java:1024` |
| 14 | `MaintenanceProcessor` | maintenance 模式注册表与执行 | `internal/maintenance/MaintenanceProcessor.java:41`；`IgniteKernal.java:1026-1028` |
| 15 | CompressionProcessor（可选） | 页面压缩（ignite-compress 模块在 classpath 时） | `processors/compress/CompressionProcessor.java`；`IgniteKernal.java:1056`；类型映射 `internal/IgniteComponentType.java:102` |
| 16 | `GridMarshallerMappingProcessor` | marshaller 类 ID 映射的集群级交换 | `processors/marshaller/GridMarshallerMappingProcessor.java:76`；`IgniteKernal.java:1057` |
| 17 | `DiscoveryNodeValidationProcessor` → `RollingUpgradeProcessor` | join 时节点校验/滚动升级兼容 | `internal/IgniteKernal.java:1058`、`createComponent` 映射 `:3227-3228`；`processors/rollingupgrade/RollingUpgradeProcessor.java` |
| 18 | `GridAffinityProcessor` | 面向公共 API 的 data affinity（`Affinity` 接口后端） | `processors/affinity/GridAffinityProcessor.java:85`；`IgniteKernal.java:1059` |
| 19 | `GridSegmentationProcessor` | 网络分段检测/处理 | `processors/segmentation/GridSegmentationProcessor.java:34`；`IgniteKernal.java:1060` |
| 20 | `IgniteCacheObjectProcessor` → `CacheObjectBinaryProcessorImpl` | cache 对象（二进制）的序列化/元数据 | `processors/cache/binary/CacheObjectBinaryProcessorImpl.java:126`；`IgniteKernal.java:1064`、`createComponent` `:3224-3225` |
| 21 | `GridClusterStateProcessor` | 集群 active/inactive 状态机、baseline | `processors/cluster/GridClusterStateProcessor.java:130`；`IgniteKernal.java:1068` |
| 22 | `PerformanceStatisticsProcessor` | 性能统计文件记录 | `processors/performancestatistics/PerformanceStatisticsProcessor.java`；`IgniteKernal.java:1069` |
| 23 | `GridCacheProcessor` | **cache 子系统入口**：构建 `GridCacheSharedContext` 与全部 shared manager、cache/group 生命周期 | `processors/cache/GridCacheProcessor.java:583-650`（start）、`:678-712`（onKernalStart 含首次 exchange + awaitRebalance）；`IgniteKernal.java:1070` |
| 24 | `IndexProcessor` | SQL/内部索引的创建管理 | `internal/cache/query/index/IndexProcessor.java:79`；`IgniteKernal.java:1078` |
| 25 | QueryEngine（可选） | ignite-calcite 在 classpath 时为 `CalciteQueryProcessor`（`QUERY_ENGINE`），否则 NoOp | `internal/IgniteComponentType.java:116-122`；`IgniteKernal.java:1080-1081` |
| 26 | `GridQueryProcessor` | 索引/SQL 查询处理器（H2 时代入口） | `processors/query/GridQueryProcessor.java:183`；`IgniteKernal.java:1083` |
| 27 | `ClientListenerProcessor` | thin client（JDBC/ODBC/Java thin）TCP 监听 | `processors/odbc/ClientListenerProcessor.java:85`；`IgniteKernal.java:1084` |
| 28 | `IgniteServiceProcessor` | 分布式服务网格（经 discovery/communication 消息驱动） | `processors/service/IgniteServiceProcessor.java:128`；`IgniteKernal.java:1085` |
| 29 | `GridTaskSessionProcessor` | compute task 会话管理 | `processors/session/GridTaskSessionProcessor.java:40`；`IgniteKernal.java:1086` |
| 30 | `GridJobProcessor` | compute job 调度执行/失败处理 | `processors/job/GridJobProcessor.java:134`；`IgniteKernal.java:1087` |
| 31 | `GridTaskProcessor` | compute task 提交与状态机 | `processors/task/GridTaskProcessor.java:118`；`IgniteKernal.java:1088` |
| 32 | Schedule processor（可选） | cron 式调度（ignite-schedule 模块） | `internal/IgniteComponentType.java:95-100`；`IgniteKernal.java:1089` |
| 33 | `IgniteRestProcessor` → `GridRestProcessor` | REST 协议（TCP/HTTP）：注册各命令 handler 并启动协议监听 | `processors/rest/GridRestProcessor.java:108`、`:529-558`（start：addHandler×N + `startTcpProtocol/startHttpProtocol`）；`IgniteKernal.java:1090`、`createComponent` `:3236-3237` |
| 34 | `DataStreamProcessor` | DataStreamer 流式写入 | `processors/datastreamer/DataStreamProcessor.java:59`；`IgniteKernal.java:1091` |
| 35 | `GridContinuousProcessor` | continuous queries/routines | `processors/continuous/GridContinuousProcessor.java:119`；`IgniteKernal.java:1092` |
| 36 | `DataStructuresProcessor` | 原子类型/锁/队列等数据结构 | `processors/datastructures/DataStructuresProcessor.java:129`；`IgniteKernal.java:1093` |
| 37 | PlatformProcessor（可选） | .NET/Java 平台互操作（配置了 platformConfiguration 时；另有 `PlatformPluginProcessor`） | `processors/platform/PlatformProcessor.java:30`（接口）；`IgniteKernal.java:1094`、`:1116-1117` |
| 38 | `DistributedMetaStorageImpl` | 分布式 metastorage（持久化 + discovery 传播） | `processors/metastorage/persistence/DistributedMetaStorageImpl.java`（类 javadoc）；`IgniteKernal.java:1095` |
| 39 | `DistributedConfigurationProcessor` | 分布式配置（`distributed configuration`）扩展 | `processors/configuration/distributed/DistributedConfigurationProcessor.java`；`IgniteKernal.java:1096` |
| 40 | `DurableBackgroundTasksProcessor` | 本地持久化后台任务 | `processors/localtask/DurableBackgroundTasksProcessor.java:38`；`IgniteKernal.java:1097` |
| 41 | `CacheObjectTransformerProcessor`（可选） | cache 对象转换扩展点（默认无实现返回 null） | `IgniteKernal.java:1099-1102`、`createComponent` `:3239-3240` |

### 5.1 `GridCacheProcessor` 与 shared managers、affinity

`GridCacheProcessor.start()`（`processors/cache/GridCacheProcessor.java:583`）创建 `ClusterCachesInfo`、`GridLocalConfigManager`，并通过 `createSharedContext`（`:3025-3103`）用 builder 构建 `GridCacheSharedContext`，固定装入以下 shared manager：`IgniteTxManager`、`GridCacheMvccManager`、`GridCacheVersionManager`、`GridCacheDeploymentManager`、`GridCachePartitionExchangeManager`、`IgniteCacheDatabaseSharedManager`（持久化为 `GridCacheDatabaseSharedManager`，否则 `IgniteCacheDatabaseSharedManager`）、`FilePageStoreManager`、`FileWriteAheadLogManager`、`WalStateManager`、`IgniteSnapshotManager`、`GridCacheIoManager`、`CacheAffinitySharedManager`、`GridCacheSharedTtlCleanupManager`、`PartitionsEvictManager`、JTA（可选）、`CacheDiagnosticManager`、CdcManager（`:3084-3102`）。随后逐个 `mgr.start(sharedCtx)`（`:606-608`）。

`onKernalStart(active)`（`:678-712`）执行入队一致性校验、`cachesInfo.onKernalStart`、`sharedCtx.exchange().onKernalStart(...)`（**触发本地 join 的首次 partition exchange**），并等待 SYNC rebalance 完成（`awaitRebalance(joinVer).get()`，`:711`）。

**affinity 缓存**：`CacheGroupContext` 初始化 cache group 时创建 `GridAffinityAssignmentCache`（`processors/cache/CacheGroupContext.java:1035-1037`：优先复用 `ctx.affinity().groupAffinity(grpId)`，否则 `GridAffinityAssignmentCache.create(...)`）；它缓存 `topology version → partition → nodes` 的映射（`processors/affinity/GridAffinityAssignmentCache.java:117` 字段 `affCache`）。per-cache 的 `GridCacheAffinityManager.start0()` 只是取 `cctx.group().affinity()`（`processors/cache/GridCacheAffinityManager.java:61-67`）——即 affinity 计算发生在 cache group 初始化/exchange 阶段（`onKernalStart` 之后），而非 processor `start()` 阶段。

---

## 6. 启动顺序完整清单（单一时间线）

下表把 `Ignition` → `IgnitionEx` → `IgniteKernal` 串成一条线。引用默认指向 `modules/core/src/main/java/` 下的文件（`org/apache/ignite/...`）。

| 步 | 动作 | 引用 |
|---|---|---|
| 1 | `Ignition.start(...)` 重载入口 + sandbox proxy 包装 | `org/apache/ignite/Ignition.java:280-380`、`:548` |
| 2 | Spring/流配置解析 `loadConfigurations` | `internal/IgnitionEx.java:870-946` |
| 3 | `IgnitionEx.start0`：注册表登记/已启动检查/warmup closure | `internal/IgnitionEx.java:1018-1079` |
| 4 | `IgniteNamedInstance.start`：`initializeConfiguration`（nodeId、logger、工作目录、默认 SPI、默认 cache 配置、DataStorage 校验） | `internal/IgnitionEx.java:1632-1655`、`:1795-1940`、`:2026-2090` |
| 5 | `new IgniteKernal(springCtx)`；`WorkersRegistry`、factory MBean、OOM handler | `internal/IgnitionEx.java:1681-1717` |
| 6 | `IgniteKernal.start`：gateway `STOPPED→STARTING`、`validateCommon`、JVM pause detector | `internal/IgniteKernal.java:844-902` |
| 7 | `GridKernalContextImpl` 组装（`MarshallerContextImpl`、`BinaryMarshaller`） | `internal/IgniteKernal.java:921-930`；`internal/GridKernalContextImpl.java:431-465` |
| 8 | P1 `DiagnosticProcessor` → P2 `GridInternalSubscriptionProcessor` → P3 `ClusterProcessor` | `internal/IgniteKernal.java:932-942` |
| 9 | `initializeMarshaller()`（BinaryMarshaller setContext） | `internal/IgniteKernal.java:936`、`:1408-1422` |
| 10 | P4 `GridResourceProcessor`（+ `IgniteSchedulerImpl`）；lifecycle beans 注入 → `BEFORE_NODE_START` → `LifecycleAware` 组件启动 | `internal/IgniteKernal.java:946-968`；`internal/util/IgniteUtils.java:5970` |
| 11 | P5 `IgnitePluginProcessor` → P6 `FailureProcessor` → P7 security → P8 `PoolProcessor`（线程池） | `internal/IgniteKernal.java:970-980` |
| 12 | P9 `GridClosureProcessor` → P10 `GridPortProcessor` → P11 `GridJobMetricsProcessor` → P12 `GridTimeoutProcessor` | `internal/IgniteKernal.java:984-992` |
| 13 | M1 `GridTracingManager` → M2 `GridMetricManager` → M3 `GridSystemViewManager` → M4 `GridIoManager`（**启动 TcpCommunicationSpi**）→ M5 `GridCheckpointManager` | `internal/IgniteKernal.java:997-1005` |
| 14 | M6 `GridEventStorageManager` → M7 `GridDeploymentManager` → M8 `GridLoadBalancerManager` → M9 `GridFailoverManager` → M10 `GridCollisionManager` → M11 `GridIndexingManager` | `internal/IgniteKernal.java:1007-1012` |
| 15 | `GridDiscoveryManager` **仅注册**（`ctx.add(discoMgr, false)`，不启动、不入 comps） | `internal/IgniteKernal.java:1016-1018` |
| 16 | M12 `GridEncryptionManager` | `internal/IgniteKernal.java:1022` |
| 17 | P13 `PdsConsistentIdProcessor` → P14 `MaintenanceProcessor`（maintenance 时换 `IsolatedDiscoverySpi` 重建 discoMgr） | `internal/IgniteKernal.java:1024-1051` |
| 18 | P15 Compression（可选）→ P16 `GridMarshallerMappingProcessor` → P17 DiscoveryNodeValidation（`RollingUpgradeProcessor`）→ P18 `GridAffinityProcessor` → P19 `GridSegmentationProcessor` | `internal/IgniteKernal.java:1055-1060` |
| 19 | P20 `IgniteCacheObjectProcessor`（`CacheObjectBinaryProcessorImpl`）→ P21 `GridClusterStateProcessor` → P22 `PerformanceStatisticsProcessor` → P23 `GridCacheProcessor`（shared managers，见 §5.1） | `internal/IgniteKernal.java:1064-1070` |
| 20 | （authentication 时）`IgniteAuthenticationProcessor.startProcessor()` → P24 `IndexProcessor` → P25 QueryEngine（可选）→ P26 `GridQueryProcessor` | `internal/IgniteKernal.java:1072-1083` |
| 21 | P27 `ClientListenerProcessor` → P28 `IgniteServiceProcessor` → P29 `GridTaskSessionProcessor` → P30 `GridJobProcessor` → P31 `GridTaskProcessor` → P32 Schedule（可选）→ P33 `GridRestProcessor`（REST TCP/HTTP） | `internal/IgniteKernal.java:1084-1090` |
| 22 | P34 `DataStreamProcessor` → P35 `GridContinuousProcessor` → P36 `DataStructuresProcessor` → P37 Platform（可选）→ P38 `DistributedMetaStorageImpl` → P39 `DistributedConfigurationProcessor` → P40 `DurableBackgroundTasksProcessor` → P41 CacheObjectTransformer（可选） | `internal/IgniteKernal.java:1091-1102` |
| 23 | plugins `provider.start(...)`；`PlatformPluginProcessor`；`registerMBeansDuringInitPhase` | `internal/IgniteKernal.java:1106-1119` |
| 24 | `fillNodeAttributes`（环境/系统属性、IP/MAC、版本、marshaller、SPI 类名、用户属性） | `internal/IgniteKernal.java:1123`、`:1457-1637` |
| 25 | metastore ready → **`startMemoryRestore`（持久化恢复）** → `recoveryMode(false)` | `internal/IgniteKernal.java:1125-1135` |
| 26 | `gw.setState(STARTED)` → **`startManager(discoMgr)`（最后一个组件）**：`GridDiscoveryManager.start` → `startSpi` → `TcpDiscoverySpi.spiStart` → `ServerImpl.spiStart`（initLocalNode → joinTopology 环形拓扑）→ `U.await(startLatch)`（本地 join） | `internal/IgniteKernal.java:1150-1160`；`internal/managers/discovery/GridDiscoveryManager.java:483-1097`；`org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java:2116`；`org/apache/ignite/spi/discovery/tcp/ServerImpl.java:407-495`、`:1101` |
| 27 | 文件编码/物理内存/JVM/OS 检查与建议 | `internal/IgniteKernal.java:1164-1177` |
| 28 | `ctx.discovery().onKernalStart(true)` → `ctx.io().onKernalStart(true)` | `internal/IgniteKernal.java:1185-1189` |
| 29 | （join 时有 state transition 则等待）active 判定 | `internal/IgniteKernal.java:1191-1204` |
| 30 | metrics/system view 注册；`registerMBeansAfterNodeStarted` | `internal/IgniteKernal.java:1206-1215` |
| 31 | 其余组件依次 `onKernalStart(active)`（含 `GridCacheProcessor.onKernalStart` → 首次 exchange + SYNC rebalance 等待） | `internal/IgniteKernal.java:1220-1245`；`internal/processors/cache/GridCacheProcessor.java:678-712` |
| 32 | plugins `onIgniteStart()`；（client 重连等待） | `internal/IgniteKernal.java:1247-1252` |
| 33 | lifecycle beans `AFTER_NODE_START` | `internal/IgniteKernal.java:1255` |
| 34 | `startTime` 标记、metrics 日志任务、`ackKernalStarted`、`ackTopology(EVT_NODE_JOINED)` | `internal/IgniteKernal.java:1279-1303` |
| 35 | 回到 `IgniteNamedInstance`：`state = STARTED`、shutdown hook | `internal/IgnitionEx.java:1733`、`:1761-1782` |
| 36 | 回到静态 `start0`：`notifyStateChange(name, STARTED)` → `Ignition.start()` 返回 | `internal/IgnitionEx.java:1096`；`org/apache/ignite/Ignition.java:298-305` |

## 7. 关键里程碑小结

- **node id**：`IgnitionEx.initializeConfiguration`（`IgnitionEx.java:1822`）在进入 kernal 前生成；`TcpDiscoverySpi.initLocalNode`（`TcpDiscoverySpi.java:1187`）用它构造 `TcpDiscoveryNode` 并挂 attributes。
- **attributes**：kernal 阶段 `fillNodeAttributes`（`IgniteKernal.java:1457`）集中填充；SPI 相关属性由 `GridManagerAdapter.startSpi → parseNodeAttributes` 补充（`GridManagerAdapter.java:284`）；随 join 请求广播（`ServerImpl.joinTopology`，`ServerImpl.java:1122-1124`）。
- **marshaller**：context 持有 `BinaryMarshaller`（`GridKernalContextImpl.java:410`），`initializeMarshaller()`（`IgniteKernal.java:1408`）绑定 context 与实例名；映射交换由 `GridMarshallerMappingProcessor` 负责（`IgniteKernal.java:1057`）。
- **加入环形拓扑**：`IgniteKernal` 中 discovery manager 被刻意放在最后启动（`IgniteKernal.java:1155-1156`）；实际 join 发生在 `ServerImpl.spiStart → joinTopology`（`ServerImpl.java:484`、`:1101`）；join 完成的进程内信号是 `GridDiscoveryManager.startLatch` countDown 与 `locJoin` future 完成（`GridDiscoveryManager.java:774`、`:808`）。
- **延迟启动/按需组件**：可选组件经 `IgniteKernal#createComponent`（`IgniteKernal.java:3215-3240`）按 classpath 模块（`IgniteComponentType`，`internal/IgniteComponentType.java:32`：SCHEDULE/COMPRESSION/QUERY_ENGINE/JTA…）或插件 provider 创建；`GridDiscoveryManager` 是"先注册后启动"的特殊组件（`ctx.add(discoMgr, false)`）。

---

## 8. 引用文件清单

以下为本报告实际打开/grep 验证过的源码文件（均位于 `vendors/ignite/` 下，前缀 `modules/core/src/main/java/`）：

1. `org/apache/ignite/Ignition.java`
2. `org/apache/ignite/internal/IgnitionEx.java`
3. `org/apache/ignite/internal/IgniteKernal.java`
4. `org/apache/ignite/internal/GridKernalContextImpl.java`
5. `org/apache/ignite/internal/GridKernalGateway.java`
6. `org/apache/ignite/internal/GridKernalState.java`
7. `org/apache/ignite/internal/GridComponent.java`
8. `org/apache/ignite/internal/IgniteComponentType.java`
9. `org/apache/ignite/internal/managers/GridManagerAdapter.java`
10. `org/apache/ignite/internal/managers/IgniteMBeansManager.java`
11. `org/apache/ignite/internal/managers/discovery/GridDiscoveryManager.java`
12. `org/apache/ignite/internal/managers/communication/GridIoManager.java`
13. `org/apache/ignite/internal/managers/eventstorage/GridEventStorageManager.java`
14. `org/apache/ignite/internal/managers/systemview/GridSystemViewManager.java`
15. `org/apache/ignite/internal/managers/checkpoint/GridCheckpointManager.java`
16. `org/apache/ignite/internal/managers/deployment/GridDeploymentManager.java`
17. `org/apache/ignite/internal/managers/loadbalancer/GridLoadBalancerManager.java`
18. `org/apache/ignite/internal/managers/failover/GridFailoverManager.java`
19. `org/apache/ignite/internal/managers/collision/GridCollisionManager.java`
20. `org/apache/ignite/internal/managers/indexing/GridIndexingManager.java`
21. `org/apache/ignite/internal/managers/encryption/GridEncryptionManager.java`
22. `org/apache/ignite/internal/managers/tracing/GridTracingManager.java`
23. `org/apache/ignite/internal/processors/metric/GridMetricManager.java`
24. `org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java`
25. `org/apache/ignite/spi/discovery/tcp/ServerImpl.java`
26. `org/apache/ignite/spi/communication/tcp/TcpCommunicationSpi.java`
27. `org/apache/ignite/internal/processors/cache/GridCacheProcessor.java`
28. `org/apache/ignite/internal/processors/cache/GridCacheAffinityManager.java`
29. `org/apache/ignite/internal/processors/affinity/GridAffinityAssignmentCache.java`
30. `org/apache/ignite/internal/processors/cache/CacheGroupContext.java`
31. `org/apache/ignite/internal/processors/cluster/ClusterProcessor.java`
32. `org/apache/ignite/internal/cluster/ClusterGroupAdapter.java`
33. `org/apache/ignite/internal/plugin/IgniteLogInfoProviderImpl.java`
34. `org/apache/ignite/internal/util/IgniteUtils.java`
35. `org/apache/ignite/internal/processors/pool/PoolProcessor.java`
36. `org/apache/ignite/internal/processors/rest/GridRestProcessor.java`
37. `org/apache/ignite/internal/processors/diagnostic/DiagnosticProcessor.java`
38. `org/apache/ignite/internal/processors/subscription/GridInternalSubscriptionProcessor.java`
39. `org/apache/ignite/internal/processors/plugin/IgnitePluginProcessor.java`
40. `org/apache/ignite/internal/processors/failure/FailureProcessor.java`
41. `org/apache/ignite/internal/processors/closure/GridClosureProcessor.java`
42. `org/apache/ignite/internal/processors/port/GridPortProcessor.java`
43. `org/apache/ignite/internal/processors/jobmetrics/GridJobMetricsProcessor.java`
44. `org/apache/ignite/internal/processors/timeout/GridTimeoutProcessor.java`
45. `org/apache/ignite/internal/processors/cache/persistence/filename/PdsConsistentIdProcessor.java`
46. `org/apache/ignite/internal/maintenance/MaintenanceProcessor.java`
47. `org/apache/ignite/internal/processors/compress/CompressionProcessor.java`
48. `org/apache/ignite/internal/processors/marshaller/GridMarshallerMappingProcessor.java`
49. `org/apache/ignite/internal/processors/rollingupgrade/RollingUpgradeProcessor.java`
50. `org/apache/ignite/internal/processors/affinity/GridAffinityProcessor.java`
51. `org/apache/ignite/internal/processors/segmentation/GridSegmentationProcessor.java`
52. `org/apache/ignite/internal/processors/cache/binary/CacheObjectBinaryProcessorImpl.java`
53. `org/apache/ignite/internal/processors/cluster/GridClusterStateProcessor.java`
54. `org/apache/ignite/internal/processors/performancestatistics/PerformanceStatisticsProcessor.java`
55. `org/apache/ignite/internal/cache/query/index/IndexProcessor.java`
56. `org/apache/ignite/internal/processors/query/GridQueryProcessor.java`
57. `org/apache/ignite/internal/processors/odbc/ClientListenerProcessor.java`
58. `org/apache/ignite/internal/processors/service/IgniteServiceProcessor.java`
59. `org/apache/ignite/internal/processors/session/GridTaskSessionProcessor.java`
60. `org/apache/ignite/internal/processors/job/GridJobProcessor.java`
61. `org/apache/ignite/internal/processors/task/GridTaskProcessor.java`
62. `org/apache/ignite/internal/processors/datastreamer/DataStreamProcessor.java`
63. `org/apache/ignite/internal/processors/continuous/GridContinuousProcessor.java`
64. `org/apache/ignite/internal/processors/datastructures/DataStructuresProcessor.java`
65. `org/apache/ignite/internal/processors/platform/PlatformProcessor.java`
66. `org/apache/ignite/internal/processors/metastorage/persistence/DistributedMetaStorageImpl.java`
67. `org/apache/ignite/internal/processors/configuration/distributed/DistributedConfigurationProcessor.java`
68. `org/apache/ignite/internal/processors/localtask/DurableBackgroundTasksProcessor.java`
