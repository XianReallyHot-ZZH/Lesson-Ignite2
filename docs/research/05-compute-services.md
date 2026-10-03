# 05 · 计算与服务面：Compute Task、服务网格、DataStreamer 与部署/Peer Class Loading

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文覆盖四条执行路径：① `ignite.compute()` 的 task 生命周期（含 load balancing 与 failover）；② `IgniteServiceProcessor` 服务网格的部署/重分配/调用；③ `IgniteDataStreamer` 批量写入与单条 put 的汇合点；④ `GridDeploymentManager` 部署体系与 peer class loading（含 `modules/urideploy`）。正文短路径约定：`internal/` 指 `modules/core/src/main/java/org/apache/ignite/internal/`，`proc/` 指 `internal/processors/`，`managers/` 指 `internal/managers/`，`spi/` 指 `modules/core/src/main/java/org/apache/ignite/spi/`，`cfg/` 指 `modules/core/src/main/java/org/apache/ignite/configuration/`；**文末"引用文件清单"给出全部完整路径**（均已逐一打开验证），行号以当前 submodule 内容为准。

---

## 0. 全景先决事实（影响全篇的三个结构判断）

1. **compute 没有独立的"分布式执行框架"，一切皆 `ComputeTask`**。`ignite.compute().run/call/apply/broadcast/affinityCall` 等闭包 API 在 `GridClosureProcessor` 里被折算成 11 个内置 task 适配器（T1–T11）后交给 `GridTaskProcessor.execute(...)`（`proc/closure/GridClosureProcessor.java` L171/L198）。真正的自定义 task 也走同一入口。因此复刻 compute 面只需要实现一条 task 状态机 + 两种核心消息（job 请求/响应）。
2. **服务网格的"控制面"跑在 discovery 上，"数据面"跑在 compute 上**。部署/取消/重分配由 discovery 自定义消息（`ServiceChangeBatchRequest` 等）驱动，逐节点收敛由每拓扑版本一个的 `ServiceDeploymentTask` 完成，其协调消息走 `TOPIC_SERVICES`；而服务方法的远程调用直接复用 compute 闭包通道（`GridServiceProxy` → `ctx.closure().callAsync(...)`，`proc/service/GridServiceProxy.java` L221-228）。
3. **2.18 的 compute 门面多了一层 `IgniteComputeHandler`**（`internal/IgniteComputeHandler.java` L53）：`IgniteComputeImpl` 不再直接持有 `GridClosureProcessor`，而是经该 handler 聚合 `TaskExecutionOptions`（pool/timeout/failover 开关等）再分发到 `ctx.task()` 或 `ctx.closure()`（`internal/IgniteComputeImpl.java` L66）。这是 2.17/2.18 的新抽象，复刻课可以合并回处理器本身。

---

## 1. Compute task 生命周期

### 1.1 调用链总览（用户 task，10 步）

| # | 组件（类#方法） | 位置 | 一句话 |
|---|---|---|---|
| 1 | `IgniteComputeImpl#execute/run/call/broadcast` | `internal/IgniteComputeImpl.java` L220-307 | 用户 API 入口，委托给 `IgniteComputeHandler`（task → `ctx.task().execute`，闭包 → `ctx.closure().runAsync/callAsync`） |
| 2 | `GridClosureProcessor#runAsync(mode, jobs, opts)` | `proc/closure/GridClosureProcessor.java` L152-203 | 闭包包装成内置 task（如 `T1`/`T2`），同样进入 `ctx.task().execute(...)`（L171/L198） |
| 3 | `GridTaskProcessor#execute(...)` → `startTask` | `proc/task/GridTaskProcessor.java` L408/L447/L504 → L528 | 解析/隐式部署 task 类（L561/L590/L630），创建 `GridTaskSessionImpl`（L679）+ `ComputeTaskInternalFuture`（L697）+ `GridTaskWorker`（L714） |
| 4 | `GridTaskWorker#body` | `proc/task/GridTaskWorker.java` L469 | 实例化 task、取 topology、拿 load balancer、资源注入后调 `task.map(shuffledNodes, arg)`（L516） |
| 5 | `processMappedJobs` → `sendRequest` | 同文件 L575/L1345 | 生成 jobId/sibling，注册 `jobRes` 表，把每个 job 打包成 `GridJobExecuteRequest` 发 `TOPIC_JOB`（L1429）或本地短路（L1417） |
| 6 | `GridJobProcessor#processJobExecuteRequest` | `proc/job/GridJobProcessor.java` L1191 | worker 端：解析 deployment（L1220-1230）、重建 session、创建 `GridJobWorker`（L1304）、初始化后派发执行 |
| 7 | `GridJobWorker#execute0` → `job.execute()` | `proc/job/GridJobWorker.java` L537/L604 | 在 deployment classLoader 内执行用户 job（`U.wrapThreadLoader` L598） |
| 8 | `GridJobWorker#finishJob` | 同文件 L835-937 | 打包 `GridJobExecuteResponse`（L901-909），回 `TOPIC_TASK`（L936）/per-task 有序 topic（L924）/本地直调（L933） |
| 9 | `GridTaskProcessor#processJobExecuteResponse` → `GridTaskWorker#onResponse` | `GridTaskProcessor.java` L1061-1087；`GridTaskWorker.java` L721 | 按 session 路由到 taskWorker，调 `task.result()` 得 policy（L857 → L1060） |
| 10 | `onResponse` 状态机 → `reduce`/failover | `GridTaskWorker.java` L895-971 | WAIT/REDUCE/FAILOVER 三分支；全到齐或 policy=REDUCE 进 `reduce()`（L1134），FAILOVER 走 `GridFailoverManager`（L1208）后重发（L1271-1288） |

### 1.2 入口与 task 启动

- 同步 `execute` 就是异步 `executeAsync(...).get()`（`saveOrGet`，`IgniteComputeImpl.java` L220）；`executeAsync(taskName/taskCls/task)` 三种重载最终都进 `GridTaskProcessor.startTask(taskName, taskCls, task, sesId, arg, opts)`（`GridTaskProcessor.java` L421/L454/L513）。
- `startTask` 做四类部署判定：按 taskName → `ctx.deploy().getDeployment(taskName)`（L561，从 deployment SPI 反查类）；按 taskCls/task → `ctx.deploy().deploy(cls, ldr)` 隐式/显式部署（L590/L630）。task 未部署成功时 future 直接以 `IgniteDeploymentCheckedException` 完成。
- `@ComputeTaskMapAsync` 注解决定 map 阶段在当前线程还是 public executor 上跑（L750-768，默认当前线程 `taskWorker.run()` L768）。

### 1.3 task 状态机

`GridTaskWorker` 的状态只有 4 态（L136-148）：`WAITING → REDUCING → REDUCED → FINISHING`（`FINISHING` 在 `finishTask` 内设置，L1617-1620）。核心不变量：**所有 `jobRes` 的处理都要求 state==WAITING**（L610/L750/L872），进入 REDUCING 后到达的响应被丢弃（L750-756）。`result cache`（`@ComputeTaskNoResultCache` 可关）决定 `jobRes` 是否保留已完成的 job 结果（L603-604/L821-822）。

### 1.4 map 与派发

- `getTaskTopology()`（L685）取 projection 节点并 `Collections.shuffle`（L700-702）——这就是 map 前的随机化，防止用户 task 总把第一个 job 发给同一节点。
- `processMappedJobs`（L575）为每个 job 生成 `IgniteUuid` jobId、`GridJobSiblingImpl`、`GridJobResultImpl`（L596-600），**先把全部 job 登记进 `jobRes` 再发送**（L617-634，注释明确是为避免"结果先于发送到达"的竞态），且把本地 job 挪到发送列表末尾（L643-661）。
- `sendRequest`（L1345）：目标节点已离开 → 伪造一个带 fake exception 的 `GridJobExecuteResponse` 喂给 `onResponse`（L1359-1373），从而自动触发 failover；构造 `GridJobExecuteRequest` 时携带 sesId/jobId/task 名、job 实例、siblings、session/job 属性、`dep.classLoaderId()/deployMode()/participants()`、affinity 信息（L1389-1414）。**本地短路条件**：目标即本节点且未开 `isMarshalLocalJobs` → 直接 `ctx.job().processJobExecuteRequest(localNode, req)`（L1416-1417），零序列化零网络。

### 1.5 worker 端执行

- `GridJobProcessor` 在 start 时注册 `TOPIC_JOB`（jobExecLsnr）与 `TOPIC_JOB_CANCEL`（cancelLsnr）（L427-428）；`GridTaskProcessor` 注册 `TOPIC_TASK`（响应）、`TOPIC_TASK_CANCEL`、`TOPIC_JOB_SIBLINGS`（L202-204）。
- `processJobExecuteRequest`（L1191）：按 `req.forceLocalDeployment()` 决定 `getLocalDeployment` 还是 `getGlobalDeployment(deploymentMode, taskName, taskClassName, userVersion, sndNodeId, classLoaderId, participants)`（L1220-1230）——这是 compute 与部署体系的接缝（§4.2）。deployment 缺失 → 立即回错误响应（L1385-1393）。
- 执行派发三分支（L1326-1381）：internal job → `runSync`；`jobAlwaysActivate`（默认，因默认 collision SPI 是 Noop，`jobAlwaysActivate = !ctx.collision().enabled()`，L343）→ `executeAsync` 或同步跑；启用了 collision SPI → 先入 `passiveJobs` 再 `handleCollisions()`（L1361-1366）。**复刻课可以先只实现"总是激活"分支**。
- `GridJobWorker` 继承 `GridWorker`；`execute0`（L537）里 `job.execute()`（L604）被包在 deployment classLoader 里执行；取消由 `JobCancelListener`（L2173-2182）→ `cancelJob(sesId, jobId, sys)` 完成中断。
- `finishJob`（L835）：组装 `GridJobExecuteResponse`（含结果、异常、job 属性、cancelled 标志、可选 retry 版本，L901-909）。回程三通道：fullSupport（session 属性同步）→ `sendOrderedMessage(taskTopic)` 保序（L920-930）；目标本地 → `ctx.task().processJobExecuteResponse` 直调（L932-933）；否则 `sendToGridTopic(sndNode, TOPIC_TASK, jobRes, SYSTEM_POOL/MANAGEMENT_POOL)`（L936）。

### 1.6 结果处理与 failover

- `onResponse`（L721）先做去重/防乱序（occupied 标志 + `delayedRess` 延迟队列，L797-811），再调 `result(jobRes, results)` → `task.result(...)`（L857/L1060）。`ComputeTaskAdapter#result` 默认策略：异常属于 `ComputeExecutionRejectedException`/`ClusterTopologyException`/`ComputeJobFailoverException` 才返回 FAILOVER，用户业务异常直接抛出令 task 失败，否则 WAIT（`compute/ComputeTaskAdapter.java` L89-107）。
- 状态机三分支（L895-950）：`REDUCE` → state=REDUCING；`WAIT` → 收齐全部结果才 REDUCE（L911-916）；`FAILOVER` → 调 `failover(res, jobRes, top)`（L945）。
- `failover` → `ctx.failover().failover(ses, jobRes, top, affPartId, affCacheName, mapTopVer)`（L1208）。`GridFailoverManager.failover` 只是 SPI 门面：`getSpi(taskSes.getFailoverSpi()).failover(new GridFailoverContextImpl(...), top)`（`managers/failover/GridFailoverManager.java` L67-80）。
- 默认 SPI 是 `AlwaysFailoverSpi`（`cfg/IgniteConfiguration.java` L2192-2196 javadoc；实际缺省注入在 `internal/IgnitionEx.java` L2052-2053）。其 `failover`（`spi/failover/always/AlwaysFailoverSpi.java` L192）：affinity 任务 → `mapPartitionToNode` 重新定位 primary（L206-232）；普通任务 → 用 jobContext 属性 `FAILED_NODE_LIST_ATTR` 累积失败节点、从 topology 剔除后交给 load balancer 选新节点（L234-266）；尝试上限 `maxFailoverAttempts` 默认 5（L101/L241-246）。拿到新节点后 `sendFailoverRequest` → 重走 `sendRequest`（L1271-1288）。
- 发送前/发送中节点死亡、或响应是 fake 的场景全部汇入同一条 onResponse→failover 路径——**failover 不是异常分支而是 result policy 的正常输出**。
- 取消：task 级 `cancelChildren`（L1293）对本地 job 调 `ctx.job().cancelJob`（L1310），对远端发 `GridJobCancelRequest` 到 `TOPIC_JOB_CANCEL`（L1316-1319）。

### 1.7 Load balancing

- `GridTaskWorker#body` 为 task 申请 `ComputeLoadBalancer`（L504），注入到 `@LoadBalancerResource` 字段（L512）——内置闭包 task T1/T2 就这样拿到 lb（`GridClosureProcessor.java` L1041-1042/L1059/L1090）。
- `GridLoadBalancerManager#getBalancedNode`（L70-82）选 SPI（internal task 强制 RoundRobin，L78-79）；`getLoadBalancer`（L90-110）返回支持 exclNodes 过滤的适配器。默认 SPI 为 `RoundRobinLoadBalancingSpi`（`IgnitionEx.java` L2055-2056），`isPerTask=true` 时每个 session 一个 `RoundRobinPerTaskLoadBalancer`（`spi/loadbalancing/roundrobin/RoundRobinLoadBalancingSpi.java` L300-315）。
- 闭包模式下 `GridClosureProcessor#absMap` 自己完成映射：BROADCAST = 每个 job × 每个节点；BALANCE = 每个 job 问一次 `lb.getBalancedNode(job, null)`（L227-245），因此 run/call 默认就是轮询分发。

### 1.8 消息类清单（TOPIC_TASK / TOPIC_JOB 族）

| 消息类 | 声明 | Topic | 方向/用途 |
|---|---|---|---|
| `GridJobExecuteRequest` | `internal/GridJobExecuteRequest.java` L43 | `TOPIC_JOB`（`internal/GridTopic.java` L38） | master → worker，携带 job 实例 + session/sibling/属性 + deployment 标识 |
| `GridJobExecuteResponse` | `internal/GridJobExecuteResponse.java` L40 | `TOPIC_TASK`（L44）或 `taskTopic`（per-task 有序子 topic，`GridTaskWorker.java` L924） | worker → master，job 结果/异常/取消/重试 |
| `GridJobCancelRequest` | `internal/GridJobCancelRequest.java` L28 | `TOPIC_JOB_CANCEL`（L50） | master → worker，取消单个 job |
| `GridTaskCancelRequest` | `internal/GridTaskCancelRequest.java` L27 | `TOPIC_TASK_CANCEL`（L53） | 发起端 → task 节点，取消整个 task |
| `GridJobSiblingsRequest/Response` | `internal/GridJobSiblingsRequest.java` L27；`GridJobSiblingsResponse.java` L32 | `TOPIC_JOB_SIBLINGS`（L41） | 查询 task 的 job siblings（checkpoint/session 场景） |
| `GridTaskSessionRequest` | `internal/GridTaskSessionRequest.java` L31 | `TOPIC_TASK` | session 属性同步（fullSupport） |

---

## 2. IgniteServiceProcessor：分布式服务网格

### 2.1 三张注册表与两条通道

- `IgniteServiceProcessor`（`proc/service/IgniteServiceProcessor.java` L128）维护：`locServices`（本节点实例，L151）、`registeredServices`（**discovery 线程更新**的"目标状态"，L167）、`deployedServices`（**deployment worker 更新**的"已达成状态"，L187）——两态追赶的设计与 cache 的 exchange 思想同构。
- 控制面：`start()` 注册 4 个 discovery 自定义事件监听（L261-291）：`ServiceChangeBatchRequest` → `processServicesChangeRequest`；`DynamicCacheChangeBatch`（cache 停启会影响 affinity 服务）；`ServiceClusterDeploymentResultBatch` → `processServicesFullDeployments`。
- 数据面：`ServiceDeploymentManager` 注册 `TOPIC_SERVICES` 消息监听（`proc/service/ServiceDeploymentManager.java` L113）与 `EVT_NODE_JOINED/LEFT/FAILED/EVT_DISCOVERY_CUSTOM_EVT` discovery 监听（L110-111），并启动单线程 `services-deployment-worker`（L128，worker 类 L443）。

### 2.2 部署：discovery 自定义消息 + 二阶段收敛

1. **发起**：所有 `deployClusterSingleton/deployNodeSingleton/deployMultiple/deployKeyAffinitySingleton` 都汇入 `deployAll`（L615-668）。服务实例用 **JDK marshaller** 序列化成字节存进 `LazyServiceConfiguration`（`marsh = ctx.marshallerContext().jdkMarshaller()` L243；`U.marshal(marsh, cfg.getService())` L702-704）——服务网格不依赖 binary marshaller。随后生成 `srvcId`，把 `ServiceDeploymentRequest` 打包成 `ServiceChangeBatchRequest`，`ctx.discovery().sendCustomEvent(msg)` 广播（L808-821）。
2. **登记**：每个节点在 discovery 线程收到后 `processServicesChangeRequest`（L1776）：校验权限/重复名/cache 存在性，`registerService(desc)` 写入 `registeredServices`（L1837），并把 `ServiceDeploymentActions`（to deploy/to undeploy）挂回消息（L1879-1887）。
3. **收敛**：discovery 事件触发 `ServiceDeploymentManager` 为**该拓扑版本**创建/排队一个 `ServiceDeploymentTask`（`checkClusterStateAndAddTask` L215-223；cluster inactive 则丢弃、transition 则 pending）。每个节点的 task 把本地各服务实例数打包成 `ServiceSingleNodeDeploymentResultBatch`，发给协调者（`ServiceDeploymentTask.java` L366-412，本地是 crd 则直调）；crd 汇总全部节点后计算 `expDeps`（期望拓扑），组装 `ServiceClusterDeploymentResultBatch` 并 `ctx.discovery().sendCustomEvent(msg)` 广播（L566-574）；各节点 `onReceiveFullDeploymentsMessage` → `srvcProc.updateServicesTopologies(fullTops)` + 本地数量不符则 `srvcProc.redeploy(...)`（L453-493）。
4. **新节点加入**：全量服务信息随 discovery common data 传播——`collectGridNodeData` 把 `registeredServices` 塞进 `ServiceProcessorCommonDiscoveryData`（L382-392），新节点 `onGridDataReceived` 逐个 `registerService`（L395-411）；静态配置服务经 `collectJoiningNodeData` 随 join 数据上行（L414-417）。

### 2.3 分配算法与单例语义

`IgniteServiceProcessor#reassign`（L1157-1270）由 crd 在 `ServiceDeploymentTask` 内调用（L589-592）：

- **affinity 单例**（`cacheName+affKey` 非空）：`ctx.affinity().mapKeyToNode(cacheName, affKey, topVer)` 定位唯一节点（L1172-1180）——与 key 的 primary 一致，天然稳定。
- **普通分配**：`totalCnt/size` 均摊、余数随机（种子 `srvcId.localId()`，L1215）加一；若旧分配存在且份数相同则**优先沿用旧节点**（L1217-1231，注释 "Avoid redundant moving of services"）。`deployClusterSingleton` = totalCnt=1/maxPerNode=1（L624-626），`deployNodeSingleton` = totalCnt=0/maxPerNode=1（L615-617），`maxPerNodeCnt` 封顶（L1201-1204）。

### 2.4 本地部署/取消：ServiceContext 生命周期

`redeploy`（L1282-1316）对齐"本节点应持有份数"（L1288）：多则 `cancel`（L1295-1298），少则为每份新建 `ServiceContextImpl(name, UUID.randomUUID(), cacheName, affKey, Executors.newSingleThreadExecutor(threadFactory), ...)`（L1304-1309）——**每个服务实例独占单线程 executor**，`Service.execute()` 就跑在该线程上（L1356-1359）。实例本身每份都是反序列化的新副本（`copyAndInject` L1324），`init` 同步执行（L1327）、失败则整体回滚（L1331-1340）。`ServiceContextImpl`（`proc/service/ServiceContextImpl.java` L38）持有 name/execId/cacheName/affKey/executor/方法反射缓存（L46-63）与 cancelled 标志（L77）；`cancel(ctx)` 先翻标志再调 `srvc.cancel(ctx)`（`IgniteServiceProcessor.java` L1490-1499）。

### 2.5 服务调用：本地直调 / compute 搭车，sticky 语义

`ignite.services().serviceProxy(name, itf, sticky, timeout)` → `new GridServiceProxy(...)` 动态代理（L1096-1107）。`invokeMethod`（`GridServiceProxy.java` L181-285）：

- **定位节点** `nodeForService(name, sticky)`（L360-380）：sticky=true 时优先用代理内缓存的 `rmtNode`（AtomicReference CAS，L363-374）；否则 `randomNodeForService`（L387-419）：本节点有实例则本地，否则查 `ctx.service().serviceTopology(name, waitTimeout)` 后随机。
- **本地**：直接反射调用 `callServiceLocally`（L204-218），零开销。
- **远程**：`ctx.closure().callAsync(GridClosureCallMode.BROADCAST, new ServiceProxyCallable(methodName, name, paramTypes, args, callAttrs), options(Collections.singleton(node)).withPool(SERVICE_POOL).withFailoverDisabled().withTimeout(waitTimeout))`（L221-228）——**服务调用没有独立 RPC 协议，就是一条定位到单节点的 compute 闭包任务**；`ServiceProxyCallable#call` 在远端按名查 `ServiceContextImpl`、反射调方法并把结果 marshal 成字节返回（L569-584）。
- **重试**：`ClusterTopologyCheckedException`/`GridServiceNotFoundException` 视为可重试：清空 sticky 缓存、sleep 10ms 重来，直到 `waitTimeout`（L240-279）。failover 被 `withFailoverDisabled()` 显式关掉，因为"换节点"由代理自身循环负责（节点上可能根本没有该服务）。

---

## 3. DataStreamer：批量写入路径

### 3.1 调用链总览（8 步）

| # | 组件 | 位置 | 一句话 |
|---|---|---|---|
| 1 | `DataStreamerImpl#addData → addDataInternal` | `proc/datastreamer/DataStreamerImpl.java` L593-711 | 线程级缓冲 `threadBufMap`，攒到 `perThreadBufferSize`（默认 4096）触发 `loadData`（L690-691） |
| 2 | `loadData → load0` | 同文件 L732/L815 | 按 affinity 把 entries 分桶到 per-node `Buffer`（L933-940），失败进 remap 队列（L989-1041） |
| 3 | `Buffer#update` | 同文件 L1627 | 分区 stripe（`part % stripes.length`，L1645），攒到 `perNodeBufferSize`（默认 512）flush（L1678-1682） |
| 4 | `Buffer#submit` | 同文件 L1906 | 本地 → `localUpdate`；远端 → marshal + p2p deploy（L1965-1984）+ `DataStreamerRequest` 发 `TOPIC_DATASTREAM`（L1995-2015） |
| 5 | `DataStreamProcessor#processRequest` | `proc/datastreamer/DataStreamProcessor.java` L203 | server 端：等 affinity 就绪（L215-240）、解析 deployment/updater（L255-296）、`localUpdate` |
| 6 | `localUpdate` → `DataStreamerUpdateJob#call` | 同文件 L311-408；`DataStreamerUpdateJob.java` L99 | server 端同步执行 job：skipStore/keepBinary 落到 cache proxy（L107-111），`receiver.receive(cache, entries)`（L142/L145） |
| 7a | 汇合点 A（`allowOverwrite=true`） | `DataStreamerCacheUpdaters.java` L88-98 | `updateAll` → `cache.putAll(putMap)`（L97）→ 与用户 `cache.put` 同一条数据面链路 |
| 7b | 汇合点 B（默认 `IsolatedUpdater`） | `DataStreamerImpl.java` L2238-2382 | 绕过 put：`internalCache.entryEx(key)`（L2307）→ `entry.initialValue(..., DR_LOAD/DR_PRELOAD, ...)`（L2329-2337）直接初始化 entry |
| 8 | 回执 | `DataStreamProcessor.java` L417-433 | `DataStreamerResponse` 沿 per-streamer response topic 回发起端，`Buffer#onResponse` 释放并行信号量 |

### 3.2 client 端缓冲模型（两级 + 三种 flush 触发）

- **线程缓冲**：`addDataInternal` 按 `threadBufMap` 攒批，阈值 `bufLdrSzPerThread`（`DFLT_PER_THREAD_BUFFER_SIZE=4096`，`IgniteDataStreamer.java` L137）触发提交（L662-695）；`tryFlush` 会先清空所有线程缓冲（L1277-1297）。
- **节点缓冲**：`Buffer`（L1556）按目标节点组织，内部再按分区 stripe 切片（L1613-1616），单 stripe 攒到 `bufSize`（`DFLT_PER_NODE_BUFFER_SIZE=512`，`IgniteDataStreamer.java` L134）即 `submit`（L1678-1682）。并行度由 `Semaphore perNodeParallelOps` 限流（L1611；默认 `远端 CPU 数 × 8`，`DFLT_PARALLEL_OPS_MULTIPLIER=8`，`IgniteDataStreamer.java` L131；`DataStreamerImpl.java` L1604-1609）。
- **自动 flush**：`DataStreamProcessor` 启动名为 `grid-data-loader-flusher` 的守护线程，从 `DelayQueue` 周期取 streamer 执行 `tryFlush`（`DataStreamProcessor.java` L102-127）；`autoFlushFrequency` 控制入队/出队（`DataStreamerImpl.java` L572-588）。手动 `flush()`（L1256）等待所有在途请求完成。

### 3.3 请求与 server 端

- 远端请求消息是 **`DataStreamerRequest`**（`DataStreamerRequest.java` L43；注意 2.18 源码中**不存在** `DataStreamDataPayload` 类）：内含 reqId、response topic 字节、cacheName、updater（`StreamReceiver` 序列化字节）、entries、`skipStore/keepBinary`、deployment 标识（deployMode/类名/用户版本/participants/classLoaderId）与 topology 版本（`DataStreamerImpl.java` L1995-2012）。
- server 端 `processRequest`（`DataStreamProcessor.java` L203）：若请求的 affinity 版本更新则等 `affinityReadyFuture` 后重入（L215-240）；deployment 解析与 compute job 完全同构（`getGlobalDeployment(...)`，L258-266）；然后 `localUpdate` **在 IO 线程上同步**执行 `job.call()`（L364-395），无独立执行线程池——并发来自发送端的 stripe 切分与 `DATA_STREAMER_POOL` IO 线程。
- 默认（`allowOverwrite=false`，即 `IsolatedUpdater`）时 server 端还要拿 topology 读锁并注册 `addDataStreamerFuture`（L331-373），保证 exchange 与 streamer 互斥；若 topology 已越过请求版本则回 remap 错误，client 重新定位后重发（`DataStreamerImpl.java` L989-1041，上限 `maxRemapCnt` L978-981）。

### 3.4 与单条 put 的汇合点（关键结论）

- **`allowOverwrite(true)`**：updater 换成 `DataStreamerCacheUpdaters.individual()/batched()/batchedSorted()`（`DataStreamerImpl.java` L508），`DataStreamerUpdateJob` 拿到的是普通 `IgniteCacheProxy`（`cacheNoGate()`，`DataStreamerUpdateJob.java` L103），`skipStore` 通过 `withSkipStore()` 实现（L107-108），最终 `cache.putAll(...)`（`DataStreamerCacheUpdaters.java` L97）——与报告 03 的 put 主链路在 `IgniteCacheProxyImpl#putAll`（`proc/cache/IgniteCacheProxyImpl.java` L1354；单条 put 为 L1283）→ `GridCacheAdapter#putAll`（L2465）**完全汇合**。
- **默认（不覆盖）**：`IsolatedUpdater#receive`（L2244）直接下探到 cache 内部：`proxy.context().cache()` 拿 `GridCacheAdapter`（L2250），逐条 `entryEx(key, topVer)` + `initialValue(val, ver, ttl, expiry, ..., DR_LOAD/DR_PRELOAD, ...)`（L2307/L2329-2337；接口 `GridCacheEntryEx#initialValue` 在 `proc/cache/GridCacheEntryEx.java` L607，实现 `GridCacheMapEntry.java` L2487），跳过 near cache、锁、版本协商与 backup 消息——这是"初始加载"语义（等价 preload/dr 路径），最后统一 `wal().flush`（L2373-2374）。**所以 DataStreamer 不是"更快的 put"，而是一条只在 primary 本地生效、不覆盖已有值的旁路**。

---

## 4. 部署与 Peer Class Loading

### 4.1 GridDeploymentManager 结构

`managers/deployment/GridDeploymentManager.java`（L52）= SPI 门面（`GridManagerAdapter<DeploymentSpi>`）+ 三个 store + 一个通信器：

| 组件 | 位置 | 职责 |
|---|---|---|
| `locStore`（`GridDeploymentLocalStore`） | L54；`GridDeploymentLocalStore.java` L62 | 本节点显式部署；**唯一与 SPI 直接对话的 store**（`spi.register` L213/L376、`spi.findResource` L174、`spi.unregister` L414，并在 start 时 `spi.setListener(new LocalDeploymentListener())` L80） |
| `ldrStore`（`GridDeploymentPerLoaderStore`） | L57 | ISOLATED/PRIVATE：每个 `GridDeploymentClassLoader` 一个 `IsolatedDeployment`（`GridDeploymentPerLoaderStore.java` L418） |
| `verStore`（`GridDeploymentPerVersionStore`） | L60 | SHARED/CONTINUOUS：按 userVersion 共享，`SharedDeployment` + participants 记录（L1115） |
| `comm`（`GridDeploymentCommunication`） | L63 | p2p 类字节传输（`TOPIC_CLASSLOAD`） |

关键退化：**p2p 关闭且 SPI 带 `@IgnoreIfPeerClassLoadingDisabled`（`LocalDeploymentSpi` 即如此）时**，manager 直接构造单一 `LocalDeployment`（ctor L74-87），`enabled()` 返回 false（L161-163），`deploy/getGlobalDeployment` 全部短路返回它（L301-309/L425-426）——整个 store/SPI/通信体系都不参与。默认配置正是如此：`DFLT_P2P_ENABLED=false`（`cfg/IgniteConfiguration.java` L116/L338），`DFLT_DEPLOYMENT_MODE=SHARED`（L146/L466）。

### 4.2 部署生命周期与四种模式

- 发起端 `deploy(cls, ldr)`（L251）：lambda 类归一到外围类（L257-269）；若 `ldr` 已是 p2p 的 `GridDeploymentClassLoader` 则按 classLoaderId 查三个 store 复用（L271-299）；否则 `locStore.explicitDeploy`（L312）。task 执行前 `GridTaskProcessor.startTask` 一定先调它（L590/L630）。
- worker 端 `getGlobalDeployment(depMode, rsrcName, clsName, userVer, sndNodeId, clsLdrId, participants, nodeFilter)`（L416）：p2p 关 → locStore 查名（L444-448）；SHARED/CONTINUOUS → 先查 `verStore` 缓存、再尝试复用本地同版本部署（受 `peerClassLoadingLocalClassPathExclude` 影响，L459-515）、最后 `verStore.getDeployment(meta)` 触发真正的远端加载（L517）；PRIVATE/ISOLATED → `ldrStore.getDeployment`（L521-524）。`GridJobExecuteRequest.forceLocalDeployment`（internal job）则走 `getLocalDeployment`（`GridJobProcessor.java` L1220-1231）。
- `GridDeployment`（抽象类，`managers/deployment/GridDeployment.java`）提供引用计数（`acquire()` L271 / `release()` L291）、`deployedClass`（L454）与 classLoader 生命周期；job 请求持有它直到 job 结束（`GridJobProcessor.java` L1256/L1397）。

### 4.3 p2p 类加载机制（TOPIC_CLASSLOAD）

链路：worker 反序列化 job 时 classloader 缺类 → `GridDeploymentClassLoader#findClass`（`GridDeploymentClassLoader.java` L519）先查本地 deployment（L526-533），miss 则 `sendClassRequest`（L538）→ `GridDeploymentCommunication#sendResourceRequest`（L56 文件；L384）：构造 `GridDeploymentRequest(resTopic, clsLdrId, rsrcName, false)`，附带**请求节点链 `nodeIds` 防循环**（L390-403），发 `TOPIC_CLASSLOAD`（P2P_POOL，L425），同步等 `GridDeploymentResponse`（L430-446）。对端 `processResourceRequest`（L185）按 classLoaderId 找 deployment、`Class.forName` 校验后把类字节塞进 response（L203-224；`@IgniteNotPeerDeployable` 黑名单 L221）。拿到字节后 `defineClass`（`GridDeploymentClassLoader.java` L545-546）。SHARED 模式首次还会用临时 `GridDeploymentClassLoader` 探测远端是否真有该类（`checkLoadRemoteClass`，`GridDeploymentPerVersionStore.java` L685-729）。

### 4.4 与 marshaller 的接缝：GridMarshallerMappingProcessor

`proc/marshaller/GridMarshallerMappingProcessor.java`（L76）解决的是**typeId ↔ 类名映射**的集群一致性，不是类字节：start 时注册 discovery 自定义事件 `MappingProposedMessage/MappingAcceptedMessage`（L117-119）与 `TOPIC_MAPPING_MARSH`（client 收 `MissingMappingResponseListener`、server 收 `MissingMappingRequestListener`，L121-124）；server 收到缺失映射查询后 `resolveMissedMapping` 并回 `MissingMappingResponseMessage`（L172-187）。只有类名确定后 p2p classloader 才能去拉字节——两者是先后关系而非替代关系。JdkMarshaller（默认）下该 processor 基本闲置，binary 场景才活跃。

### 4.5 LocalDeploymentSpi

`spi/deployment/local/LocalDeploymentSpi.java`：纯 JVM 内注册表 `ldrRsrcs`（ClassLoader → 资源名映射，L85），`register(ldr, cls)` 登记（L189）、`findResource` 遍历查找（L118-130）。带 `@IgnoreIfPeerClassLoadingDisabled`（L69）且已 `@Deprecated`（L70，将被 IEP-144 IgniteClassPath 取代）。**默认部署 SPI 即它**：不开 p2p 时它配合 §4.1 的 `LocalDeployment` 退化路径，等于"没有部署体系"。

### 4.6 modules/urideploy：UriDeploymentSpi

`modules/urideploy/.../UriDeploymentSpi.java`（L316）：

- **scanner 家族**：默认注册 `UriDeploymentFileScanner`（`file://`，`acceptsURI` L52-56，默认扫描周期 5s，`DFLT_SCAN_FREQ=5000` L46）与 `UriDeploymentHttpScanner`（`http://`，L80，默认 300s，L71）（`UriDeploymentSpi.java` L633-638）。**2.18 不存在 classes:// 或 maven:// scanner**（`scanners/` 目录只有 file/http 两子包）。
- **扫描机制**：每个 URI 一个 `UriDeploymentScannerManager`，内部 `IgniteSpiThread "grid-uri-scanner"` 循环 sleep(freq) 再 scan（`UriDeploymentScannerManager.java` L108-143）；freq 可用 URI userinfo 的 `;freq=ms` 参数覆盖（`getFrequencyFromUri` L694-709）。发现新/更新的 `.gar`/`.jar`（FilenameFilter L557-565）→ `GridUriDeploymentFileProcessor.processFile` 解包到临时目录（L590-595；`GridUriDeploymentFileProcessor.java` L72）→ `newUnitReceived` 注册 `GridUriDeploymentUnitDescriptor`；删除则触发 undeploy（L603-619）。
- **与 GridDeploymentManager 的接缝**：完全走 `DeploymentSpi` 接口——`findResource(rsrcName)`（L712）在 `unitLoaders`（后部署优先）里按类名/别名加载并返回 `DeploymentResourceAdapter`（L730-766），首次调用会阻塞等所有 scanner 完成首轮扫描（L715-728）；`register/unregister` 处理显式部署。本地 store 通过 `spi.setListener(LocalDeploymentListener)` 感知 SPI 侧 undeploy（`GridDeploymentLocalStore.java` L80）。task 按 taskName 执行时 `ctx.deploy().getDeployment(taskName)` → locStore → `spi.findResource` 即可找到 URI 部署的类（`GridTaskProcessor.java` L561）。

---

## 5. 分级结论：compute/服务面复刻的必修闭包与可简化项

| 分级 | 组件链 | 依据 |
|---|---|---|
| **必修（缺了就不通）** | `IgniteComputeImpl`（薄门面）→ `GridTaskProcessor.execute/startTask` → `GridTaskWorker`（4 态状态机 + `jobRes` 表）→ `GridJobProcessor.processJobExecuteRequest` → `GridJobWorker.execute0/finishJob` + 消息 `GridJobExecuteRequest`/`GridJobExecuteResponse`（TOPIC_JOB/TOPIC_TASK）+ 本地短路 | 单条 task 的最小闭环：§1.1 十步里去掉任何一环，execute→map→job→result→reduce 都不成立 |
| **必修（failover 语义）** | `ComputeJobResultPolicy` 三值协议 + `GridFailoverManager` 门面 + 一个简单 failover SPI（failedNodes 列表 + 上限） | `ComputeTaskAdapter#result` 默认把拓扑/拒绝异常翻译成 FAILOVER（§1.6）；master 侧 fake response 机制依赖它 |
| **必修（服务最小闭包）** | `IgniteServiceProcessor` 的 deployAll→discovery 自定义消息→`ServiceDeploymentActions`；单节点 `reassign`（先做 cluster singleton + totalCount 分摊）；`redeploy` 的 init/execute/cancel 与 per-instance executor；`GridServiceProxy` 本地直调 + 远程 compute 搭车 | §2 的控制面可以只支持"单协调者 + 全量重算"，但 ServiceContext 三回调与 proxy 通道不可省 |
| **必修（DataStreamer 最小闭包）** | `addData` 线程缓冲 → per-node Buffer → `DataStreamerRequest`（TOPIC_DATASTREAM）→ server `DataStreamerUpdateJob` → 汇合点 A（`cache.putAll`） | 汇合点 A 让 streamer 复用数据面而不重造写入；默认 `IsolatedUpdater`/`initialValue` 旁路可后置 |
| **必修但可极简（部署）** | `GridDeploymentManager` 的退化路径：p2p 关闭 + 单一 `LocalDeployment` + `LocalDeploymentSpi` | §4.1：默认配置下整个部署体系就是"JVM 内注册表"，先复刻这条即可跑通 compute |
| **可简化（compute）** | collision SPI 体系（`passiveJobs/handleCollisions` 只在自定义 collision SPI 时启用，默认 `jobAlwaysActivate=true`）；session fullSupport/`GridTaskSessionRequest` 属性同步；`GridJobSiblingsRequest` 族；checkpoint SPI；`@ComputeTaskMapAsync` 异步 map；连续 mapper（`TaskContinuousMapperResource`）；affinity call 的 retry 分支（`GridTaskWorker.java` L880-893） | 这些都是低频/兼容特性；`jobAlwaysActivate` 分支证明同步执行即可（§1.5） |
| **可简化（load balancing）** | 只实现 RoundRobin（global + perTask 两种粒度），跳过 Weighted/Adaptive SPI | internal task 本来就被强制 RoundRobin（§1.7） |
| **可简化（服务）** | 二阶段 `ServiceDeploymentTask`（单节点结果上报 + crd 汇聚 + 全量广播）可先降级为"crd 单点计算后广播"；`ServiceUndeploymentRequest`/批量 batch、动态 cache 联动、安全权限校验、statistics 指标 | 协议正确性不依赖全量二阶段，只依赖"registeredServices（目标态）与 deployedServices（现实态）最终一致" |
| **可简化（DataStreamer）** | stripe 切分（可退化为单 buffer）、`perNodeParallelOps` 信号量、`grid-data-loader-flusher` 自动 flush（可先只留手动 flush/close）、remap 队列 | 并发度与自动 flush 是吞吐优化不是正确性前提；remap 只需在 topology 变化时报错重试 |
| **可后置（部署进阶）** | p2p 类加载全套（`GridDeploymentCommunication`/`GridDeploymentClassLoader`/perLoader/perVersion store/participants 协议）、`GridMarshallerMappingProcessor`、`UriDeploymentSpi` | 默认关闭 p2p；`UriDeploymentSpi` 属独立模块单向依赖 core，接口（`DeploymentSpi`）留在内核即可 |

**一句话主干**：compute 面的内核是"`GridTaskWorker` 状态机 + `GridJobExecuteRequest/Response` 两种消息"；服务网格是在 discovery 上再造一个"目标态/现实态追赶"的部署状态机、执行面完全寄生在 compute 上；DataStreamer 是数据面的一条批量旁路，只在 `allowOverwrite=true` 时汇回 put 主链；部署体系在默认配置下可整体退化为一个 JVM 内注册表——四者可以按"compute → services(单例) → datastreamer(putAll 汇合) → p2p(可选)"的顺序分层复刻。

---

## 6. 引用文件清单（全部实际打开验证）

### modules/core（内部包）

- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridTopic.java`（topic 常量族）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgniteComputeImpl.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgniteComputeHandler.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgniteServicesImpl.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/IgnitionEx.java`（默认 SPI 注入）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridJobExecuteRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridJobExecuteResponse.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridTaskCancelRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridJobCancelRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridJobSiblingsRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridJobSiblingsResponse.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/GridTaskSessionRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/closure/GridClosureProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/task/GridTaskProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/task/GridTaskWorker.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/job/GridJobProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/job/GridJobWorker.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/loadbalancer/GridLoadBalancerManager.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/failover/GridFailoverManager.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/IgniteServiceProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceDeploymentManager.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceDeploymentTask.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/GridServiceProxy.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceContextImpl.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceChangeBatchRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceChangeAbstractRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceDeploymentRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceUndeploymentRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceClusterDeploymentResultBatch.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/service/ServiceSingleNodeDeploymentResultBatch.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamerImpl.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamerUpdateJob.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamerCacheUpdaters.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamerRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/datastreamer/DataStreamerResponse.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java`（putAll 汇合点）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheAdapter.java`（putAll L2465）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheEntryEx.java`（initialValue 接口）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheMapEntry.java`（initialValue 实现 L2487）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/processors/marshaller/GridMarshallerMappingProcessor.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentManager.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeployment.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentLocalStore.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentPerLoaderStore.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentPerVersionStore.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentCommunication.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentClassLoader.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentRequest.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentResponse.java`

### modules/core（公开 API / SPI / 配置）

- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/compute/ComputeTaskAdapter.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/IgniteDataStreamer.java`（默认参数常量）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/failover/always/AlwaysFailoverSpi.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/loadbalancing/roundrobin/RoundRobinLoadBalancingSpi.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/deployment/DeploymentSpi.java`（目录确认）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/deployment/DeploymentListener.java`（目录确认）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/deployment/IgnoreIfPeerClassLoadingDisabled.java`（目录确认）
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/spi/deployment/local/LocalDeploymentSpi.java`
- `vendors/ignite/modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java`

### modules/urideploy

- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/UriDeploymentSpi.java`
- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/GridUriDeploymentFileProcessor.java`
- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/UriDeploymentScannerManager.java`
- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/file/UriDeploymentFileScanner.java`
- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/http/UriDeploymentHttpScanner.java`
- `vendors/ignite/modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/`（目录结构：仅 file/http 两个 scanner 子包）

---

## 7. 对复刻课的启示

1. **先做 task 后做闭包**：闭包 API（run/call/broadcast）只是 11 个模板 task 的语法糖，`GridClosureProcessor#absMap` 用 `GridClosureCallMode` 两分支（BROADCAST 全连接、BALANCE 问 lb）就完成了映射（§1.7）。课程顺序应是"手写 ComputeTask 跑通 → 再加闭包适配层"。
2. **failover 是 policy 不是异常处理**：整个 failover 面收敛为"`task.result()` 返回 FAILOVER → 换节点重发 `sendRequest`"，配合 master 侧伪造 response（节点离开时）这一技巧，可以用不到百行代码实现 AlwaysFailoverSpi 的核心语义（§1.6）。
3. **服务网格是"discovery 状态机 + compute 数据面"的组合拳**，没有新传输协议。复刻时先做目标态/现实态两张表 + 每拓扑事件重算分配，proxy 直接复用第 1 步的闭包通道；per-instance 单线程 executor 与 init/execute/cancel 三回调是服务生命周期的教学重点（§2.4-2.5）。
4. **DataStreamer 教学价值在"汇合点"**：`allowOverwrite=true` 时它就是客户端攒批 + `cache.putAll`，和数据面课程无缝衔接；默认 `IsolatedUpdater` 则展示了一条"绕过事务/锁、直接 initialValue + WAL flush"的加载旁路，正好复习报告 03 中 entry 写入的底层（§3.4）。
5. **部署体系可以整体推后**：默认配置下 p2p 关闭、deployment SPI 是已废弃的 `LocalDeploymentSpi`，`GridDeploymentManager` 退化为单一 `LocalDeployment`（§4.1）。建议课程主线先实现这个退化路径，把 p2p（TOPIC_CLASSLOAD + classloader 链防环）与 `UriDeploymentSpi`（file scanner + freq 参数）作为独立选修课。
6. **线程模型清单**：compute 面 = public/system executor（map）+ collision 派发（默认直跑）+ job worker 线程；服务面 = discovery 线程（登记）+ `services-deployment-worker`（收敛）+ 每服务实例单线程；streamer 面 = flusher 守护线程 + IO 线程同步执行。默认配置下没有复杂的异步 job 池，复刻难度低于直觉。
