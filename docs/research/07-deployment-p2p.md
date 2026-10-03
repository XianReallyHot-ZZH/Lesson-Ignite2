# 07 · 部署子系统专项：GridDeploymentManager 状态机、peer class loading 管道与 UriDeploymentSpi

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文是部署**专项**深挖，只回答四个问题：① `GridDeploymentManager` 状态机；② peer class loading（p2p）管道与 marshaller 映射协议；③ `modules/urideploy` 内部；④ 复刻分级结论。compute/task 生命周期本身（job 调度、future 链）不在范围，只在"接缝"处引用。正文短路径约定：`managers/deployment/` = `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/`；`spi/deployment/` = `modules/core/src/main/java/org/apache/ignite/spi/deployment/`；`uri/` = `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/`；**文末"引用文件清单"给出全部完整可定位路径**（均已逐一打开验证），行号以当前 submodule 内容为准。

---

## 0. 全景与两个先决事实

部署子系统是三层结构加一条独立旁路：

```
DeploymentSpi（spi/deployment/DeploymentSpi.java，4 个方法 L72/L92/L102/L110）
  ├─ LocalDeploymentSpi（默认，@Deprecated）        ← SPI 层：本地注册表
  └─ UriDeploymentSpi（modules/urideploy）           ← SPI 层：URI 扫描 + GAR/JAR
        ↓ register / findResource / unregister / listener 回调
GridDeploymentManager（3 个 store + comm）           ← 管理层：发现/创建/版本化/废弃
  ├─ GridDeploymentLocalStore   本地部署（按 alias 索引）
  ├─ GridDeploymentPerLoaderStore   PRIVATE/ISOLATED 的 p2p 部署（按 ldrId 索引）
  ├─ GridDeploymentPerVersionStore  SHARED/CONTINUOUS 的 p2p 部署（按 userVersion 索引）
  └─ GridDeploymentCommunication（TOPIC_CLASSLOAD） + GridDeploymentClassLoader ← p2p 网络层
─────────────────────────────────────────────
旁路：GridMarshallerMappingProcessor（marshaller 的 typeId↔className 映射交换，
  走 discovery custom event + TOPIC_MAPPING_MARSH，与 p2p 类加载完全独立）
```

**先决事实 1：p2p 默认关闭**。`configuration/IgniteConfiguration.java` L116 `DFLT_P2P_ENABLED = false`；默认 SPI 是 `LocalDeploymentSpi`（`spi/deployment/local/LocalDeploymentSpi.java` L65–71，同时标注 `@IgnoreIfPeerClassLoadingDisabled` 与 `@Deprecated`，注释指向 IEP-144 IgniteClassPath 替代方案）。p2p 关闭时若 SPI 带 `@IgnoreIfPeerClassLoadingDisabled`（`spi/deployment/IgnoreIfPeerClassLoadingDisabled.java` L36），manager 构造函数会创建一个**永不废弃的单例 `LocalDeployment`**（`managers/deployment/GridDeploymentManager.java` L74–91，内部类 L636–693：`undeployed()` 恒 false、`acquire()` 恒 true），三 store 仍在但不会被远端请求触达。

**先决事实 2：`GridDeployment` 既是句柄也是状态机**。每个部署的活性由一个 `AtomicStampedReference<Boolean>`（undeployed 标志 + 使用计数，`managers/deployment/GridDeployment.java` L86）表达，task/job 侧通过 `acquire/release` 与 store 侧的 `undeploy` 竞争，见 §1.1。

---

## 1. GridDeploymentManager 状态机

### 1.1 `GridDeployment`：发现→使用→废弃的原子状态

- **字段**：时间戳（L61）、`depMode`、`clsLdr`、`clsLdrId`（`IgniteUuid`）、`userVer`、`loc` 标志（L63–79）。
- **acquire**（L271–286）：CAS 把计数 +1；若已 undeployed 且计数为 0（obsolete）则返回 false——这是消费方（task/job processor）判断"部署还活着"的唯一入口。
- **release**（L291–304）：计数 -1。`GridJobProcessor.release(GridDeployment)`（`processors/job/GridJobProcessor.java` L562–566）与 `GridTaskProcessor.release`（`processors/task/GridTaskProcessor.java` L876–880）在 release 后检查 `obsolete()` 并触发 `ctx.resource().onUndeployed(dep)` 资源注入清理。
- **undeploy**（L232–246）：CAS 置 undeployed=true，但不等待计数归零；`obsolete()`（L311–317）= undeployed && 计数==0，即"废弃且无人在用，可整体清理"。
- **pendingUndeploy**（L81–82、L253–263）：已被**调度**为废弃（等待 networkTimeout 宽限），store 侧跳过对它的复用判断。
- **sequenceNumber** = `clsLdrId.localId()`（L172–174）：同一节点多次 redeploy 时 `IgniteUuid` 全局 id 相同、localId 递增，构成版本序。
- **participants**（L322–327）：委托给 `clsLdr`（若它是 `GridDeploymentClassLoader`）——SHARED 模式下参与者表放在 classloader 里（§2.1）。

### 1.2 manager 装配与三个 store

`start()`（`GridDeploymentManager.java` L94–115）：注册 `GridProtocolHandler`（`gg://` URL 协议入口，见 §3.6）→ `startSpi()` → 创建 `GridDeploymentCommunication` → `startStores()`（L586–594）依次实例化 `GridDeploymentLocalStore` / `GridDeploymentPerLoaderStore` / `GridDeploymentPerVersionStore`。`enabled()`（L161–163）= SPI 非 no-op 且无 locDep 特例。

### 1.3 发现与创建：三条入口

**① 本地类（task 侧）**：`GridTaskProcessor` 启动 task 时 `ctx.deploy().getDeployment(taskName)`（L561）或 `ctx.deploy().deploy(taskCls, ldr)`（L590/L630）。`deploy(cls, clsLdr)`（`GridDeploymentManager.java` L251–313）：lambda 折算到外围类（L257–269）；若 `clsLdr` 已是 `GridDeploymentClassLoader`（嵌套执行），按 **locStore(按 meta) → ldrStore(按 ldrId) → verStore(按 ldrId)** 顺序查现成部署（L290–297），并校验 ISOLATED/PRIVATE 类不得部署进 SHARED/CONTINUOUS 节点（L275–280）；否则走 `locStore.explicitDeploy`（L312）。

**② 本地按名（`getLocalDeployment`，L386–403）**：构造 `GridDeploymentMetadata{alias, className, senderNodeId=本机, record=true}` 交给 `locStore.getDeployment(meta)`（`managers/deployment/GridDeploymentLocalStore.java` L155–239），其内部分四步：
1. 按 alias 查 `cache: ConcurrentMap<String, Deque<GridDeployment>>`（L64、L250–274，按 classLoaderId/clsLdr 匹配）；
2. miss 时问 SPI：`spi.findResource(alias)`（L174）——`UriDeploymentSpi` 在这里被触达（§3.4）；
3. SPI 也没有则**自动部署**：取当前线程 contextClassLoader（排除 `GridDeploymentClassLoader`，L200–205），`U.forName` 加载后 `spi.register(ldr, cls)` 反向注册（L207–221）；
4. `deploy(...)`（L284–366）创建 `GridDeployment`：`ldrId = IgniteUuid.fromUuid(localNodeId)`（L331）、`userVer = userVersion(ldr)`（L333）、`loc=true`（L335），同一 classloader 的后续类并入同一 deployment（L300–316），并按 alias 与 className 双键缓存（L321–353）。

**③ 远端 job 请求（p2p 触发点）**：`GridJobProcessor.processJobExecuteRequest`（L1191）按 `GridJobExecuteRequest` 携带的部署元数据（`internal/GridJobExecuteRequest.java`：`taskClassName` L301、`userVersion` L329、`classLoaderId` L478、`deploymentMode` L492、`loaderParticipants` L522；wire 形态即 `managers/deployment/GridDeploymentInfoBean.java` L37–60）调用 `getLocalDeployment` 或 **`getGlobalDeployment`**（`GridDeploymentManager.java` L416–544）。拿到 `dep` 后 `dep.acquire()`（`GridJobProcessor.java` L1256），再用部署的 classloader 完成请求体反序列化：`req.finishUnmarshal(marsh, U.resolveClassLoader(dep.classLoader(), ctx.config()))`（L1263–1264；`resolveClassLoader` 见 `internal/util/IgniteUtils.java` L1763–1779）。job 执行线程全程用 `U.wrapThreadLoader(dep.classLoader(), ...)` 包裹用户代码（`processors/job/GridJobWorker.java` L598、L762）——这是 deployment 与序列化/classloader 的核心接缝。

### 1.4 p2p 模式 vs shared 模式：`getGlobalDeployment` 的分叉

`isPerVersionMode`（L560–562）= SHARED ∨ CONTINUOUS：

**SHARED/CONTINUOUS（verStore）**，L452–518：
1. 先查 `verStore.searchDeploymentCache(meta)`（`managers/deployment/GridDeploymentPerVersionStore.java` L282–297：按 **userVersion** 取列表，找 `hasParticipant(senderNodeId, classLoaderId)` 的那个）；
2. **本地复用判定**：类若在本节点 classpath/SPI 中存在且模式、userVer 一致，直接复用本地部署、绝不 p2p 拉取（L485–515；模式不一致 L492–500、版本不一致 L502–508 都放弃）；`peerClassLoadingLocalClassPathExclude` 在此否决复用（L458–477，前缀匹配、尾部 `*` 剥离）——语义是"这些包**必须** p2p 加载，不许用本地副本"；
3. 否则 `verStore.getDeployment(meta)`（L300–629）：先用**临时 classloader** 探测远端是否真有该类（`checkLoadRemoteClass`，L314 与 L685–789：`getResourceAsStreamEx` 一次，结果按 ldrId 缓存进 `rsrcCache`，死 loader 用 `deadClsLdrs` 有界集合短路 L701–721）；SHARED 下同一 sender 的旧部署能加载该类则直接挂参与者复用（L334–349）；否则在 userVersion 桶里创建新 `SharedDeployment`（`createNewDeployment` L1019–1105：**本地新 ldrId**（L1024，强调"这个 id 属于本节点的共享部署，不是 sender 的 loader id"）、`GridDeploymentClassLoader` 带 participants 构造（L1063–1077）、按 userVersion 入桶 L1094）；CONTINUOUS 下先收集 `depsToCheck` 候选、在**锁外**做网络验证后二次检查/重试（L411–439、L478–613）。
4. **节点离开**（L107–167）：从 `SharedDeployment` 移除参与者（`removeParticipant` L1160–1177，同时把死掉的各节点 ldrId 记入 `deadClsLdrs` 并清 `rsrcCache`）；SHARED 部署在**最后一个参与者离开**时 undeploy（L130–148），CONTINUOUS 部署**保留**（L149–152）。
5. **hot redeploy/版本变更**：`checkRedeploy`（L892–972）对"无参与者 + userVersion 变化"的 CONTINUOUS 部署调度 networkTimeout 后的延迟废弃。

**PRIVATE/ISOLATED（ldrStore）**，L520–544 → `GridDeploymentPerLoaderStore.getDeployment(meta)`（`managers/deployment/GridDeploymentPerLoaderStore.java` L199–333）：
- cache 就是 `Map<IgniteUuid, IsolatedDeployment>`（L59），按请求的 `classLoaderId` 精确命中（每个 sender 的每次 redeploy 一个独立 loader）；
- sender 已离线则拒绝（L212–217）；
- 未命中时做**序号仲裁**：同 sender 旧部署序号更小且含同类 → 调度旧部署废弃（L230–247）；序号更大（过期请求）→ 按 networkTimeout 宽限部署再立刻调度废弃，超宽限直接忽略（L249–275）；
- 创建 `GridDeploymentClassLoader`（parent = 上下文 loader 或 meta.parentLoader，L286–308），包成 `IsolatedDeployment` 入 cache（L310–313），随后**同步加载请求的类**完成 p2p 预热（L321–330）；
- 节点离开/重连清理：disco listener（L84–117）+ `onKernalStart`（L147–172）；`scheduleUndeploy` 用 `ctx.timeout()` 定时器（L352–378）。

**版本化的根**：`userVersion` = `GridKernalContextImpl.userVersion(ldr)`（`internal/GridKernalContextImpl.java` L921–922）→ spring 组件解析 classloader 资源 `META-INF/ignite.xml` 中 bean `userVersion`（`modules/spring/src/main/java/org/apache/ignite/internal/util/spring/IgniteSpringHelperImpl.java` L311–347），缺省 `"0"`（`IgniteUtils.java` L315）。

### 1.5 废弃周期（显式 + 隐式）

- **显式 undeploy**：`undeployTask(taskName, locUndeploy, rmtNodes)`（`GridDeploymentManager.java` L210–225）本地 `locStore.explicitUndeploy` → `spi.unregister(rsrcName)`（`GridDeploymentLocalStore.java` L409–415），远端经 `comm.sendUndeployRequest` 复用 p2p 通道（§2.4）；接收端 `undeployTask(nodeId, taskName)`（L231–243）对三个 store 全量 `explicitUndeploy`。
- **清理顺序**（`GridDeploymentLocalStore.undeploy(ldr)` L545–589 与 `SharedDeployment.recordUndeployed` L1292–1341）：`ctx.resource().onUndeployed`（资源注入）→ `ctx.cache().onUndeployed`（cache 侧类缓存，仅 SHARED L1324）→ `ctx.marshaller().onUndeploy(ldr)`（清 marshaller 类缓存）→ JDK 序列化 Caches → `GridAnnotationsCache/GridClassLoaderCache`（最后，L575–584、L1318–1339）。SPI 侧回调链：`DeploymentListener.onUnregistered`（`GridDeploymentLocalStore` L599–607 注册）。

---

## 2. peer class loading 管道

### 2.1 缺类触发：`GridDeploymentClassLoader`

`managers/deployment/GridDeploymentClassLoader.java`（L63，`extends ClassLoader implements GridDeploymentInfo`，participants 表内建 L94–116：`nodeList` + `nodeLdrMap`）。关键行为：

- `loadClass(name)`（L451–482）：`org.apache.ignite.compute.ComputeJob` 永不走 p2p（L460，避免转型问题）；命中 `isLocallyExcluded`（`peerClassLoadingLocalClassPathExclude` 前缀匹配，L435–448）才 `p2pLoadClass`（自解析，L503–516），否则标准 parent 委派（L467）。
- `findClass(name)`（L519–568）：先查 local deployment（为 URI 部署兜底，L522–534）→ `U.classNameToResourceName` → **`sendClassRequest`**（L536–538）→ `defineClass` + 可选 `byteMap` 缓存 + `definePackage`（L540–560；byteMap 仅 CONTINUOUS 开启，见 `GridDeploymentPerVersionStore.java` L1046）。
- `sendClassRequest`（L594–718）：`missedRsrcs`（`GridBoundedLinkedHashSet`，大小 = `peerClassLoadingMissedResourcesCacheSize`，默认 100）直接快速失败（L604–608）；按 `nodeList` 顺序逐节点请求（跳过本机 L618–621、跳过离线 L627–632）；失败记入 missed 并 break（L649–654，"共享资源所有节点都该有"）；全部失败抛 `P2PClassNotFoundException`（L707–717，类在 `P2PClassLoadingIssues.java` 中配合 `NoClassDefFoundError` 转译）。
- 资源（非 class）走 `getResourceAsStreamEx`（L731–763）与 `sendResourceRequest`（L771–866），perVersion store 的探测复用它。

### 2.2 请求-响应协议（TOPIC_CLASSLOAD）

客户端 `GridDeploymentCommunication.sendResourceRequest`（`managers/deployment/GridDeploymentCommunication.java` L384–471）：
1. **防环**：`activeReqNodeIds` ThreadLocal 记录"我正在替谁加载"，目标在链上则抛异常（L390–396；服务端处理请求时把 `req.nodeIds` + 请求方塞入该 ThreadLocal，L143–160）；
2. 构造 `GridDeploymentRequest(resTopic, ldrId, rsrcName, isUndeploy=false)`（L398–400；字段全表 `managers/deployment/GridDeploymentRequest.java` L36–56：`resTopic/resTopicBytes/rsrcName/ldrId/isUndeploy/nodeIds`），请求节点链 `req.nodeIds(nodeIds)` 随行（L403）；
3. responseTopic 是 `TOPIC_CLASSLOAD.topic(随机uuid)`（L398），marshal 成 bytes 附带（L422–423）；经 `ctx.io().sendToGridTopic(..., TOPIC_CLASSLOAD, req, GridIoPolicy.P2P_POOL)` 发送（L425，独立 p2p 线程池，默认 2 线程，`IgniteConfiguration.java` L194）；
4. `qryMux.wait(threshold)` 阻塞至响应/超时（`threshold` = networkTimeout，L430–460）；目标节点离开由 discovery listener 注入伪失败响应（L409、L481–519）。

服务端 `processDeploymentRequest`（L125–166）分派 undeploy / 资源请求；`processResourceRequest`（L185–295）：
- `ctx.deploy().getDeployment(req.classLoaderId())`（L203；即 §1.2 的 locStore→ldrStore→verStore 三连查）；
- loader 非 p2p loader 时先 `Class.forName` 并拒绝 `@IgniteNotPeerDeployable`（L211–242，还处理 Java 21 `$$Lambda/` 类名，L298–306）；
- `ldr.getResourceAsStream(req.resourceName())` 读字节进 `GridByteArrayList`（L244–269）；失败仅 DEBUG（框架探测式请求很常见，L246–257）；
- 回 `GridDeploymentResponse{success, errMsg, byteSrc}`（`GridDeploymentResponse.java` L28–39），`sendToCustomTopic(..., P2P_POOL)`（L294、L323–349）。

**undeploy 复用同一主题**：`sendUndeployRequest`（L357–369）发 `GridDeploymentRequest(isUndeploy=true)`；接收端 `processUndeployRequest`（L172–177）→ `ctx.deploy().undeployTask(nodeId, resourceName)`。

### 2.3 marshaller 映射交换协议（独立旁路，服务 binary typeId↔className）

这条协议**不传输字节码**，只同步"typeId → 类名"映射，供 binary marshaller 把紧凑 typeId 还原成类名：

- **id 规则**：`typeId = clsName.hashCode()`；系统类型在启动时从 `META-INF/classnames.properties` 预加载（`internal/MarshallerContextImpl.java` L119–155，`internal/marshaller/MarshallerUtils.java` L48–51）。
- **JOIN 时交换什么**：`GridMarshallerMappingProcessor.collectJoiningNodeData/collectGridNodeData`（`internal/processors/marshaller/GridMarshallerMappingProcessor.java` L328–336，`MARSHALLER_PROC` ordinal L374–376）把 `MarshallerContextImpl.getCachedMappings()`（L192–212，`List<Map<Integer, MappedName>>` 按 platformId 分桶）塞进 discovery 数据；对端 `onJoiningNodeDataReceived/onGridDataReceived`（L339–350）→ `onMappingDataReceived`（`MarshallerContextImpl.java` L218–225，逐条入缓存 + 落盘）。
- **新映射提议**：序列化侧 `BinaryContext.registerUserClassName`（`modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryContext.java` L1209–1222）→ `MarshallerContextImpl.registerClassName`（L303–350）→ `MarshallerMappingTransport.proposeMapping`（`internal/processors/marshaller/MarshallerMappingTransport.java` L99–131）→ **discovery custom event `MappingProposedMessage`**（L127–128）；各节点 `MappingProposedListener`（Processor L237–274）做重复/冲突检测（`onMappingProposed`，`MarshallerContextImpl.java` L406–412），协调者随后广播 **`MappingAcceptedMessage`**，`MappingAcceptedListener`（L298–325）→ `onMappingAccepted`（`MarshallerContextImpl.java` L417–429）入缓存 + 异步落盘。
- **client 缺映射**：client 节点 discovery 事件异步到达，`getClassName` 本地缓存与磁盘（`MarshallerMappingFileStore`，文件名 `{typeId}.classname{platformId}`，`internal/MarshallerMappingFileStore.java` L46–49）都没有时（`MarshallerContextImpl.java` L459–498）→ `transport.requestMapping`（Transport L137–160）经 **`TOPIC_MAPPING_MARSH`** 发 `MissingMappingRequestMessage`；server 端 `MissingMappingRequestListener`（Processor L160–197）`resolveMissedMapping` 后回 `MissingMappingResponseMessage`，client 端 listener（L202–232）完成 future。listener 的注册按 client/server 分支在 `start()` L121–124。
- **反查入口**（unmarshal 时缺类名的场景）：`BinaryContext.descriptorForTypeId` 调 `marshCtx.getClassName(JAVA_ID, typeId)` / `getClass(typeId, ldr)`（`BinaryContext.java` L731–741；`MarshallerContextImpl.getClass` L432–439 用给定 loader `U.forName`——**类加载本身仍由 deployment/classloader 体系负责**，两条子系统在此拼合）。

### 2.4 配置项作用点小结

| 配置 | 默认 | 作用点 |
|---|---|---|
| `peerClassLoadingEnabled` | false（`IgniteConfiguration.java` L116） | manager 构造 locDep 特例（L74–91）；`getGlobalDeployment` 是否走 p2p store（L444–448） |
| `peerClassLoadingLocalClassPathExclude` | null（getter L1454） | ① shared 本地复用否决（`GridDeploymentManager.java` L458–477）；② classloader 强制 p2p 加载（`GridDeploymentClassLoader.java` L435–448 + L461） |
| `peerClassLoadingMissedResourcesCacheSize` | 100（L149） | `missedRsrcs` 有界集合（classloader L206–207）与 perVersion `rsrcCache`（`GridDeploymentPerVersionStore.java` L101、L761–776） |
| `networkTimeout` | — | p2p 请求 threshold（`GridDeploymentPerLoaderStore.java` L303；comm L432）；hot-redeploy 废弃宽限（同文件 L246、`GridDeploymentPerVersionStore.java` L898） |

---

## 3. UriDeploymentSpi 模块内部（modules/urideploy）

### 3.1 结构

主类 `uri/UriDeploymentSpi.java`（1411 行）+ 辅助类：`GridUriDeploymentFileProcessor`（包处理）、`GridUriDeploymentFileProcessorResult`、`GridUriDeploymentUnitDescriptor`（部署单元描述符，`Type.FILE/DIRECTORY`）、`GridUriDiscovery`（无描述符时的全扫描）、`GridUriDeploymentSpringParser/SpringDocument`（`META-INF/ignite.xml` 任务描述符）、`GridUriDeploymentJarVerifier`（签名校验）、`GridUriDeploymentUriParser`（URI 规整/编码，`encodePath` L149–174）、`GridUriDeploymentClassLoader(+Factory)`（children-first `URLClassLoader`）；scanner 家族在 `uri/scanners/`：`UriDeploymentScanner` 接口 + `UriDeploymentScannerManager` + `file/UriDeploymentFileScanner` + `http/UriDeploymentHttpScanner`。

### 3.2 生命周期（spiStart / spiStop）

`spiStart`（`UriDeploymentSpi.java` L542–682）：
1. `initializeUriList` / 缺省 `addDefaultUri`（L548–551）；`initializeTemporaryDirectoryPath`（L553，下载暂存目录，缺省 `ignite/deployment/tmp`，节点子目录）；
2. 文件名过滤器只认 `.gar`/`.jar`（L557–565）；
3. 注册 scanner 回调（L569–630）：`onNewOrUpdatedFile` → `GridUriDeploymentFileProcessor.processFile` → `newUnitReceived`（L589–596）；`onDeletedFiles` → `processDeletedFiles`（L603–619）；`onFirstScanFinished` 计数（L622–629，`findResource` 首次调用要等全部 scanner 完成首扫，L716–728）；
4. 默认 scanners = `UriDeploymentFileScanner` + `UriDeploymentHttpScanner`（L633–638），自定义走 `setScanners`（L498–503）；
5. **每个 URI 一个 `UriDeploymentScannerManager`**：`acceptsURI` 匹配的 scanner + 扫描周期（URI userinfo 里的 `freq=` 参数优先，L640–669、`getFrequencyFromUri` L694–709；否则 scanner 默认值）。manager 自身是名为 `grid-uri-scanner` 的 `IgniteSpiThread`：循环 `scanner.scan(ctx)` + `Thread.sleep(freq)` + 首扫回调（`uri/scanners/UriDeploymentScannerManager.java` L107–147）。

`spiStop`（L506–539）：cancel/join 全部 scanner 线程 → 释放所有 unit classloader（`onUnitReleased`）→ 删除暂存目录 → 注销 MBean。

### 3.3 部署单元处理（FileProcessor）

`GridUriDeploymentFileProcessor.processFile`（`uri/GridUriDeploymentFileProcessor.java` L72–138）：
1. **完整性**：非目录先 `GridUriDeploymentJarVerifier.verify`（签名校验，L76、L448–457）；
2. 压缩包解压到暂存目录 `dirzip_<name>`（L83–95）；
3. 有 `META-INF/ignite.xml`（L101–108）→ `GridUriDeploymentSpringParser.parseTasksDocument` 解析 Spring bean 列表，`processWithDescriptorFile`（L335–372）用 `GridUriDeploymentClassLoaderFactory.create` 建 loader 后逐个 `doc.getTasks(clsLdr)`；
4. 无描述符 → **全 classpath 扫描**：`GridUriDeploymentDiscovery.getClasses(clsLdr, file)`（L384–416），`isAllowedTaskClass` 过滤（public、非抽象/接口、静态内部类，L429–437）；
5. 计算 md5（目录递归累加 digest，L134–135、L148–216）供跨 unit 去重。

`GridUriDeploymentClassLoader`（`uri/GridUriDeploymentClassLoader.java` L34–103）：**children-first**——`loadClass` 先 `findLoadedClass` 再 `findClass`（即本 unit 的 URL），失败才 `super.loadClass`（L46–75）；`loadClassIsolated` 完全不查 parent（L84–103），SPI 的 `findResource` 用它防止任务类被 parent 抢先解析（§3.4）。清理时 `cleanupUnit` 关闭 loader 并删暂存目录（`FileProcessor` L298–323）。

### 3.4 与 GridDeploymentManager 的注册接缝

- **unit 登记**：`newUnitReceived`（`UriDeploymentSpi.java` L1124–1215）：md5 相同的 unit 直接跳过（L1140）；同 URI 换文件 → 移除旧 unit（L1152–1167）；按时间戳插入 `unitLoaders`（最新优先），`checkUnitCollision` 处理任务名/类名冲突（L1183–1190、L1219–1271）。文件删除 → `processDeletedFiles`（L1301–1326）。unit 释放统一走 `onUnitReleased`（L1328–1339）：`cleanupUnit` + `lsnr.onUnregistered(clsLdr)` → manager 侧 `GridDeploymentLocalStore.LocalDeploymentListener.onUnregistered` → `undeploy(ldr)`（`GridDeploymentLocalStore.java` L599–607 → L545–589），完成 §1.5 的全链清理。
- **SPI 四方法**：`findResource(rsrcName)`（L712–775）：等首扫完成 → 按 `unitLoaders` 时间戳倒序尝试 alias/类名（L735–745，URI loader 用 `loadClassIsolated`）→ 返回 `DeploymentResourceAdapter(alias, cls, ldr)`；`register(ldr, rsrc)`（L778–818）+ `addResources`（L855–917，任务名别名冲突检测）；`unregister(rsrcName)`（L821–840）→ `onUnitReleased`。
- manager 侧入口即 §1.3-②：`GridDeploymentLocalStore.getDeployment(meta)` 的 `spi.findResource(alias)`（L174）与 `explicitDeploy` 的 `spi.register`（L376）。**URI 部署的类进入 deployment 体系的路径就是"SPI 按名供货 → LocalStore 包成 `GridDeployment(loc=true)`"**，跨节点分发仍靠 p2p（`GridDeploymentClassLoader.findClass` 的 local-deployment 兜底 L522–534 即为 URI loader 留的路径）。

### 3.5 scanner 家族与单测

- `file/UriDeploymentFileScanner.java`（L44–78）：`file://`，默认 5s 扫描（`DFLT_SCAN_FREQ=5000`）；`http/UriDeploymentHttpScanner.java`（L69–147）：`http(s)://`，默认 300s；用 `lastModified` + `If-Modified-Since` 增量拉取（L292–327），TLS 可配。**2.18 的 scanner 目录只有 file/http**——文档提到的历史 maven resolver 已不存在于默认 scanners（L633–638 仅注册两个），自定义协议经 `UriDeploymentScanner` 接口接入。
- **单测**（核心行为采样）：`modules/urideploy/src/test/.../uri/` 13 个测试类——`GridUriDeploymentFileProcessorSelfTest`（L40–65：合法/损坏/空/错误引用 XML 四 case）、`GridUriDeploymentSimpleSelfTest`、`GridUriDeploymentMd5CheckSelfTest`（去重）、`GridUriDeploymentClassLoaderSelfTest`/`ClassloaderRegisterSelfTest`/`ClassLoaderMultiThreadedSelfTest`（children-first 与并发）、`GridUriDeploymentMultiScannersSelfTest`/`ErrorThrottlingTest`（多 URI 多 scanner）、`scanners/file/GridFileDeploymentSelfTest` 与 `GridFileDeploymentUndeploySelfTest`（部署/删除联动）、`scanners/http/GridHttpDeploymentSelfTest`；外部任务 fixture 在 `modules/extdata/uri`。core 侧 p2p 行为测试见 §5 清单（37 个文件的 `modules/core/src/test/.../p2p/`，含 4 种 DeploymentMode 的 `GridMultinodeRedeploy*` 系列、`GridP2PNodeLeftSelfTest`、`GridP2PTimeoutSelfTest`、`GridP2PMissedResourceCacheSizeSelfTest` 等；另有 `managers/deployment/` 下 6 个，如 `DeploymentRequestOfUnknownClassProcessingTest`、`GridDeploymentMessageCountSelfTest`）。

---

## 4. 分级结论：部署子系统复刻范围（仿 02 §5）

| 分级 | 组件 | 依据 |
|---|---|---|
| **必修（最小闭包，无网络）** | `DeploymentSpi` 接口 4 方法 + `LocalDeploymentSpi`（极简：一个 `ConcurrentLinkedHashMap` 注册表）；`GridDeployment`（usage 状态机：acquire/release/undeploy/obsolete）；`GridDeploymentManager` 骨架 + `GridDeploymentLocalStore`（按 alias 索引 + SPI 供货 + 自动部署）；task/job 侧接缝（`getLocalDeployment`/`deploy` + `acquire/release` + `resolveClassLoader`） | p2p 关闭时的全部生产行为；§1.3-①② 是唯一入口；`GridDeployment` 的 AtomicStampedReference 语义是整个子系统的并发核心 |
| **必修（p2p 课主体）** | `GridDeploymentCommunication`（TOPIC_CLASSLOAD + `GridDeploymentRequest/Response` 3 消息 + undeploy 复用 + 防环 nodeIds 链 + 伪响应）；`GridDeploymentClassLoader`（loadClass/findClass/sendClassRequest/getResourceAsStream + missedRsrcs）；**PerLoaderStore 与 PerVersionStore 二选一**（建议 SHARED 简化版：searchDeploymentCache + createNewDeployment + 节点离开清理，PRIVATE 可留作业） | §2.1–2.2 是自洽的最小消息集；两个 store 共享 classloader/comm，差异只在索引与复用策略（§1.4） |
| **必修但可极简** | marshaller 映射协议：`typeId=hashCode` + JOIN discovery 数据交换 + `MappingProposed/Accepted` 两个 custom event；`MappedName` 三态（无/proposed/accepted）可退化为两态；client `MissingMapping*` 与磁盘持久化（`MarshallerMappingFileStore`）可后置 | 服务的是 binary marshaller 的 typeId 还原（§2.3），不传输字节码；协议面小但可与 p2p 完全解耦开发 |
| **可简化项** | SHARED 完整参与者协议（participants/deadClsLdrs/rsrcCache/depsToCheck 二次检查重试，`GridDeploymentPerVersionStore.java` L300–629）；hot redeploy 序号仲裁与延迟废弃（networkTimeout 定时器）；CONTINUOUS 的 byteMap 与"无参与者保留"语义；`IgniteNotPeerDeployable`/`$$Lambda/` 类名边角；`GridProtocolHandler`（gg://）与 MBean | 全是正确性/性能边角，不影响主链；测试（`GridMultinodeRedeploy*`）可逐个点亮 |
| **外围可选（按需选学）** | `UriDeploymentSpi` 的 file scanner + `FileProcessor`（gar/jar 解压 + ignite.xml 描述符 + children-first loader + md5 去重）；http scanner（增量拉取）；自定义 scanner 插件点 | 单向被 `GridDeploymentLocalStore` 消费（§3.4），接口只有 4 方法；file scanner + processor 是最有教学价值的一块 |
| **不需要复刻** | maven resolver（2.18 已无）；`GridUriDeploymentJarVerifier` 签名校验（可 stub 为 true）；`UriDeploymentSpiMBean`/`LocalDeploymentSpiMBean`；email 类历史 scanner | 无生产代码或纯运维面 |

**部署课弧的自然依赖顺序**：① `DeploymentSpi` + `LocalDeploymentSpi` + manager 骨架/`GridDeployment` 状态机（纯本地，零网络）→ ② p2p 管道（`GridDeploymentClassLoader` + comm 三消息 + PerLoader/PerVersion 之一；前置：GridIoManager 主题发送）→ ③ marshaller 映射协议（前置：discovery custom event 与 JOIN 数据交换；可与 ② 并行，接口在 `MarshallerContext` 收口）→ ④ `UriDeploymentSpi`（file scanner 先行，http 可选；只依赖 ① 的 SPI 接口）。理由：② ③ 都只依赖 ① 的 `GridDeployment`/SPI 抽象而互不依赖（§2.3 与 §2.1 的唯一交点是把类名解析成类加载的 `getClassName(typeId, ldr)`）；④ 是 ① 的纯消费者。

---

## 5. 引用文件清单（全部实际打开验证）

> 行号均相对 `vendors/ignite/` 当前 submodule 内容。

**managers/deployment（core，状态机与 p2p 核心）**
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentManager.java`（类 L52、构造 locDep L74–91、start L94–115、enabled L161–163、deploy L251–313、getDeployment(ldrId) L340–354、getDeployment(rsrcName) L360–380、getLocalDeployment L386–403、getGlobalDeployment L416–544、exclude 判定 L458–477、startStores L586–594、LocalDeployment L636–693）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeployment.java`（usage L86、undeployed L225–227、undeploy L232–246、acquire L271–286、release L291–304、obsolete L311–317、sequenceNumber L172–174、participants L322–327）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentStore.java`（接口全貌 L30–105）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentStoreAdapter.java`（userVersion/isTask/clearSerializationCaches L100–129）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentLocalStore.java`（cache L64、setListener L80、getDeployment(meta) L155–239、deployment(meta) L250–274、deploy L284–366、explicitDeploy L369–406、explicitUndeploy L409–415、undeploy 清理 L545–589、LocalDeploymentListener L599–607）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentPerLoaderStore.java`（disco listener L84–117、getDeployment L199–333、GridDeploymentClassLoader 创建 L293–308、scheduleUndeploy L352–378、IsolatedDeployment L418–435）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentPerVersionStore.java`（cache/deadClsLdrs L68–73、node-left L107–167、searchDeploymentCache L282–297、getDeployment L300–629、addParticipants L632–658、checkLoadRemoteClass L685–789、checkRedeploy L892–972、explicitUndeploy L975–1010、createNewDeployment L1019–1105、SharedDeployment L1115–1347）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentCommunication.java`（start L102–107、processDeploymentRequest L125–166、processResourceRequest L185–295、sendResponse L323–349、sendUndeployRequest L357–369、sendResourceRequest L384–471、disco 伪响应 L481–519）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentRequest.java`（字段 L36–56）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentResponse.java`（字段 L28–39）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentInfoBean.java`（wire 形态 L37–60）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/GridDeploymentClassLoader.java`（字段 L94–131、ctor L159–212/239–284、register L326–350、isLocallyExcluded L435–448、loadClass L451–482、findClass L519–568、sendClassRequest L594–718、getResourceAsStreamEx L731–763、sendResourceRequest L771–866）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/P2PClassLoadingIssues.java`（NoClassDefFoundError 转译 L33–60）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/P2PClassNotFoundException.java`（L25–38）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/deployment/protocol/gg/GridProtocolHandler.java`（registerDeploymentManager L38–53）

**SPI 层（core）**
- `modules/core/src/main/java/org/apache/ignite/spi/deployment/DeploymentSpi.java`（findResource L72、register L92、unregister L102、setListener L110）
- `modules/core/src/main/java/org/apache/ignite/spi/deployment/IgnoreIfPeerClassLoadingDisabled.java`（注解 L36）
- `modules/core/src/main/java/org/apache/ignite/spi/deployment/local/LocalDeploymentSpi.java`（@Deprecated/@IgnoreIfPeerClassLoadingDisabled L65–71、spiStart/Stop L91–110、findResource L118–130、register L189–222）

**消费接缝（core processors）**
- `modules/core/src/main/java/org/apache/ignite/internal/processors/job/GridJobProcessor.java`（processJobExecuteRequest L1191、getGlobalDeployment 调用 L1220–1230、acquire L1256、finishUnmarshal L1263–1264、release L562–566）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/job/GridJobWorker.java`（wrapThreadLoader L598、L762）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/task/GridTaskProcessor.java`（getDeployment/deploy 调用 L561/L590/L630、acquire L711、release L876–880）
- `modules/core/src/main/java/org/apache/ignite/internal/GridJobExecuteRequest.java`（taskClassName L301、userVersion L329、classLoaderId L478、deploymentMode L492、loaderParticipants L522）
- `modules/core/src/main/java/org/apache/ignite/internal/GridKernalContextImpl.java`（userVersion L921–922）
- `modules/core/src/main/java/org/apache/ignite/internal/util/IgniteUtils.java`（DFLT_USER_VERSION L315、resolveClassLoader L1763–1779）

**marshaller 映射协议（core + binary/api + spring）**
- `modules/core/src/main/java/org/apache/ignite/internal/processors/marshaller/GridMarshallerMappingProcessor.java`（流程 javadoc L58–75、start L105–137、MissingMappingRequest/Response Listener L160–232、MappingProposed/Accepted Listener L237–325、JOIN 数据 L328–350、MARSHALLER_PROC L374–376）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/marshaller/MarshallerMappingTransport.java`（proposeMapping L99–131、requestMapping L137–160）
- `modules/core/src/main/java/org/apache/ignite/internal/MarshallerContextImpl.java`（ctor 系统类型 L119–155、getCachedMappings L192–212、onMappingDataReceived L218–225、registerClassName L303–350、registerClassNameLocally L359–371、onMappingProposed/Accepted L406–429、getClassName L459–531、getClass L432–439）
- `modules/core/src/main/java/org/apache/ignite/internal/MarshallerMappingFileStore.java`（文件格式 javadoc L46–49）
- `modules/core/src/main/java/org/apache/ignite/marshaller/MarshallerUtils.java`（CLS_NAMES_FILE 等常量 L48–54）
- `modules/binary/api/src/main/java/org/apache/ignite/marshaller/MarshallerContext.java`（registerClassName 重载 L42–63）
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryContext.java`（descriptorForTypeId 的 getClassName L731–741、registerUserClassName L1209–1222）
- `modules/spring/src/main/java/org/apache/ignite/internal/util/spring/IgniteSpringHelperImpl.java`（userVersion L311–347）

**配置（core）**
- `modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java`（DFLT_P2P_ENABLED L116、DFLT_P2P_MISSED_RESOURCES_CACHE_SIZE L149、DFLT_P2P_THREAD_CNT L194、getPeerClassLoadingLocalClassPathExclude L1454）

**urideploy 模块**
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/UriDeploymentSpi.java`（可部署单元/包结构 javadoc L89–125、协议列表 L190–200、spiStop L506–539、spiStart L542–682、freq 参数 L694–709、findResource L712–775、register L778–818、unregister L821–840、addResources L855–917、newUnitReceived L1124–1215、checkUnitCollision L1219–1271、newUnitReceived(uri,...) L1279–1296、processDeletedFiles L1301–1326、onUnitReleased L1328–1339）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/GridUriDeploymentFileProcessor.java`（processFile L72–138、md5 L148–216、cleanupUnit L298–323、processWithDescriptorFile L335–372、processNoDescriptorFile L384–416、isAllowedTaskClass L429–437、checkIntegrity L448–457）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/GridUriDeploymentClassLoader.java`（children-first L34–75、loadClassIsolated L84–103）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/GridUriDeploymentUriParser.java`（encodePath L149–174）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/GridUriDeploymentDiscovery.java`（getClasses L61）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/UriDeploymentScannerManager.java`（scanner 线程 L107–147）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/file/UriDeploymentFileScanner.java`（L44–78）
- `modules/urideploy/src/main/java/org/apache/ignite/spi/deployment/uri/scanners/http/UriDeploymentHttpScanner.java`（L69–147、增量拉取 L292–327）

**测试（目录级验证）**
- `modules/core/src/test/java/org/apache/ignite/p2p/`（37 个文件：`GridP2PClassLoadingSelfTest`、`GridMultinodeRedeploy{Shared,Isolated,Private,Continuous}ModeSelfTest`、`GridP2PNodeLeftSelfTest`、`GridP2PTimeoutSelfTest`、`GridP2PHotRedeploymentSelfTest`、`GridP2PMissedResourceCacheSizeSelfTest`、`GridP2PUndeploySelfTest` 等）
- `modules/core/src/test/java/org/apache/ignite/internal/managers/deployment/`（6 个：`DeploymentRequestOfUnknownClassProcessingTest`、`GridDeploymentManagerStopSelfTest`、`GridDeploymentMessageCountSelfTest`、`GridDifferentLocalDeploymentSelfTest`、`P2PCacheOperationIntoComputeTest`、`P2PClassLoadingIssuesTest`）
- `modules/urideploy/src/test/java/org/apache/ignite/spi/deployment/uri/`（13 个，含 `GridUriDeploymentFileProcessorSelfTest` L40–65 case 清单、`GridUriDeploymentMd5CheckSelfTest`、`scanners/file/GridFileDeployment{,Undeploy}SelfTest`、`scanners/http/GridHttpDeploymentSelfTest`）
- `modules/extdata/uri/src/main/java/`（p2p/uri 测试 fixture 任务类，目录级验证）

---

## 6. 对复刻课的启示

1. **先教状态机再教网络**。`GridDeployment` 的 `AtomicStampedReference(undeployed, count)` + `acquire/release/obsolete` 三元语义（§1.1）不到 60 行，却决定了 store 与 task/job 消费方的全部并发正确性；把它做成第一课的"领域对象"，后面的 p2p 只是给这个对象增加"远端来源"。
2. **p2p 的本质是一个"按 classloader id 寻址的 resource GET 协议"**。消息集极小（request/response/undeploy 三种，§2.2），复刻时可以先做一个"假远端"（本地回环）驱动 `GridDeploymentClassLoader` 的单测，再接入真实 IO 主题——协议自包含、极易 TDD。
3. **marshaller 映射与类字节码分发是两条独立总线**，2.18 源码里一个走 discovery（MappingProposed/Accepted + JOIN 数据），一个走 communication（TOPIC_CLASSLOAD）。课程务必分开成两讲，否则学员极易把"typeId 拉取"误解成"类加载"。
4. **SHARED 模式的复杂度是可剪枝的**。`GridDeploymentPerVersionStore.getDeployment` 的锁外网络验证 + 二次检查 + 重试循环（L300–629）是生产级并发打磨，复刻课第一遍可退化为"每次新 loader + 节点离开即废弃"，用 `GridMultinodeRedeploy*` 的部分测试驱动第二遍引入参与者复用。
5. **UriDeploymentSpi 是"SPI 插件化"的最佳教具**：4 方法接口 + children-first classloader + 扫描线程 + 与 `GridDeploymentLocalStore` 的回调闭环（§3.4），且完全离线可测（file scanner）。建议把它作为部署课弧的收官项目，而不是 p2p 的前置。
6. **别忘了清理链**。undeploy 的"resource → cache → marshaller → JDK 序列化缓存 → 注解/类缓存"顺序（§1.5）是防 classloader 泄漏的关键，也是最容易在复刻中被省略、然后被 `GridDeploymentManagerStopSelfTest` 之类测试抓出来的部分——把它列为本课弧的验收标准之一。
