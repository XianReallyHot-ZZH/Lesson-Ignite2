# 16 · 客户端形态与收尾弧：binary 完整化、ClientImpl、thin client 协议

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文覆盖章 12 的三块未调研面：**binary marshaller 完整化**（含长期挂着的 metadata 集群交换核实项）、**thick client 的 ClientImpl**（join/重连/断连语义）、**thin client 线协议**（握手、cache op、查询游标、COPY 推包）。路径缩写约定：无前缀 = `modules/core/src/main/java/org/apache/ignite/` 之下；`binary-api/` = `modules/binary/api/src/main/java/org/apache/ignite/`；`binary-impl/` = `modules/binary/impl/src/main/java/org/apache/ignite/`。行号以当前 submodule 内容为准，文末"引用文件清单"给出全部完整路径（均已逐一打开验证）。

---

## 0. 先决事实（影响全篇的三个 2.18 结构性事实）

1. **binary 在 2.18 已从 core 拆成独立双模块**：公开 API 与通用机制在 `modules/binary/api`（`binary-api/binary/BinaryObject.java` 等 24 个公开类 + `internal/binary/` 下的 `BinaryContext`、`GridBinaryMarshaller`、`BinarySchema`、`BinaryMetadata` 等），重量级实现在 `modules/binary/impl`（`BinaryObjectImpl`、`BinaryObjectBuilderImpl`、streams 实现）。core 模块的 `internal/binary/` 目录只剩 `package-info.java` 一个文件（find 验证）。但 **metadata 交换的处理器侧仍在 core**：`internal/processors/cache/binary/`（`CacheObjectBinaryProcessorImpl`、`BinaryMetadataTransport`、5 个 Metadata* 消息类）。
2. **"compact 无 schema"确实存在，但它是 footer 的紧凑形态而非独立格式**：`BinaryConfiguration.compactFooter`（默认 **true**，`configuration/BinaryConfiguration.java` L38）控制对象尾部 footer 是否省略 field id——详见 §1.4/§1.7。
3. **thin client 协议没有 magic bytes**：连接建立后第一条消息以 4 字节长度前缀 + 首字节 `HANDSHAKE=1` 识别（`ClientListenerNioListener.java` L364、`ClientListenerRequest.java` L25），随后一个 byte 区分三类客户端（0=ODBC、1=JDBC、2=THIN，`ClientListenerNioListener.java` L59–69）。

---

## 1. binary 完整化全貌

### 1.1 公开面与 withKeepBinary 全链

- 用户拿到的不可变二进制对象是 `BinaryObject`（接口，binary-api），运行时形态几乎总是 `binary-impl/.../BinaryObjectImpl.java`（on-heap：`byte[] arr + int start`，L325–332 的 `bytes()/start()`；off-heap 变体 `BinaryObjectOffheapImpl` 同包）。
- 关键读方法：`typeId()`（L352–370）从头偏移 4 处读 int；**若 typeId==0（`UNREGISTERED_TYPE_ID`，`GridBinaryMarshaller.java` L204），则紧跟 24 字节头之后是一个 STRING 形态的完整类名**，读它并经 `ctx.typeId(clsName)` 解析（L357–366）——这是"首次出现的类型自带类名"机制。`type()` 返回 `BinaryType`（L373–375）；`field(String/int fieldId)` 走 reader（L383–390）。
- 构建器：`BinaryObjectBuilder`（binary-api 接口）→ `binary-impl/.../builder/BinaryObjectBuilderImpl.java`：从类型名新建（L106–108）或从既有对象出发（L123–131，经 `BinaryBuilderReader` 惰性读旧值）；`build()`（L176）在写完字段后 `ctx.updateMetaIfNeeded(...)`（L340）触发元数据登记；`getField/setField` 族在 L509–565。惰性中间结构（`BinaryLazyMap`、`BinaryLazyArrayList` 等同包类）保证"改一个字段不必反序列化整个对象"。
- **withKeepBinary 全链**：用户 API `IgniteCache.withKeepBinary()` 的真身在 `internal/processors/cache/GatewayProtectedCacheProxy.java` L255–269——它就是 `keepBinary()`：以 `opCtx.keepBinary()` 复制出一个新 proxy（`CacheOperationContext` 携带布尔标志）；后续所有 cache 操作在 `GridCacheContext` 侧据此决定 key/value 是否强制走 `toCacheObject` 的 binary 路径、结果是否保持 `BinaryObject` 不反序列化。注意 `IgniteCacheProxyImpl.java` L334–336 的 `withKeepBinary()` 是 `throw new UnsupportedOperationException()`——它是内部代理层，不是用户入口。

### 1.2 二进制格式：24 字节头、type 代码、枚举/集合/Map/嵌套

`binary-api/.../GridBinaryMarshaller.java` 定义全部 wire 格式：

- **对象头（L206–231）**：`[1]type=OBJ(103)｜[1]proto_ver(1)｜[2]flags｜[4]typeId｜[4]hashCode｜[4]totalLen｜[4]schemaId｜[4]schemaOrRawOffset`，共 24 字节（`DFLT_HDR_LEN`，L231）。flags 里含 `BinaryUtils` L147 的 "compact footer, no field IDs" 位。
- **type 代码（L45–198 摘录）**：原语 BYTE=1..BOOLEAN=8、STRING=9、UUID=10、DATE=11、数组 BYTE_ARR=12..BOOLEAN_ARR=19、OBJ_ARR=23、COL=24、MAP=25、BINARY_OBJ=27、ENUM=28、ENUM_ARR=29、DECIMAL=30/31、CLASS=32、TIMESTAMP=33/34、PROXY=35、TIME=36/37、**BINARY_ENUM=38**；特殊值 NULL=101、HANDLE=102（循环引用句柄）、OBJ=103。速查表：

| code | 类型 | code | 类型 | code | 类型 |
|---|---|---|---|---|---|
| 1–8 | byte/short/int/long/float/double/char/boolean | 12–19 | 对应原语一维数组 | 101 | NULL |
| 9 | String | 20–22 | String[]/UUID[]/Date[] | 102 | HANDLE（循环引用） |
| 10 | UUID | 23 | Object[]（元素各自带 type 头） | 103 | OBJ（完整二进制对象） |
| 11 | Date | 24 | Collection（后跟 subtype byte） | -2/-3 | OPTM_MARSH/TRANSFORMED（遗留） |
| 30/31 | BigDecimal/BigDecimal[] | 25 | Map（后跟 subtype byte） | 99 | 平台工厂代理 |
| 33/34 | Timestamp/Timestamp[] | 27 | BINARY_OBJ（嵌套二进制对象） | 28/29 | ENUM/ENUM[]（class+ordinal） |
| 36/37 | Time/Time[] | 38 | BINARY_ENUM（typeId+ordinal） | 32 | CLASS |

- **集合/Map 内嵌 subtype**（同文件 L171–195）：COL 后跟一个 byte——USER_COL=0/ARR_LIST=1/LINKED_LIST=2/HASH_SET=3/LINKED_HASH_SET=4/SINGLETON_LIST=5/USER_SET=-1；MAP 后跟 HASH_MAP=1/LINKED_HASH_MAP=2。读取端按该 byte 重建具体集合类型（`BinaryUtils.knownCollection/knownMap`）。
- **枚举**：普通 ENUM=28 写 class 名 + ordinal；**binary enum**（`BINARY_ENUM=38`）只写 typeId + ordinal，枚举值表存进 metadata 的 `nameToOrdinal/ordinalToName`（见 §1.5），实现类 `binary-impl/.../BinaryEnumObjectImpl.java`（L47，实现 `BinaryObjectEx`）。
- **嵌套**：字段值本身就是完整二进制对象（子对象自带 24 字节头），或经 HANDLE=102 回指同一次 marshal 内已写对象（去重循环引用）。

### 1.3 BinaryNameMapper / BinaryIdMapper 与配置默认

- `BinaryBasicNameMapper`（binary-api）：`DFLT_SIMPLE_NAME=false`（L27）——默认**全限定类名**做 typeName；`fieldName` 恒等返回（L77–81）。
- `BinaryBasicIdMapper`：`DFLT_LOWER_CASE=true`（L27）；`typeId(typeName)` = **小写化后的 String.hashCode**（L69–81、L131–145 的 `lowerCaseHashCode`，非 `String.toLowerCase().hashCode()`，是逐字符小写滚 hash）；`fieldId(typeId, fieldName)` 同样小写 hash（L90–102）。id 为 0 会抛异常（防碰撞哨兵）。
- 组装在 `binary-api/.../BinaryContext.java`：默认组合 `DFLT_MAPPER = BinaryBasicNameMapper(false) + BinaryBasicIdMapper(true)`（L88–93）；`resolveMapper`（L422–440）处理用户在 `BinaryConfiguration` 里配 null/部分配置的组合，并识别出"simpleName+lowerCase"特例映射到预解析的 `SIMPLE_NAME_LOWER_CASE_MAPPER`（L92–93，.NET/平台端默认）。`BinaryInternalMapper`（binary-api，L69–90）串联 nameMapper→idMapper 并兜底 0 id。配置默认 null → 走 DFLT_MAPPER（`BinaryConfiguration.java` L41/L44）。

### 1.4 BinarySchema 与 compact footer（字段 id 顺序集的优化）

- **`BinarySchema`**（binary-api，L38–76）= `schemaId + int[] 字段id（按写入顺序）`，外加 id→order 反查结构；≤4 个字段时直接内联在 4 个 int 槽位（L66–76 的 id0–id3），更多才退到开放寻址 map（"低字段数比对可留在 L1 cache line"的注释，L33–36）。每个 typeId 持有一个 `BinarySchemaRegistry`（L32；inline 4 槽 + HashMap 兜底，L73–94），按对象头里的 `schemaId` O(1) 查 schema。
- **footer 写入**（`binary-api/.../BinaryWriterSchemaHolder.java` L83–142）：`compactFooter=true` 时 footer 只写**每个字段的 offset**（无 fieldId）——按最大 offset 选 1/2/4 字节宽（L94–113，返回 `BinaryUtils.OFFSET_1/2/4`，`BinaryUtils.java` L155–161）；`false` 时写 `(fieldId:int, offset)` 对（L114–139）。**compact 模式下 fieldId→offset 映射必须靠 schema（即 metadata）反解**——这正是 metadata 必须全集群一致的硬原因（§1.6）。
- schema 本身持久化在 `BinaryMetadata.schemas` 集合里随元数据传播（§1.5），新 schema 出现时由 `MetadataUpdateProposedMessage` 扩散。

### 1.5 BinaryMetadata / BinaryTypeImpl 结构

- `binary-api/.../BinaryMetadata.java`（L41–78）：`typeId、typeName、Map<String,BinaryFieldMetadata> fields（字段名→字段类型元数据）、affKeyFieldName、Collection<BinarySchema> schemas、Set<Integer> schemaIds、isEnum、Map<String,Integer> nameToOrdinal / Map<Integer,String> ordinalToName`——一个类型的**全部**格式知识都在这一个可 Externalizable 对象里。
- `binary-api/.../BinaryTypeImpl.java`（L31–77）是 `BinaryType` 的包装：`fieldNames()/fieldTypeName()/field(fieldName)/affinityKeyFieldName()` 全部委托给内部 `BinaryMetadata`。affinity 字段名也走 metadata 传播（affinity function 按 `affKeyFieldName` 提取 key 内字段做 hash 时消费它）。

### 1.6 BinaryType metadata 的集群交换机制（长期挂项，已核实）

**结论：三条通道 + 一条兜底，全部在 `internal/processors/cache/binary/`。**

更新协议的完整时序（通道 1）：

```
writer 节点                     coordinator(discovery 首站)              其余节点
   |--MetadataUpdateProposed----------------------------------------->|  (pendingVer==0)
   |                                |--冲突? 标记 REJECTED 原路退回--|
   |                                |--pendingVer+=1, merge, 继续广播-->|
   |   (发起者也在广播路径上更新本地+写盘 prepare, L602–628)              |--merge 入本地缓存 + prepare 写盘
   |<------------------ ring 走完后 discovery 层自动回投 ----------------|
   |            MetadataUpdateAccepted(acceptedVer==pendingVer)         |
   |   (各节点: acceptedVer 对齐, server 异步落盘完成,                 |
   |    唤醒 syncMap 中阻塞线程 + schemaWaitFuts, L664–742)             |
```

1. **增量更新：discovery custom event 双消息协议**。任一节点写新类型 → `BinaryMetadataHandler.addMeta` → `CacheObjectBinaryProcessorImpl.addMeta`（L527–541）→ `BinaryMetadataTransport.requestMetadataUpdate`（`BinaryMetadataTransport.java` L196–268）→ `discoMgr.sendCustomEvent(new MetadataUpdateProposedMessage(mergedMeta, origNodeId))`（L255）。协议在 `MetadataUpdateProposedMessage.java` L38–72 的 javadoc 完整成文：**发起者阻塞等待；coordinator（discovery custom event 环的第一站）校验冲突并给 `pendingVer+1`；消息沿 ring 广播、各节点合并入本地缓存（`MetadataUpdateProposedListener`，transport L532–659，coordinator 盖版本在 L550–596）；消息走完 ring 后 discovery 层回投 `MetadataUpdateAcceptedMessage`（`ackMessage()`，消息类 L125–127），各节点更新 acceptedVer、落盘（`MetadataUpdateAcceptedListener`，transport L664–742，server 侧 `metadataFileStore.writeMetadataAsync` L707）**。消息是 `isMutable()` 的（L132–134），coordinator 原地改消息内容。
2. **join 全量交换：discovery 数据包（DiscoveryDataBag）**。`CacheObjectBinaryProcessorImpl` 实现了 `discoveryDataType()=BINARY_PROC`（L1454–1456）：老节点把整个 `metadataLocCache` 塞进 grid common data（`collectGridNodeData`，L1459–1470）；**新 join 节点在 `onGridDataReceived`（L1538–1548）一次性收下全量快照**（acceptedVer 直接对齐 pendingVer，L1545–1547）；新节点也上报自己的本地元数据（`collectJoiningNodeData` L1473–1480），coordinator 侧 `validateNode/validateBinaryMetadata`（L1387–1399、L1422–1451）做兼容性校验（不兼容直接拒绝 join），各节点 `onJoiningNodeDataReceived`（L1483–1535）合并之。
3. **client 拉取：IO 消息（TOPIC_METADATA_REQ）**。client 节点读元数据发现缺失/过期时，`metadata0`/`metadata(typeId,schemaId)` 调 `transport.requestUpToDateMetadata(typeId)`（L363–376，assert `ctx.clientNode()`）→ `ClientMetadataRequestFuture.requestMetadata()`（`ClientMetadataRequestFuture.java` L44、L90）发 `MetadataRequestMessage`；server 端 `MetadataRequestListener`（transport L822）回 `MetadataResponseMessage`；client 端 `MetadataResponseListener`（L872）接收。
4. **兜底与落盘**：server 缺 schema 且无更新在途时走"last resort waiting"——`awaitSchemaUpdate` + `IGNITE_WAIT_SCHEMA_UPDATE` 超时等待（`CacheObjectBinaryProcessorImpl.java` L812–836）。持久化落盘仅 server 侧、且仅当 `CU.isPersistenceEnabled||isCdcEnabled`（`BinaryMetadataFileStore.enabled`，`BinaryMetadataFileStore.java` L388–390；启动时 `restoreMetadata` 回填，processor L306–307）。
5. **keepBinary 跨节点读字段的消费路径**：`BinaryObjectImpl.field(name)`（L383）→ `BinaryReaderExImpl.unmarshalField(String)`（binary-impl L330）→ `fieldId(name)` 用 **mapper** 算（L1983–1987，不需要 metadata）；但 **compact footer 反解 offset 需要 schema**：`getOrCreateSchema`（L1990–2013）先查 `BinarySchemaRegistry`，未命中则取 `ctx.metadata(typeId, schemaId)`（→ processor L744–777，client 缺失则触发通道 3 拉取），再没有就抛 "Cannot find metadata for object with compact footer"（L2000–2003）。

### 1.7 "compact 无 schema 格式"核实结论

**存在，启用条件是 `BinaryConfiguration.compactFooter`，默认开启**（`BinaryConfiguration.java` L38 `DFLT_COMPACT_FOOTER=true`、L169–180）。格式 = footer 不含 fieldId、只含按最大 offset 定宽（1/2/4 字节）的 offset 数组（§1.4）；代价是读端必须有 schema（metadata）。集群一致性靠 attribute 校验兜底：节点属性 `ATTR_BINARY_CONFIGURATION` 含 compactFooter（processor L272–303，L280 显式列入；`IgniteKernal.java` L1544 启动时写入）。检索范围：`modules/binary/api`、`modules/binary/impl`、`core/internal/processors/cache/binary`、`core/internal/marshaller` 全量 grep "compact"，落点仅上述 footer/mapper 一族，无第二套"无 schema 二进制格式"。

---

## 2. ClientImpl 内部结构（thick client 的 discovery 实现）

`spi/discovery/tcp/ClientImpl.java`（2819 行，class 声明 L146）。与 server 侧 `TcpDiscoveryImpl` 的姊妹实现：**client 不进 ring，只与一个 router server 保持单条 TCP 连接**，所有 discovery 消息经 router 收发。

### 2.1 结构总览：三线程 + 状态机

- `spiStart`（L290–329）起三个线程：`SocketWriter`（L1276，唯一出站队列）、`SocketReader`（L1105，读 router 来的消息投给 msgWorker）、`MessageWorker`（L1710，单线程处理全部 discovery 消息与生命周期事件），外加定时 `MetricsSender`（L1087，按 `metricsUpdateFreq` 上报本 client 指标，L314）。
- 状态机 `State`：`STARTING → CONNECTED ⇄ DISCONNECTED → SEGMENTED/STOPPED`；`getSpiState()` 以 `sockWriter.isOnline()` 二值化（L271–277）。joinLatch/leaveLatch 控制启动与关停（L193/L196）。
- 拓扑容器：`rmtNodes`（L163，UUID→节点）+ `topHist`（L169）+ `delayDiscoData`（L166，join 未完成期间暂存他人 discovery 数据）。

```
                 ┌─────────────── 单条 TCP ───────────────┐
  [SocketWriter] ──出站队列──► router server 节点 ◄──读线程── [SocketReader]
        ▲                                                      │ 投递
        │ sendMessage()（ping/心跳/left/custom event）          ▼
  [MessageWorker] ◄──────────── BlockingDeque ─────────── [SocketClosedMessage/
        │  processDiscoveryMessage:                 Reconnector 产出/JoinTimeout...]
        │  NodeAdded/NodeAddFinished/NodeFailed/CustomEvent/MetricsUpdate/ClientReconnect
        ▼
  spi.lsnr.onDiscovery → GridDiscoveryManager → kernal（EVT_*）
  [MetricsSender 定时] ──TcpDiscoveryMetricsUpdateMessage──► router
```

### 2.2 client join：router 选择与消息往返

1. **router 选择**：`joinTopology`（L561–634）从 IP finder 解析地址列表（L576），重连时把**上一个 router 地址换到队首**（"process failed node last"，L598–603），逐个尝试 `sendJoinRequests`（L637–676）：响应 `RES_OK` 即成功，`RES_CONTINUE_JOIN/RES_WAIT` 则关 socket 等待重试（L657–665）。
2. **单地址往返**（`sendJoinRequest`，L697–883）：`spi.openSocket` → 发 `TcpDiscoveryHandshakeRequest`（`client(true)`，L733–738）；若响应带 `redirectAddresses`（跨 DC）则递归重连正确 DC（L743–754）；成功后**记录 `locNode.clientRouterNodeId(rmtNodeId)`**（L761）——这就是 router 绑定。
3. **join 消息**：首 join 发 `TcpDiscoveryJoinRequestMessage`，携带 `DiscoveryDataPacket`（`spi.collectExchangeData`，L776–784，client 侧的 discovery 数据交换出口）；**重连则发 `TcpDiscoveryClientReconnectMessage`（携带 lastMsgId，L800）**。收到 byte receipt（RES_OK）后，这条 socket 成为 currSock。
4. **拓扑获取**：`processNodeAddedMessage`（L2208–2272）在"自己的 NodeAdded"里一次性拿 **`msg.topology()` 全量拓扑 + topologyHistory** 灌入 rmtNodes/topHist（L2216–2239）；他人 join 期间的 `gridDiscoveryData` 若自己还没 join 完先存 delayDiscoData（L2257–2264）。`processNodeAddFinishedMessage`（L2277–2346）在"自己的 NodeAddFinished"里处理 `clientDiscoData` 回程数据 + 冲放 delayDiscoData（L2294–2304）、设置 order/visible、`state=CONNECTED`（L2325）、发 `EVT_NODE_JOINED`（L2327）、开 joinLatch（L2341）。此后拓扑增量靠 router 转发的 ring 消息（NodeAdded/NodeAddFinished/NodeFailed/CustomEvent），指标靠 `TcpDiscoveryMetricsUpdateMessage`（L2518）。

首 join 的消息往返全景：

```
client                          router server（ring 成员）
  |--- TcpDiscoveryHandshakeRequest(client=true) --------->|
  |<-- TcpDiscoveryHandshakeResponse(redirect?/creatorId)--|  ← 记录 clientRouterNodeId
  |--- TcpDiscoveryJoinRequestMessage(node, DiscoveryDataPacket) -->|
  |<-- byte receipt (RES_OK) ------------------------------|
  |         （消息进 ring：coordinator 校验、广播 NodeAdded(client)）
  |<-- NodeAdded(自己): topology 全量 + topologyHistory ----|
  |<-- 其他节点的 NodeAdded/CustomEvent(暂存 delayDiscoData)|
  |<-- NodeAddFinished(自己): clientDiscoData + attributes + order -->|  → CONNECTED
```

重连场景把第 3 步换成 `TcpDiscoveryClientReconnectMessage(lastMsgId)`，第 5–6 步换成 router 缓存的 `pendingMessages` 回放（§2.4）。

### 2.3 断连检测与 Reconnector 状态机

状态机与断连处置全景：

```
 STARTING ──join 成功──► CONNECTED ──currSock 关闭──► (Reconnector 线程)
    │ │                    │ │                          │成功: 扶正新 socket
    │ │joinTimeout         │ │forceFailMsg              │      +回放 pendingMessages
    │ ▼                    │ ▼                          │失败: SPI_RECONNECT_FAILED
    │ joinError 退出   SPI_RECONNECT/RECONNECT_FAILED   ▼
    │                    │ │(换新 UUID, onDisconnected, throttle)     │reconnectDisabled
    │                    ▼ ▼                                        ▼
    └──────────────► DISCONNECTED ──tryJoin 重来──► CONNECTED   SEGMENTED(终态)
                         │ joinTimeout
                         ▼
                      SEGMENTED
```

- 检测 = **currSock 关闭**：SocketReader 投 `SocketClosedMessage`，MessageWorker 判 `msg.sock == currSock` 后（L1849–1887）：若曾收 `forceFailMsg`（server 判定本 client 已死，L1841–1848）→ `SPI_RECONNECT_FAILED`；否则**起 `Reconnector` 线程**（L1883–1884）。
- `Reconnector`（L1539–1705）循环：`joinTopology(prevAddr)` 重选 router → 在新 socket 上**等 server 回 `TcpDiscoveryClientReconnectMessage`**（L1626–1648），期间把收到的 ensured 消息缓存到 `msgs` 列表（L1650–1655）；success 则把 reconnect 消息 + 缓存消息一并投回 MessageWorker（L1635–1640）；超时/失败 → `SPI_RECONNECT_FAILED`（L1701）。join 阶段断连（`join=true`）失败直接 `joinError`。
- MessageWorker 收到 `SPI_RECONNECT_FAILED`（L1889–1970）：若 `spi.isClientReconnectDisabled()` → `SEGMENTED` + `EVT_NODE_SEGMENTED`（L1899–1917）；否则 **换新 UUID**（`locNode.onClientDisconnected(newId)`，L1966；force-fail 场景先睡 `IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY`，L1932–1948）、`onDisconnected()`（L2066–2085：置 DISCONNECTED、发 `EVT_CLIENT_NODE_DISCONNECTED`、fail 掉所有 ping future）→ `tryJoin()` 全重来。`SPI_RECONNECT`（L1809–1839）是 kernal 主动触发的同类路径，带 `throttleClientReconnect` 指数退避（L2039–2061）。joinTimeout 内 join 不成 → STARTING 报错退出 / DISCONNECTED 转 SEGMENTED（L1759–1781）。

### 2.4 重连后的补齐（衔接 09 号 §1.2 ring 视角）

- SPI 侧：`processClientReconnectMessage`（L2537–2578）把 Reconnector 的新 socket 扶正为 currSock（L2545–2548），**顺序重放 `msg.pendingMessages()`**——router 在 client 失联期间为它缓存的 ring 消息（L2552–2557）。
- Kernal 侧两段回调：`IgniteKernal.onDisconnected`（`internal/IgniteKernal.java` L3057–3121）创建 reconnect future、逆序通知全部 GridComponent 的 `onDisconnected(userFut)`；`onReconnected`（L3127–3187）正序 `comp.onReconnected(clusterRestarted)` 并汇总 `reconnectExchangeFuture()`（L3144），**失败若含 `IgniteNeedReconnectException` 则整轮重试**（L3163–3181），其它错误直接停节点。
- Cache 侧：`GridCacheProcessor.onDisconnected`（L876–910）以 `IgniteClientDisconnectedCheckedException` fail 掉全部 pending 动态 cache future 并锁 gate；`onReconnected`（L931–981）按 `ClusterCachesReconnectResult` 停掉已消失的 cache/group（`stopCacheOnReconnect` L916–928）、对存活 cache 调 `cache.onReconnected()`、必要时重建 SQL schema（L964–979）。09 号报告 §1.2 的 server 侧视角（router 缓存 + ClientReconnectMessage）与本节客户端视角拼成完整闭环。

### 2.5 断连异常的触发面

- **公共 API 抛的是 `IgniteClientDisconnectedException`（携带 reconnectFuture）**：cache 一切操作经 `GridCacheGateway` 入口，断连时 `throw new CacheException(new IgniteClientDisconnectedException(reconnectFut, ...))`（`internal/processors/cache/GridCacheGateway.java` L96）。二进制侧：`metadata(typeId,schemaId)` 在 client 断连且拉不到元数据时同样抛（`CacheObjectBinaryProcessorImpl.java` L765–768）。
- **`IgniteNeedReconnectException` 是内部信号**（`internal/IgniteNeedReconnectException.java` L24–30 "Indicates that node should try reconnect"）：由 `GridDhtAssignmentFetchFuture` L225（拉 affinity 失败）、`GridCachePartitionExchangeManager` L804/L3226（exchange 中）抛出，被 `IgniteKernal.onReconnected` L3163 捕获后驱动重连重试——不直达用户。
- discovery 公共操作：`pingNode` 断连态抛 `IgniteClientDisconnectedCheckedException`（ClientImpl L418–422）；`sendCustomEvent` 断连态抛 `IgniteClientDisconnectedException`（L495–499）。

### 2.6 client 端的 exchange 参与视角（补 10 号 §4.3 的 client 半边）

- exchange future 的 client 分叉：`ExchangeLocalState.CLIENT`（`dht/preloader/GridDhtPartitionsExchangeFuture.java` L923）；本机 client 事件（join）与 server 事件均产 `ExchangeType.CLIENT`（L1009–1024、L1540），**其他 client 的 join/left 产 `ExchangeType.NONE`**（`onClientNodeEvent`，L1499–1520——client 对 client 不做 exchange）。
- `clientOnlyExchange`（L1567–1574）：向 coordinator `sendLocalPartitions`（一条 `GridDhtPartitionsSingleMessage`），然后等 server 侧回带 `clientTops` 的 single message（`processSingleMessage`，L3047）完成——即 10 号已证的 server 侧 clientTops 协议的客户端对端。crd 为 null（**所有 server 都没了**）时用 idealAssignment 兜底或 `onAllServersLeft`（L1580–1594）。
- 分区状态容器是 `dht/topology/GridClientPartitionTopology.java`（L76，implements `GridDhtPartitionTopology`；2.18 位于 topology/ 而非 preloader/）：`beforeExchange`（L307）与两版 `update(...)`（L746/L923，带 stale 判定）维护 full map，供 near-cache/affinity-aware 路由使用。join 时若需 affinity，client 用 `GridDhtAssignmentFetchFuture` 向 server 拉分配（其失败是 `IgniteNeedReconnectException` 的来源之一）。

### 2.7 client 的 cache 行为

- **near cache 非默认**：thick client 建普通 client cache；要 near 必须显式给 `NearCacheConfiguration`（`Ignite.getOrCreateNearCache`，`Ignite.java` L394）。thin client 则完全没有 near（§3）。
- 断连期间：所有 cache 操作在 gate 处抛 `IgniteClientDisconnectedException`（§2.5），动态 cache 请求 future 被 fail（§2.4）。
- 重连后：按集群现状停/留 cache、重建 proxy 与索引 schema（§2.4）；`EVT_CLIENT_NODE_RECONNECTED` 且 server 已视其死亡的场合，**continuous query 与 remote listener 全部失效需自行重建**（ClientImpl L2329–2336 的显式告警）。

---

## 3. thin client 线协议（org.apache.ignite.client + internal/client/thin + processors/odbc）

### 3.1 入口与三类客户端

`ClientListenerProcessor` 在 kernal 启动时建 GridNioServer 挂 `ClientListenerNioListener`（`internal/processors/odbc/ClientListenerProcessor.java` L194；启动位详见 01 号报告）。一个端口三种方言，靠握手 byte 分流：`ODBC_CLIENT=0 / JDBC_CLIENT=1 / THIN_CLIENT=2`（`ClientListenerNioListener.java` L59–69）。TCP 帧统一为 **4 字节长度前缀 + 消息体**（`ClientListenerNioMessageParser.java` 类 javadoc L29–38）。

### 3.2 握手与版本协商

- **请求**（client 端组包 `internal/client/thin/TcpClientChannel.java` `handshakeReq` L843–876）：`[int 总长][byte 1=HANDSHAKE][short major][short minor][short patch][byte clientType=2]` +（版本支持时）`[byte[] features 位图]` + `[map userAttrs]` + `[string user][string pwd]`。
- **server 端**（`ClientListenerNioListener.onHandshake` L357–460）：首字节必须 1（L364–378），否则关连接；读三段版本（L374–380）与 clientType（L382）；`connCtx.isVersionSupported(ver)`（L391）通过则 `initializeFromHandshake` 完成认证与 handler/parser 创建（`platform/client/ClientConnectionContext.java` L188–234；thin 支持版本 1.0.0–1.7.0，`SUPPORTED_VERS` L85–92，DEFAULT 1.7.0）。成功响应（`ClientRequestHandler.writeHandshake` L171–180）：`[boolean true][byte[] features][UUID serverNodeId]`（后两项按版本特性）。

握手往返的字节布局（thin、1.7.0 特性集）：

```
client → server:
  [int len][byte 1][short 1][short 7][short 0][byte 2]     // HANDSHAKE + 版本 + THIN_CLIENT
  [byte[] features 位图]                                    // ≥1.7.0 (BITMAP_FEATURES)
  [map userAttrs] [string user][string pwd]                 // 有认证时
server → client (成功):
  [int len][boolean true][byte[] srvFeatures][UUID srvNodeId]
server → client (失败):
  [int len][boolean false][short major][short minor][short patch][string err][int statusCode?]
                                                              // statusCode 仅客户端版本≥1.1.0
```
- **协商失败处理**：server 回 `[boolean false][short×3 本端支持版本][string 错误][int 状态码（客户端版本≥1.1.0 时）]`（L425–460）；client 端（`TcpClientChannel.handshake` L715–840）按错误码分流：`AUTH_FAILED` 抛认证异常、`NODE_IN_RECOVERY_MODE` 抛恢复异常、**版本不一致且 server 版本可用 → 自动用 server 版本重发握手**（L814–820），否则抛 `ClientProtocolError`。

### 3.3 client 端连接结构（注意：2.18 无 ClientConnectionPool 类）

- `IgniteClient.start(cfg)` → `internal/client/thin/TcpIgniteClient.java`（L89，工厂 L493）→ **`ReliableChannel`**（L151）。`ReliableChannel.java`（L62–64，"Communication channel with failover and partition awareness"）持有 per-address 的 `ClientChannelHolder` 列表（L69），请求失败换下一个 channel 重试。
- 每个 channel 是 **`TcpClientChannel`**（L99，实现 `ClientChannel/ClientMessageHandler/ClientConnectionStateHandler`）：自增 reqId（L134）、pendingReqs future 表（L137）、心跳定时器（L170）；底层 socket 来自 **`ClientConnectionMultiplexer`** 接口（`io/ClientConnectionMultiplexer.java` L47–51 `open(addr, msgHnd, stateHnd)`），默认实现 `io/gridnioserver/GridNioClientConnectionMultiplexer`（复用 GridNio client 栈）。affinity-aware 分区路由由 `ClientCacheAffinityContext/Mapping` 与 server 下发的 affinity 版本配合完成（`ClientResponse` 的 flags 字段，§3.5）。

### 3.4 server 端 handler 分派

握手后每条消息：`ClientListenerNioListener.onMessage`（L177–255）取 `connCtx.parser()` 解码 → `ClientMessageParser.decode`（`platform/client/ClientMessageParser.java` L463–464 起，`short opCode` 大 switch 映射到 `ClientRequest` 子类）→ `ClientRequestHandler.handle`（`platform/client/ClientRequestHandler.java` L84–120：tx 请求先 `txCtx.acquire`）→ `handle0`（L123–146）：事务型 cache 请求走异步 `processAsync`（默认先等 10ms 再转异步响应，L52/L126–139），其余同步 `req0.process(ctx)`。异常统一经 `handleException`（L149 起）转状态码 + SQLState 前缀（SQL 类请求）。

### 3.5 cache op 子集：op 码与字节级例子

op 码表（`internal/client/thin/ClientOperation.java`）：**CACHE_GET=1000（L46）、CACHE_PUT=1001（L43）、CACHE_PUT_IF_ABSENT=1002（L108）、CACHE_GET_ALL=1003（L79）、CACHE_PUT_ALL=1004（L76）、CACHE_GET_AND_PUT=1005 … CACHE_GET_AND_PUT_IF_ABSENT=1008、CACHE_REPLACE=1009（L85）、CACHE_REPLACE_IF_EQUALS=1010（L82）、CACHE_CONTAINS_KEY=1011（L64）、CACHE_CONTAINS_KEYS=1012（L67）、CACHE_REMOVE_KEY=1016（L88）、CACHE_REMOVE_IF_EQUALS=1017、CACHE_REMOVE_KEYS=1018、CACHE_REMOVE_ALL=1019、CACHE_GET_SIZE=1020（L73）**；管理类 CACHE_CREATE/GET_OR_CREATE/DESTROY=1050–1056、CACHE_PARTITIONS=1101。

**CACHE_PUT 请求的字节级布局**（client 组包 `TcpClientChannel.send` L355–400 + server 解码链）：

```
[int  msgLen]                       // 长度前缀（写 0 占位后回填，L389/L396）
[short opCode = 1001]               // L390
[long  reqId]                       // L391；server 端 ClientRequest.java L37 readLong
[int   cacheId]                     // ClientCacheRequest.java L58-63 readInt
[byte  flags]                       // 同上 readByte：bit0=keepBinary 等（L98）
[obj   key]                         // ClientCacheKeyRequest.java L47 readObjectDetached
[obj   val]                         // ClientCacheKeyValueRequest.java L37 readObjectDetached
```

响应（`platform/client/ClientResponse.encode` L77–101）：`[long reqId][short flags（版本支持 partition awareness 时）][AffinityTopologyVersion（变更时）]`；失败再跟 `[int status][string err]`。**put 成功时无业务载荷**（flags 未标 error 即结束，L95–97）。server 端处理即 `ClientCachePutRequest.process0`：`cache(ctx).put(key(), val())`（`platform/client/cache/ClientCachePutRequest.java` L41–44）。

同族 op 的差异只在 key/value 段：`CACHE_GET`（1000）= `cacheId/flags + key`，响应多一个 value 对象；`CACHE_GET_ALL`（1003）把 key 段换成 `[int cnt]key*`（`ClientCacheGetAllRequest extends ClientCacheKeysRequest`），`CACHE_PUT_ALL`（1004）为 `[int cnt](key,val)*`（`ClientCachePutAllRequest.java` L49–50 逐对读）；`CACHE_CONTAINS_KEY`（1011）响应是 boolean；`CACHE_REMOVE_KEY`（1016）与 put 一样是空载荷响应；`CACHE_REPLACE`（1009）= key+newVal（`ClientCacheReplaceRequest extends ClientCacheKeyValueRequest`，响应 boolean），`CACHE_REPLACE_IF_EQUALS`（1010）再叠加期望旧值（`ClientCacheReplaceIfEqualsRequest.java` L41 在 KeyValue 之上多读一个 `newVal`，即 key+oldVal+newVal）。

### 3.6 查询 op 与游标协议

- **scan query**：`QUERY_SCAN=2000`（L139）。请求（`ClientCacheScanQueryRequest.java` L60–72）：`[cacheId/flags][obj filter（可 null）][byte filterPlatform][int pageSize][int partition（<0=null）][boolean loc]`。响应（`ClientCacheQueryResponse.java` L46–48 的 encode）：`[cursorId long][int rowCount][entries...][boolean hasNext]`——page 布局由 `ClientCacheQueryCursor.writePage`（L70–85）统一：**预留 int 写行数 → 每行一个 entry → hasNext 布尔；无下页时自动 release 游标资源**。翻页：`QUERY_SCAN_CURSOR_GET_PAGE=2001`（L142）携带 cursorId，响应仅再写一页（`ClientCacheQueryNextPageResponse.java` L46 的 encode）。游标数量受 `maxOpenCursorsPerConnection` 限制（`ClientConnectionContext.incrementCursors` L256–267；scan 请求在 process L89 注册游标、L96 入资源表）。
- **SQL fields query**：`QUERY_SQL_FIELDS=2004`（L150）。请求（`ClientCacheSqlFieldsQueryRequest.java` L70–83）：`[schema][pageSize][maxRows][sql][args[]][byte stmtType][booleans: distributedJoins/loc/replicatedOnly/enforceJoinOrder/collocated/lazy][includeFieldNames]`。响应（`ClientCacheSqlFieldsQueryResponse.java` L59–71 的 encode）：`[cursorId][int 列数][列名（includeFieldNames 时）][首页 page]`；翻页 `QUERY_SQL_FIELDS_CURSOR_GET_PAGE=2005`（L154）。scan（QUERY_SQL=2002）/query continuous（2006/2007）同构。
- **资源回收**：`RESOURCE_CLOSE=0`（L31）按 long id 关闭游标/连续查询句柄；`HEARTBEAT=1`（L34）保活。

### 3.7 JDBC thin 在协议栈的位置（一段话）

JDBC thin（`internal/jdbc/thin/JdbcThinConnection` 等）**同一个 ClientListener 端口、同一套 4 字节长度前缀帧，但是平行方言**：握手 clientType=1，版本序列独立（2.1.0→2.9.0+，`odbc/jdbc/JdbcConnectionContext.java` L56–77），消息是 `JdbcRequest` 族自有 op 码（如 `QRY_EXEC=2`、`BULK_LOAD_BATCH=13`，`JdbcRequest.java` L35/L68），连接上下文/解析器/handler 均与 thin client 分置（`JdbcConnectionContext` vs `ClientConnectionContext`）。ODBC 客户端（clientType=0）同理为第三方言。

### 3.8 COPY/BULK LOAD 的客户端推包协议（衔接 14 号）

1. client 提交 `COPY FROM '/local/file' INTO tbl(...)` —— 普通 JDBC `QRY_EXEC`（`JdbcThinStatement.java` L253–254 检测返回类型）。
2. server 解析 SQL 得 `SqlBulkLoadCommand`，构造 `BulkLoadProcessor`，**返回 `JdbcBulkLoadAckResult(cursorId, BulkLoadAckClientParameters)`**（`odbc/jdbc/JdbcRequestHandler.java` L671–683）。`BulkLoadAckClientParameters`（`internal/processors/bulkload/BulkLoadAckClientParameters.java` L39–51）= `{locFileName, packetSize}`，默认包大小 **4MB**（L46 `DFLT_PACKET_SIZE = 4*1024*1024`），合法区间 [1, Integer.MAX_VALUE-512]。
3. client 循环读本地文件按 packetSize 切块，逐块发 **`JdbcBulkLoadBatchRequest(cursorId, batchIdx, cmd, data)`**（`JdbcThinStatement.sendFile` L331–396，使用 sticky IO 固定同一条连接）；cmd 三值：`CMD_CONTINUE=0` / `CMD_FINISHED_ERROR=1` / `CMD_FINISHED_EOF=2`（`JdbcBulkLoadBatchRequest.java` L33–59）。server 侧流式交给 `BulkLoadCsvParser → BulkLoadCacheWriter/DataStreamerWriter` 落 cache。
4. EOF 包之后 server 返回统计（复制行数），整个推包结束。此协议在 thin client（clientType=2）中**无对应 op**——COPY 推包是 JDBC 方言专属。

---

## 4. 章 12「客户端形态与收尾弧」切课建议

切课纪律沿用 ROADMAP §4：一课一个新概念簇、单课 2–4h、宁多切课不一课双簇。依赖图（12.2 是全章枢纽）：

```
03 号数据面（值=byte[]）
      │
      ▼
    12.1 binary 格式与 schema ──► 12.2 metadata 集群交换 ──► 12.3 BinaryObject/builder
      │                                │                        │
      │         09 号 ring（server 半边）▼                        │
      └───────────────────────────► 12.4 ClientImpl（thick）      │
                                   │        │                     │
                     01 号启动位    ▼        └────────┐            │
                     08 号 NIO ──► 12.5 thin 握手+cache op ◄───────┘
                                   │
                      8/9 章 SQL 弧 ▼
                                12.6 thin 查询游标
                                   │
                    14 号 SQL/COPY  ▼
                                12.7 JSR-107 + metrics + COPY
```

| 课 | 概念簇 | tracer（红绿切片） | 依赖 |
|---|---|---|---|
| 12.1 binary 完整化（上）：格式与 schema | 24 字节头、type 代码、enum/集合/Map、NameMapper/IdMapper、BinarySchema + compactFooter 读写 | 手写 marshal/desmarshal 与 vendor `BinaryMarshaller` 字节逐位对照测试 | 03 号数据面（值已是 byte[]） |
| 12.2 binary 完整化（下）：metadata 集群交换 | BinaryMetadata/BinaryTypeImpl、MetadataUpdateProposed/Accepted 双消息 + coordinator 盖版本、join 全量（BINARY_PROC discovery 数据包）、client 拉取（TOPIC_METADATA_REQ） | 双节点：A 写新类型 → B keepBinary 按字段名读出值；断网窗口内写 → acceptedVer 对齐 | 12.1；02 号 discovery custom event 机制 |
| 12.3 BinaryObject API 与 builder | BinaryObject/Impl、field(typeId=0 类名内嵌)、BinaryObjectBuilder、withKeepBinary 全链、affinity 字段名消费 | builder 改一个字段重建对象 + keepBinary 读路径 e2e | 12.1、12.2 |
| 12.4 thick client：ClientImpl | router 单连接模型、join 三步（handshake/join/NodeAddFinished）、Reconnector 状态机、pendingMessages 补齐、`IgniteClientDisconnectedException` 触发面、GridClientPartitionTopology | kill router → client 换新 UUID 重连、cache 操作先抛断连异常后恢复 | 09 号 ring（server 半边已在）、12.2（重连后元数据） |
| 12.5 thin client（上）：握手与 cache op 双端 | 长度前缀帧、HANDSHAKE/版本协商/features、op 码、CACHE_* 请求响应布局、ResourceRegistry 游标句柄 | **复刻 server + 复刻 thin client put/get**；随后 **官方 ignite-client 连复刻节点** put/get（约束④前半） | 12.1（thin 也用 binary 编码）、01 号 ClientListenerProcessor 启动位、08 号 GridNioServer |
| 12.6 thin client（下）：查询游标与连接结构 | QUERY_SCAN/SQL_FIELDS + 分页游标、ReliableChannel failover、affinity-aware、（可选）partition awareness | 官方 ignite-client 对复刻节点跑 scan + SQL fields 翻页（约束④后半） | 12.5；查询执行依赖 8/9 章已有 SQL 弧 |
| 12.7 收尾弧：JSR-107 + metrics + COPY 回访 | CachingProvider/CacheManager 包装、`GridMetricManager`/`MetricRegistry` 最小实现、JDBC thin 方言与 COPY 推包（CMD_CONTINUE/EOF 往返） | JSR-107 标准入口建 cache 读写；JDBC thin COPY 一个本地 CSV 进复刻集群（约束⑤） | 12.5（端口/帧复用）、14 号 SQL 弧 |

- **五约束落位**：① ClientImpl = 12.4，与 12.5/12.6 thin client 相邻成对教学（同一章内先 thick 后 thin，对比"进拓扑的 client"vs"不进拓扑的 client"）；② binary 完整化显式范围 = 12.1–12.3 三课（格式/交换/API），marshalling 的非 binary 分支（OptimizedMarshaller/JdkMarshaller）**不设课**（binary-api 模块里仅作引用存在）；③ JSR-107 = 12.7（`cache/CachingProvider.java`、`cache/CacheManager.java` 皆是 Ignite API 的薄包装，半课足够）；④ thin client 双端验收拆进 12.5（cache op）与 12.6（查询）两课的 tracer，避免单课双簇；⑤ COPY 推包回访挂 12.7 尾段（协议本体仅 4 个类，10 行级讲义即可）。
- **metrics 最小实现（Q13）挂靠**：挂 12.7。理由：2.18 的 metrics 是 `internal/processors/metric/GridMetricManager.java`（L79，`extends GridManagerAdapter<MetricExporterSpi>`）+ 公开 `org.apache.ignite.metric.MetricRegistry`（组件侧自注册，如 `GridIoManager.java` L473 `ctx.metric().registry(COMM_METRICS)`），与 client 形态正交、且按 ROADMAP 决策 #9"不设专门课、随课弧生长"——收尾弧给一次性收口（kernal/discovery/communication/cache 四个 registry 各 1–2 个指标 + JMX 导出接缝）。
- **可简化项判断（默认全做，仅列真正可争议项）**：
  1. thin client 的 **partition awareness**（affinity 版本下发 + client 分区路由）：可简化为"总是把请求发给任一节点"。争议点：它是官方 client 性能卖点，但验收约束④只要求连通与正确性。建议：课内留 read-only 讲义，不做实现。
  2. **features 位图协商**（1.7.0 特性）：可只支持"空位图 + 1.7.0"。真客户端默认带位图，故服务器端必须解析（不可省），客户端侧复刻版可简化。
  3. **binary metadata 的 remove 协议**（MetadataRemoveProposed/Accepted，transport L145–146）：仅控制工具用，可降级为 no-op + 注释锚点。
  4. **compactFooter=false 分支**：全保真下应实现（分支极小，`BinaryWriterSchemaHolder` L114–139），不建议省——留给"格式对照"测试当红绿切片用例反而便宜。
  5. thick client 的 **IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY / throttle 退避细节**：可按常量直译，不值得花课时。
- 其余（24 字节头、双消息 metadata 协议、Reconnector、游标分页、COPY 三态）皆为形态本体，全部进课。

---

## 引用文件清单（全部实际打开验证，相对 `vendors/ignite/`）

**binary 双模块**
- `modules/binary/api/src/main/java/org/apache/ignite/binary/BinaryBasicNameMapper.java`
- `modules/binary/api/src/main/java/org/apache/ignite/binary/BinaryBasicIdMapper.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/GridBinaryMarshaller.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryContext.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryInternalMapper.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinarySchema.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinarySchemaRegistry.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryMetadata.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryTypeImpl.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryWriterSchemaHolder.java`
- `modules/binary/api/src/main/java/org/apache/ignite/internal/binary/BinaryUtils.java`
- `modules/binary/impl/src/main/java/org/apache/ignite/internal/binary/BinaryObjectImpl.java`
- `modules/binary/impl/src/main/java/org/apache/ignite/internal/binary/BinaryEnumObjectImpl.java`
- `modules/binary/impl/src/main/java/org/apache/ignite/internal/binary/BinaryReaderExImpl.java`
- `modules/binary/impl/src/main/java/org/apache/ignite/internal/binary/builder/BinaryObjectBuilderImpl.java`

**core：binary 处理器侧与配置**
- `modules/core/src/main/java/org/apache/ignite/configuration/BinaryConfiguration.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/CacheObjectBinaryProcessorImpl.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/BinaryMetadataTransport.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/BinaryMetadataFileStore.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/MetadataUpdateProposedMessage.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/MetadataUpdateAcceptedMessage.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/binary/ClientMetadataRequestFuture.java`
- `modules/core/src/main/java/org/apache/ignite/internal/IgniteKernal.java`（L1544、L3057–3187）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GatewayProtectedCacheProxy.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/IgniteCacheProxyImpl.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheGateway.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/GridCacheProcessor.java`（L876–981）
- `modules/core/src/main/java/org/apache/ignite/cache/CachingProvider.java`
- `modules/core/src/main/java/org/apache/ignite/cache/CacheManager.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/metric/GridMetricManager.java`（L79）
- `modules/core/src/main/java/org/apache/ignite/internal/managers/communication/GridIoManager.java`（L473）
- `modules/core/src/main/java/org/apache/ignite/Ignite.java`（L394）

**ClientImpl 与 exchange**
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ClientImpl.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/preloader/GridDhtPartitionsExchangeFuture.java`（L915–1060、L1499–1594、L3047）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/topology/GridClientPartitionTopology.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/cache/distributed/dht/GridDhtAssignmentFetchFuture.java`（L225）
- `modules/core/src/main/java/org/apache/ignite/internal/IgniteNeedReconnectException.java`

**thin client 协议（client 端）**
- `modules/core/src/main/java/org/apache/ignite/internal/client/thin/ClientOperation.java`
- `modules/core/src/main/java/org/apache/ignite/internal/client/thin/TcpIgniteClient.java`（L89、L151、L493）
- `modules/core/src/main/java/org/apache/ignite/internal/client/thin/ReliableChannel.java`
- `modules/core/src/main/java/org/apache/ignite/internal/client/thin/TcpClientChannel.java`
- `modules/core/src/main/java/org/apache/ignite/internal/client/thin/io/ClientConnectionMultiplexer.java`
- `modules/core/src/main/java/org/apache/ignite/client/ClientCacheConfiguration.java`（grep 验证无 near cache 字段）

**thin client 协议（server 端）**
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/ClientListenerProcessor.java`（L194）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/ClientListenerNioListener.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/ClientListenerNioMessageParser.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/ClientListenerRequest.java`（L25）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/ClientConnectionContext.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/ClientRequestHandler.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/ClientMessageParser.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/ClientRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/ClientResponse.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheKeyRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheKeyValueRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCachePutRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheScanQueryRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheSqlFieldsQueryRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheQueryResponse.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheQueryNextPageResponse.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheQueryCursor.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/platform/client/cache/ClientCacheSqlFieldsQueryResponse.java`

**JDBC thin 与 COPY**
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/jdbc/JdbcConnectionContext.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/jdbc/JdbcRequest.java`（L35、L68）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/jdbc/JdbcRequestHandler.java`（L630–683）
- `modules/core/src/main/java/org/apache/ignite/internal/processors/odbc/jdbc/JdbcBulkLoadBatchRequest.java`
- `modules/core/src/main/java/org/apache/ignite/internal/processors/bulkload/BulkLoadAckClientParameters.java`
- `modules/core/src/main/java/org/apache/ignite/internal/jdbc/thin/JdbcThinStatement.java`（L253–254、L318–396）

---

## 对复刻课的启示

1. **模块边界照抄 vendor 的 binary 拆分收益极大**：`binary-api`（格式/上下文/schema，纯数据结构）与 `binary-impl`（ heavyweight 实现）的切线正好是"课 12.1 可独立 TDD 的部分"——不依赖任何 kernal 组件即可与 vendor 做字节级对照。
2. **metadata 交换是 binary 弧的"分布式时刻"**：建议 tracer 直接打"跨节点 keepBinary 按字段名读"——它一次性逼通 addMeta → custom event → ring 广播 → accepted → 对端 schema 注册五步；join 全量交换（BINARY_PROC discovery 数据包）在 12.2 里用"重启后新增节点"场景覆盖。
3. **thick 与 thin 必须成对教**：ClientImpl 证明了"client = 一个不进 ring、单连 router、可换 UUID 重生的 discovery 成员"；thin client 证明了"client = 一条不带 discovery 的纯请求响应连接"。两者共享 binary 编码与 cache 语义，对照讲能把 09/10 号的 ring/exchange 知识收口。
4. **thin client 课的隐形依赖是 GridNioServer**（08 号）：server 端 4 字节长度前缀帧 + 首字节握手分流是仅有的传输层新知识，其余全是 op 码与 binary 编码的排列——适合作为"收尾弧"检验前期所有接缝的整合考试。
5. **COPY 推包是免费的协议课**：三态 cmd（CONTINUE/EOF/ERROR）+ 4MB 包 + sticky 连接，四个类讲完，还能回访 14 号 SQL 解析弧与 data streamer。
