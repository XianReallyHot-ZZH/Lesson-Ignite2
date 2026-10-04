# 09 · 发现层运行期：ring 消息全集、故障检测与 ring 愈合

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文聚焦 **join 完成之后的 ring 运行期**：消息家族、心跳/故障判定、环愈合、线程模型与 IP finder 生命周期（含多播 finder 深入，见 ADR 0005：发现层全保真复刻，`TcpDiscoveryMulticastIpFinder` 属主线）。join 启动路径（`spiStart → initLocalNode → joinTopology`）已由 [01-startup-path.md](01-startup-path.md) §3.7 覆盖，本文直接引用。所有引用路径相对 `vendors/ignite/`，行号以当前 submodule 为准。

---

## 0. 先决事实：三处"想当然"与 2.18 现实的差异

调研提示里的几个旧名在 2.18 源码中**已不存在**，复刻时以本文为准：

1. **没有 `DFLT_HEARTBEAT_FREQ`、没有独立 heartbeat 消息**。心跳即 `TcpDiscoveryMetricsUpdateMessage`，频率来自 `IgniteConfiguration.metricsUpdateFrequency`（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java:2159`；默认 2000ms，`modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java:122`）。
2. **没有 `SocketTimeoutWorker` 类、没有 `MetricsUpdater` 线程**（全 modules 检索为零命中）。写超时改由 `SocketTimeoutObject` + 内核 `GridTimeoutProcessor` 关闭 socket 实现（§2.3）；metrics 产生改为在 `RingMessageWorker` 的每轮 poll 前附带执行（§2.1）。
3. **没有 `safeMessage` 方法**。"确保送达"机制的真名是 `@TcpDiscoveryEnsureDelivery` 注解 + `PendingMessages` 队列 + `TcpDiscoveryDiscardMessage`（§3.3）。

另一个结构性事实：ring 消息分**两套序列化**。类声明含 `implements Message` 的走二进制协议（有 `directType()` 类型码）；纯 `TcpDiscoveryAbstractMessage`（JDK `Serializable`）走 JDK 序列化。发送时先写一个 mode 字节区分（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java:1699`）。

---

## 1. ring 消息全集（`spi/discovery/tcp/messages/`，24 类）

### 1.1 基类公共字段

所有消息继承 `TcpDiscoveryAbstractMessage`（`messages/TcpDiscoveryAbstractMessage.java:35`）：`id`（`IgniteUuid`，幂等去重键）、`verifierNodeId`（协调者盖章）、`topologyVersion`、`flags`（client/responded/reconSuccess/forceFail 位，L40–49）、`failedNodes`（piggyback 的失败节点集，L80）、`senderNodeId`（transient，每跳重写，L52）。

### 1.2 完整清单表

directType 空 = 不实现 `Message`，走 JDK 序列化。"产生者/通道"以源码为准：

| 消息类 | directType | 关键字段 | 产生者与通道 |
|---|---|---|---|
| `TcpDiscoveryJoinRequestMessage` | — | `node`, `dataPacket` | 加入节点 → 直连目标节点（不沿环）；非协调者收到后沿环转发给协调者（`ServerImpl.java:4017`、`:4677`） |
| `TcpDiscoveryNodeAddedMessage` | — | `node`(含分配的 internalOrder), `topology`/`topHist`/`msgs`(给加入者), `gridStartTime`, `dataPacket` | 协调者构造（`ServerImpl.java:4665`）后沿环整圈；每个节点 `ring.add`（`:5021`） |
| `TcpDiscoveryNodeAddFinishedMessage` | — | `nodeId`, `clientDiscoData`, `clientNodeAttrs` | 协调者在 NodeAdded 回到后构造（`ServerImpl.java:4865`）沿环；各节点升 topVer、`EVT_NODE_JOINED`（`:5180–5252`） |
| `TcpDiscoveryNodeLeftMessage` | 16 (`:50`) | 仅公共字段（creator=离开者） | 离开节点 `spiStop0` 构造（`ServerImpl.java:539`）沿环，协调者 verify（`:5405`） |
| `TcpDiscoveryNodeFailedMessage` | — | `failedNodeId`, `order`(internalOrder), `warning` | 检测到 next 失败的节点（`ServerImpl.java:3852`）或 `checkFailedNodesList`（`:6193`）；协调者 verify（`:5615`） |
| `TcpDiscoveryMetricsUpdateMessage` | 14 (`:236`) | `serversFullMetricsMsgs`, `connectedClientsMetricsMsgs`, `clientNodeIds` | **仅协调者**周期产生（`ServerImpl.java:6322`），沿环跑两圈（laps 0 收集/1 剥离，`:5894–5963`） |
| `TcpDiscoveryConnectionCheckMessage` | 6 (`:56`) | 仅公共字段 | 任何节点 `connCheckInterval` 内无外发时产生（`ServerImpl.java:6352–6360`）沿环；不进 pending 队列（`:3671`） |
| `TcpDiscoveryStatusCheckMessage` | — | `creatorNode/Addrs`, `failedNodeId`, `status` | 怀疑者构造（`ServerImpl.java:6344`、`:704`）沿环去协调者；协调者回 `STATUS_OK/STATUS_RECON` 直发创建者（`:5729–5808`） |
| `TcpDiscoveryDiscardMessage` | 9 (`:99`) | `msgId`, `customMsgDiscard` | 协调者在 ensured 消息走完一圈时产生（如 `ServerImpl.java:4885`、`:5605`）沿环；各节点据此清 pending（`:5996`） |
| `TcpDiscoveryCustomEventMessage` | — | `msg`(transient), `msgBytes` | 任意节点 `sendCustomMessage` 注入；协调者 verify 后沿环（`ServerImpl.java:6051–6135`）；`TcpDiscoveryServerOnlyCustomEventMessage`（`messages/TcpDiscoveryServerOnlyCustomEventMessage.java:28`）为其不下发 client 的子类 |
| `TcpDiscoveryClientReconnectMessage` | — | `routerNodeId`, `lastMsgId`, `msgs` | client 重连时发给 router，经 ring 到协调者补齐历史（`ServerImpl.java:6977–7005`） |
| `TcpDiscoveryClientPingRequest` / `Response` | 3 (`:72`) / 4 (`:92`) | `nodeToPing`, `clientNodeId` / `res` | client 经 router 中转的 ping（`ServerImpl.java:6006–6045`），不走环 |
| `TcpDiscoveryClientMetricsUpdateMessage` | 13 (`:76`) | metrics 载荷 | client → router 定向发送，router 本地消化不入环（`ServerImpl.java:7157–7177`） |
| `TcpDiscoveryClientAckResponse` | 15 (`:82`) | `msgId` | router → client 的回执 |
| `TcpDiscoveryHandshakeRequest` | 8 (`:87`) | `prevNodeId`, `dcId` | 每次 openSocket 后必发（`ServerImpl.java:3421`）；环恢复时带 `previousNodeId` 触发对端反查（§3.2） |
| `TcpDiscoveryHandshakeResponse` | 10 (`:141`) | `order`, `prevNodeAliveFlag`, `redirectAddrsMsgs` | 被连节点应答自身 internalOrder / 前驱存活 / DC 重定向（`ServerImpl.java:6722–6814`） |
| `TcpDiscoveryPingRequest` / `Response` | 1 (`:72`) / 2 (`:68`) | 目标 nodeId / `clientExists` | server 端口上的直连 ping 协议（`SocketReader` 分支，`ServerImpl.java:6672–6707`） |
| `TcpDiscoveryRingLatencyCheckMessage` | 7 (`:95`) | `maxHops`, `curHops` | 手工诊断 API `checkRingLatency` 注入（`ServerImpl.java:1921–1928`），沿环计跳 |
| `TcpDiscoveryDuplicateIdMessage` | 12 (`:76`) | 重复的 nodeId | join 拒绝族：**直连发回加入者**，置 `DUPLICATE_ID`（`ServerImpl.java:7007–7035`） |
| `TcpDiscoveryAuthFailedMessage` | 11 (`:87`) | 目标节点 | join 拒绝族，同上置 `AUTH_FAILED`（`ServerImpl.java:7036–7063`） |
| `TcpDiscoveryCheckFailedMessage` | 0 (`:67`) | `error` | join 拒绝族（版本/属性检查失败），置 `CHECK_FAILED`（`ServerImpl.java:7064–7106`） |
| `TcpDiscoveryLoopbackProblemMessage` | 5 (`:96`) | 错误描述 | join 拒绝族，置 `LOOPBACK_PROBLEM`（`ServerImpl.java:7107–7135`） |
| `TcpDiscoveryDummyWakeupMessage` | — | 无 | 进程内 `WAKEUP` 哨兵，唤醒阻塞在 poll 的 msgWorker（`ServerImpl.java:217`），从不序列化 |
| 内嵌值对象：`InetAddressMessage`(-100)、`InetSocketAddressMessage`(-101)、`TcpDiscoveryNodeMetricsMessage`(-102)、`TcpDiscoveryCacheMetricsMessage`(-103)、`TcpDiscoveryClientNodesMetricsMessage`(-104)、`TcpDiscoveryNodeFullMetricsMessage`(-105) | 各见行号 | 地址/metrics 载荷 | 仅作为 `MetricsUpdateMessage`/`HandshakeResponse` 的内嵌字段序列化，不独立入环 |

"沿环转传规则"统一为：`RingMessageWorker.processMessage` 分派（`ServerImpl.java:3145–3186`，未知类型 assert false），处理后 `sendMessageToRemotes` 判定还有远端则 `sendMessageAcrossRing` 发给 next（`:3303`）；回到协调者或过期消息被 `Discard` 终结。入队去重对 StatusCheck/JoinRequest/CustomEvent/ClientReconnect 按 `id` 幂等（`:2932–2941`）。

### 1.3 ensured（确保送达）消息集

`@TcpDiscoveryEnsureDelivery` 标注于：`NodeAdded`/`NodeAddFinished`/`NodeLeft`/`NodeFailed`/`CustomEvent`/`ClientReconnect`（各文件类头，如 `messages/TcpDiscoveryNodeAddedMessage.java:35`）。判定函数 `TcpDiscoverySpi.ensured`（`TcpDiscoverySpi.java:2040`）查注解。这些消息注册进 pending、断连换 next 后重发、被 `Discard` 清除（§3.3）。

---

## 2. 故障检测

### 2.1 "心跳"的产生与静默检测

- 协调者在 `RingMessageWorker` **每次 poll 队列前**（`beforeEachPoll`，`ServerImpl.java:2881–2887`）调 `sendMetricsUpdateMessage`：距上次发出 ≥ `metricsUpdateFreq`（默认 2s）且自己是协调者才产生 `TcpDiscoveryMetricsUpdateMessage`（`ServerImpl.java:6316–6329`）。非协调者从不主动产生（`processMetricsUpdateMessage` 对非法来源丢弃，`:5879–5892`）。
- 接收侧静默监测：`metricsCheckFreq = 3 * metricsUpdateFreq + 50`（默认约 6.05s，`ServerImpl.java:2855`）。`checkMetricsReceiving`（`:6335–6347`）在"距上次收到 metrics/环消息"超时后发出 `StatusCheckMessage` 沿环找协调者仲裁：`STATUS_OK` 忽略，`STATUS_RECON` 触发 `EVT_NODE_SEGMENTED`（`:5831–5841`）。协调者若发现创建者已不在拓扑，直发 `STATUS_RECON` 令其重连（`:5737–5808`）。
- 连接活性探测：`connCheckTick = effectiveExchangeTimeout()/3`，`connCheckInterval = min(connCheckTick, 500ms)`（`ServerImpl.java:417–421`，`MAX_CON_CHECK_INTERVAL=500` 见 `:220`；`effectiveExchangeTimeout` = failureDetectionTimeout 或 sockTimeout+ackTimeout，`:2026–2029`）。`checkConnection`（`:6352–6360`）在 `connCheckInterval` 内没向 next 发过任何消息时补发一条 `ConnectionCheckMessage`——**环上没有空闲读，活性完全靠发送流量证明**。

### 2.2 发送失败的重试与超时升级

`sendMessageAcrossRing` 的重试策略（`ServerImpl.java:3399–3775`）：

- 每消息每地址 `reconCnt`（默认 10，`TcpDiscoverySpi.java:270`）次重试；`SocketTimeoutException` 时 `ackTimeout0` 翻倍，上限 `maxAckTimeout`（默认 10 分钟，`TcpDiscoverySpi.java:282`；`checkAckTimeout` 校验）——`ServerImpl.java:3577–3583`、`:3749–3755`。
- 启用 `failureDetectionTimeout` 时改为整体预算制：超预算即放弃该地址（`ServerImpl.java:3575`、`:3743–3744`），且 sock/ack/reconnect 配置被忽略（`TcpDiscoverySpi.java:2208`）。

### 2.3 socket 写超时：`SocketTimeoutObject`（替代旧 `SocketTimeoutWorker`）

所有 socket 写包在 `try (SocketTimeoutObject t = startTimer(sock, timeout))` 里（`TcpDiscoverySpi.java:1694`、`:1756`、`:1779`）。`SocketTimeoutObject`（`:2459–2529`）实现 `IgniteSpiTimeoutObject`，经 `IgniteSpiAdapter.addTimeoutObject` 挂到**内核 `GridTimeoutProcessor`**（`modules/core/src/main/java/org/apache/ignite/spi/IgniteSpiAdapter.java:954–960`）。超时回调 `onTimeout` 直接 `U.closeQuiet(sock)`（`TcpDiscoverySpi.java:2492–2505`），让阻塞在 `OutputStream.write` 的线程以 IOException 返回；正常完成则 `close()` 撤销定时器，若定时器已并发触发则抛 `SocketTimeoutException`（`:2518–2523`）。读侧则用 `sock.setSoTimeout`（如 `ServerImpl.java:6623`）。

### 2.4 failed 判定路径（crash 检测）

1. **谁判定**：只有"给该节点发消息的发送方"（即失败节点的环上前驱）。`sendMessageAcrossRing` 对 next 所有地址重试耗尽后把 next 计入本地 `failedNodes`（`ServerImpl.java:3787–3852`），并立即向自己投递 `TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder())`（`:3852`）。
2. **仲裁**：消息沿环到协调者盖 `verifierNodeId`（`:5603–5616`）；沿路各节点校验发送者在环内、`failedNode.internalOrder()` 与消息一致（`:5540–5578`），不一致丢弃（防旧拓扑幽灵消息）。
3. **应用**：verified 后各节点 `ring.removeNode`、协调者 `incrementTopologyVersion`、fire `EVT_NODE_FAILED`（`:5618–5680`）。
4. **兜底**：`checkFailedNodesList`（`:6172–6217`）周期复查——若失败检测者自己已离环（`nodeAlive(failSndNode)` 为假）且 `NodeFailed` 尚未广播（`failedNodesMsgSent`，`ServerImpl.java:269`），由本节点补发。
5. **消息 piggyback**：环上每条 ensured 消息携带 `failedNodes` 集合随行传播（`addFailedNodes`，`:3943`；接收侧合并进本地 `failedNodes`，`processMessageFailedNodes`，`:2370–2413`），使前驱更换期间的失败信息不丢。
6. **分网判定**：`CrossRingMessageSendState`（`:8135–8231`）状态机 `STARTING_POINT → FORWARD_PASS → BACKWARD_PASS → FAILED`；在 `connectionRecoveryTimeout`（默认取 `DFLT_FAILURE_DETECTION_TIMEOUT`，`TcpDiscoverySpi.java:288`）内先向前找新 next、再回退验证，全部失败则 `segmentLocalNodeOnSendFail` 置 `RING_FAILED` 本节点分段（`ServerImpl.java:3906–3921`）。

### 2.5 优雅离开（nodeLeft）与崩溃的差异

| 维度 | `NodeLeft`（优雅） | `NodeFailed`（崩溃） |
|---|---|---|
| 发起 | 离开节点自己在 `spiStop0` 构造（`ServerImpl.java:537–551`），并等协调者 verify 最多 `netTimeout`（默认 5s）才继续停机（`:553–581`） | 前驱检测发送失败后构造（`:3852`） |
| 语义 | 离开者自己沿环"亲自走一圈"（`:5360`）；若它已是最后节点且 finder 共享，先 `unregisterAddresses`（`:5339–5347`）再置 `LEFT` | 检测者代为广播 |
| 对前驱的特殊义务 | 失败节点的环上前驱（`next == leavingNode`）要把 verified `NodeLeft` **直写**给离开者让它安心退出，然后 `forceSndPending=true` 强制向新 next 重发全部 pending（`:5450–5473`） | 无 |
| 拓扑事件 | `EVT_NODE_LEFT`（`:5481`） | `EVT_NODE_FAILED`（`:5675`） |

两者的共同尾部：verified → `ring.removeNode` → 协调者 `incrementTopologyVersion` → 各节点 `topologyVersion(topVer)` 单调校验 → fire 事件 → `checkPendingCustomMessages()`（`:5492–5514`、`:5618–5693`）。

---

## 3. ring 愈合算法

### 3.1 环的数据结构：internalOrder 即环序

`TcpDiscoveryNodesRing`（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/internal/TcpDiscoveryNodesRing.java:49`）持 `TreeSet<TcpDiscoveryNode>`，比较器按 `internalOrder` 排序（`internal/TcpDiscoveryNode.java:542–551`）。`nextNode(excluded)` 取本地节点的排序后继（回绕到首，`TcpDiscoveryNodesRing.java:501–536`）；`previousNode` 取前驱（`:548`）。**加入必追加**：`ring.add` 断言新节点 internalOrder 严格大于当前最大（`:251`），由协调者 `nextNodeOrder()` 递增分配（`ServerImpl.java:4658`；计数器在 `TcpDiscoveryNodesRing.java:683–695`）。协调者 = 环上 server 节点的最小 internalOrder（`:434–470`）。client 节点不占环位（`serverNodes` 过滤 `clientRouterNodeId()==null`，`:720–728`）。

连接拓扑：每个 server 节点只维护**一条到 next 的主动出站 socket**（`RingMessageWorker.sock`，`ServerImpl.java:2836`）；所有入站连接各由一个 `SocketReader` 线程服务（§4.1）。所以"环"= 出站 socket 链 + 每个节点内存里同一份按 internalOrder 排序的节点表。

### 3.2 断连检测与换 next（`sendMessageAcrossRing` 的 `ringLoop`）

文字时序——假设环 A→B→C→A，B 崩溃，A 检测：

1. A 向 B 发任一消息失败（重试耗尽，§2.2）→ 若启用 `connectionRecoveryTimeout`，建 `CrossRingMessageSendState`（`ServerImpl.java:3779–3780`）。
2. A 把 B 加入本地 `failedNodes`，`ring.nextNode(failedNodes)` 得 C；关闭到 B 的 socket，向 C 发 `HandshakeRequest`，**恢复场景下携带 `previousNodeId=ring.previousNodeOf(C)`**（`:3424–3427`）。
3. C 收到带 `previousNodeId` 的握手后**反查自己的前驱**（即 B）：若距上次收到环消息未超 `effectiveExchangeTimeout`，再用 `connCheckTick/2` 超时对 B 的全部地址并发 ping（`SocketReader` 分支 `ServerImpl.java:6754–6807` + `checkConnection(node,timeout)` `:7270–7343`）；把结果写进 `HandshakeResponse.previousNodeAliveFlag`。
4. A 读响应：若 `previousNodeAlive=true`（B 只是慢），撤销怀疑、回头重试 B（`failedNodes.remove` + `continue ringLoop`，`:3448–3477`）；若 false，C 正式成为新 next。双方还互验 nodeId/internalOrder 防接错环（`:3490–3533`）。
5. A 向 C 补发全部 pending（ensured）消息并附上 `failedNodes`（`:3613–3668`、`:3682`），然后广播 `TcpDiscoveryNodeFailedMessage`（§2.4）。C→A 段无需任何动作——C 的出站本来就不经过 B，A 换 next 后环闭合为 A→C→A。
6. 若整圈都试完（超 `connectionRecoveryTimeout`），本地 `RING_FAILED` 分段（§2.4 第 6 条）。

### 3.3 ensured 消息的重发保障

- 发送成功即 `registerPendingMessage`：仅 `spi.ensured(msg)` 的消息进 `PendingMessages`（上限 1024，`ServerImpl.java:2694`），非 ensured（如 `ConnectionCheckMessage`）发完即弃（`:3976–3987`、`:3671`）。
- 触发重发的三个条件：检测到新失败（`failure`）、换了 next（`newNextNode`）、`NodeLeft` 强制（`forceSndPending`）——`ServerImpl.java:3613`。全部 pending 连同 `failedNodes` 重放给新 next（`:3628–3668`）。
- 若本节点成了环上唯一节点（`ring.nextNode` 返回 null 且无远端）：pending 逐条投回自己的消息队列本地消化（`processPendingMessagesLocally`，`:3881–3901`）。
- 终结：协调者见到 ensured 消息转回自己且 verified，发 `DiscardMessage(msgId)` 沿环，各节点把 pending 队列中该消息**及其之前**的全部清除（`processDiscardMessage`，`:5980–6001`；`PendingMessages.discard`，`:2735–2764`）。
- 另一条独立的历史队列 `EnsuredMessageHistory`（`:2447`，容量 `IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE`）只服务 **client 重连补齐**（`ClientReconnectMessage.lastMsgId` 之后的差量），与环愈合无关。

### 3.4 优雅离开时序（A→B→C→A，B 主动 stop）

1. B 的 `spiStop0` 构造 `TcpDiscoveryNodeLeftMessage` 投入自己的消息队列，随后阻塞等待 `spiState == LEFT` 最多 `netTimeout`（`ServerImpl.java:537–581`）。
2. B 的 msgWorker 处理该消息：creator 是本地 → 置 `STOPPING`；沿环发给 A（B 的 next）（`processNodeLeftMessage`，`:5326–5363`）。
3. A 收到：`leavingNodes.add(B)`；自己不是协调者，原样转发给 C；协调者 C 盖 `verifierNodeId`（`:5391–5409`）。
4. verified 的 `NodeLeft` 再走 C→A→B：A（B 的前驱）`ring.removeNode(B)`、`topologyVersion++`（协调者侧）或采纳消息里的 topVer、fire `EVT_NODE_LEFT`；A 还把 verified 消息**直写**给 B（`next == B`），然后 `forceSndPending=true`、关闭旧 socket、换 next（`:5411–5473`）。
5. B 收到 verified 消息回到自己：若环上已无远端且 finder 共享，`unregisterAddresses`；置 `LEFT`，`spiStop0` 的等待解除，继续销毁线程/socket（`:5338–5358`、`:584–616`）。
6. 消息转回协调者时由协调者发 `Discard` 清理各站 pending（`:5394–5403`）。

### 3.5 心跳双圈时序（metrics 的两 lap 语义）

协调者每 `metricsUpdateFreq` 产生一条 `MetricsUpdateMessage`（`ServerImpl.java:6316–6329`）。**lap 0**（消息刚产生）：每个途经节点把自己的 server metrics、cache metrics、所挂 client 的 metrics 塞进消息再转发（`:5907–5921`）；**lap 1**（消息回到协调者后再次入环）：每个节点把自己的条目剥离，同时协调者核对 `clientNodeIds`——声明存活却没交 metrics 的 client 节点，在 `clientFailureDetectionTimeout` 后由协调者直接构造 `NodeFailedMessage` 广播（`:5922–5962`）；跑满 2 圈的消息在各站被丢弃（`:5894–5899`）。消息里始终只携带"上一圈以后新增/未剥离"的条目，环越大单条消息越小。

### 3.6 节点加入时的环序（与 01 报告 §3.7 衔接）

新节点 join 由协调者分配 `internalOrder = nextNodeOrder()`（§3.1），**总是追加在环尾**（即当前最大序的 next、最小序的 previous）。`NodeAdded` 沿环走一圈让所有节点 `ring.add`；回到协调者后发 `NodeAddFinished` 走第二圈，各节点在此时升拓扑版本、fire `EVT_NODE_JOINED`（`ServerImpl.java:5131–5252`）。加入者自身则在收到属于自己的 `NodeAdded` 时 `restoreTopology` 一次性拿到全量（`:5053–5090`），并把发送方附带的 pending 消息集 `pendingMsgs.reset(msg.messages())` 初始化（`:5089`）。

---

## 4. worker 线程模型与 IP finder 生命周期

### 4.1 ServerImpl 全部线程清单

| 线程/池 | 类与位置 | 职责 |
|---|---|---|
| `tcp-disco-msg-worker-#[#]` | `MessageWorkerDiscoveryThread` 包 `RingMessageWorker`（`ServerImpl.java:7930`、`:2816`，start `:439–440`） | **单线程总引擎**：`processMessage` 分派（`:3073`）、沿环发送（`sendMessageAcrossRing`）、每轮 poll 前的周期检查（`beforeEachPoll` `:2881–2887`；队列空闲 10ms 时的 `noMessageLoop` `:3232–3245`：checkConnection / metrics 产生与接收检查 / pending custom / failedNodes 兜底） |
| `tcp-disco-srv-#[#]` | `TcpServerThread`→`TcpServer`（`:6404`、`:6432`，start `:464`） | server 端口 accept 循环，每个入站连接派生一个 SocketReader（`:6510`） |
| `tcp-disco-sock-reader-[]` × N | `SocketReader`（`:6576`） | 每入站连接一个：magic 头校验 → 握手应答（含前驱反查 §3.2）→ 循环读消息、回执 `RES_OK`、`msgWorker.addMessage` 入队（`:6933–7177`）；client 连接则改挂 `ClientMessageWorker`（`:6827–6867`）。**五类消息在 reader 内直答、不进环队列**：`PingRequest`（即问即答后关流，`:6672–6707`）、`ConnectionCheckMessage`（只刷新 `lastRingMsgReceivedTime` 并回执，`:6952–6957`）、join 拒绝族四消息（置本节点 join 状态机后丢弃，`:7007–7135`）、`ClientMetricsUpdateMessage`（本地消化，`:7157–7177`）、`ClientReconnectMessage`（部分场景直答，`:6977–7005`） |
| `ClientMessageWorker` × 客户数 | `ServerImpl.java:7623` | 单独向某个 client 节点写消息（含 NodeAdded 定制版、AckResponse），不占环 |
| `tcp-disco-ip-finder-cleaner` | `IpFinderCleaner`（`:2225`，start `:490–492`） | 仅协调者 + 共享 finder：每 `ipFinderCleanFreq`（默认 60s，`TcpDiscoverySpi.java:276`）清理（§4.3） |
| `StatisticsPrinter`（可选） | `ServerImpl.java:7594`，start `:479–482` | 周期打印 discovery 统计 |
| `disco-pool`（`utilityPool`） | `ServerImpl.java:229`、`:333` | ping 并发探测（`:7292`）、status 直答、认证检查等杂务 |
| （无）socket 超时 | `SocketTimeoutObject` 挂内核 `GridTimeoutProcessor`（§2.3） | 超时关 socket，不是 SPI 线程 |

`spiStop0` 的收尾顺序（`ServerImpl.java:523–616`）：发 `NodeLeft` 等 verify → 停 TcpServer → interrupt 全部 SocketReader → 停 IpFinderCleaner → 停 msgWorker → 停 client workers → 关 utilityPool → 停 statsPrinter → `ring.clear()`。

`RingMessageWorker` 的消息队列是 `LinkedBlockingDeque`（`MessageWorker` 基类，`ServerImpl.java:7990`），`addMessage(msg, ignoreHighPriority, fromSocket)` 对 StatusCheck/JoinRequest/CustomEvent/ClientReconnect 四类做按 `id` 的重复投递过滤（`:2932–2941`）；worker 每 10ms poll 一次（构造参数，`:2879`），poll 超时进入 `noMessageLoop` 周期检查（循环骨架 `:8021–8033`）。这决定了复刻的时间粒度：**所有周期行为最细 10ms 一档**。

### 4.2 ClientImpl（client 节点 discovery）体量与建议

`ClientImpl`（`modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ClientImpl.java:146`，2819 行）：不维护环，通过 router 节点的一条连接收发；组件为 `SocketReader`（`:1105`）、`SocketWriter`（`:1276`）、`Reconnector`（`:1539`）、`MessageWorker`（`:1710`）、`MetricsSender`（`:1087`，跑在 scheduled executor 上）与看门狗线程（`:308`）；client 的 join/reconnect/心跳补齐（`ClientReconnectMessage`、`ClientMetricsUpdateMessage`、`ClientPingRequest`）都由 router 侧的 `ClientMessageWorker` 配合（§1.2、§4.1）。

**课程建议**：client 节点 discovery 推迟到章 12（收尾/thin client）或独立选修课。理由：它是"环协议的消费者"而非组成部分；server 侧 `ClientMessageWorker` 已随 `SocketReader`/`processClientReconnectMessage` 自然进入 2.x 主线课；ClientImpl 自身的 Reconnector/状态机是独立概念簇，塞进章 2 会违反"一课一簇"。

### 4.3 IP finder 生命周期（server 侧时机）

- **注册（spiStart）**：共享 finder → `registerLocalNodeAddress` 调 `ipFinder.initializeLocalAddresses`（带 joinTimeout 内的重试循环，`ServerImpl.java:2057–2096`，调用点 `:469`）；非共享 finder 必须预配地址否则抛异常（`:470–477`）。
- **新节点地址入 finder**：协调者在 `NodeAddFinished` 处理时对共享 finder `registerAddresses(node.socketAddresses())`（`ServerImpl.java:5234`，位于 `processNodeAddFinishedMessage`）。
- **注销（离开）**：仅当共享 finder 且本地是最后一个节点，`NodeLeft` 处理中 `unregisterAddresses`（`ServerImpl.java:5339–5347`）。
- **周期清理**：协调者每 60s：对 finder 中注册但不在拓扑的地址逐个 ping，不通则注销；对拓扑有而 finder 缺的地址补注册（`cleanIpFinder`，`ServerImpl.java:2262–2345`）。

三个 finder 一句话对照：

| finder | 一句话 |
|---|---|
| `TcpDiscoveryVmIpFinder` | 纯内存地址集合 + 配置解析（支持 host:port..range、系统属性 `IGNITE_TCP_DISCOVERY_ADDRESSES`），默认非共享（`ipfinder/vm/TcpDiscoveryVmIpFinder.java:54`、`:59–89`、`:263–283`） |
| `TcpDiscoverySharedFsIpFinder` | 共享目录下以 `ip#port` 空文件作地址登记的文件共享 finder，默认共享（`ipfinder/sharedfs/TcpDiscoverySharedFsIpFinder.java:50–56`） |
| `TcpDiscoveryMulticastIpFinder` | 见 §4.4，主线复刻对象 |

### 4.4 `TcpDiscoveryMulticastIpFinder` 深入（主线）

类：`ipfinder/multicast/TcpDiscoveryMulticastIpFinder.java`（956 行），**继承 `TcpDiscoveryVmIpFinder` 且构造即 `setShared(true)`**（`:82`、`:148–150`）——即地址集合本身仍是内存版，"共享"语义完全靠多播交换协议实现。

**常量**：组地址 `DFLT_MCAST_GROUP="228.1.2.4"`（`:84`）、端口 `DFLT_MCAST_PORT=47400`（`:87`）、应答等待 `DFLT_RES_WAIT_TIME=500ms`（`:90`）、请求尝试 `DFLT_ADDR_REQ_ATTEMPTS=2`（`:93`）、TTL 默认 `-1`（用系统默认，0–255 合法，`:120–121`）。

**消息格式**（自定义 UDP 报文，无 discovery 消息类）：

- 地址请求 = 裸 `U.IGNITE_HEADER` 4 字节（`MSG_ADDR_REQ_DATA`，`:96`）；
- 地址响应 = `IGNITE_HEADER(4B) + JDK 序列化的 Collection<InetSocketAddress>`，上限 64KB（内部类 `AddressResponse`，`:718–774`，`MAX_DATA_LENGTH=720`），反序列化用节点的 jdkMarshaller（`:167–172`）。

**启动（`initializeLocalAddresses`，`:319–380`）**：解析全部非回环本地地址（`:436–501`），为每个地址建一个 **`AddressSender`** 线程（`tcp-disco-multicast-addr-sender`，`:816`）：`new MulticastSocket(47400)` + `setLoopbackMode(false)`（允许同机多节点）+ `setInterface(网卡)` + `joinGroup(228.1.2.4)` + 可选 `setTimeToLive`（`createSocket`，`:851–868`）；循环 `receive` 校验头后，把预序列化的本节点 discovery 地址**单播回请求方**（`:888–920`）。一个都建不起来则 `mcastErr=true`（`:379`）。

**发现（`getRegisteredAddresses`，`:390–429`）**：SPI 每次需要地址时触发。请求方开**临时** `MulticastSocket(0)`（回环关闭、SoTimeout=500ms、可选 TTL，`requestAddresses`，`:555–668`）：最多 2 次发送请求（失败间隔 500ms、尝试间隔 200ms），每次发送后在 `resWaitTime` 窗口内循环收多个响应；多网卡场景为每个接口派一个 `AddressReceiver` 线程并发请求（`:507–543`、`:779–810`）。收到的远端地址 `registerAddresses` 进本地集合（`:423`）。**降级**：首次请求失败（`mcastErr && firstReq`）且无预配地址时回落 `localhost:47500` 并告警（`:407–416`）。

**注销/停止**：无独立注销协议——`close()` 仅 interrupt 全部 `AddressSender`（`:678–686`），离开组的 socket 随之关闭，其他节点靠 SPI 层清理（§4.3 的 `IpFinderCleaner` ping 失败注销 + `VmIpFinder.unregisterAddresses` 只删本地内存）。

**互操作语义**：请求/响应只在多播 finder 之间交换；但收到的地址进的是 `VmIpFinder` 的静态集合，所以**预配置地址与多播学到的地址合流**（`super.getRegisteredAddresses()`，`:320–323`、`:428`）；纯 `VmIpFinder` 节点无法被多播发现，只能靠在多播节点侧预配其地址来互通。端口向 SPI 上下文登记为 UDP（`onSpiContextInitialized`，`:383–387`）。

---

## 5. 分级结论：章 2 重切建议（2.3/2.4 → 五课）

现状锚点：ROADMAP 章 2 里程碑"2 节点互相发现"；ADR 0005 要求 ring 消息全集/故障检测/环愈合/多播 finder 全保真。按"一课一个新概念簇、单课 2–4h agent 实施"拆：

| 课 | 主题 | 概念簇（新） | tracer bullet | 依赖 |
|---|---|---|---|---|
| 2.3 | **join 与环建立** | `TcpDiscoveryNodesRing`/internalOrder、JoinRequest→NodeAdded→NodeAddFinished 两圈协议、handshake、pendingMsgs 初始化 | 双节点组网，第三节点热加入触发 `EVT_NODE_JOINED` | 2.2（SPI 框架） |
| 2.4 | **ring 消息总线与转传机制** | `RingMessageWorker` 单线程模型、`sendMessageAcrossRing`、ensured 注解 + PendingMessages + Discard、消息去重/序列化双轨（directType vs JDK） | 环上广播一条自定义 ensured 消息，断言每站 pending 被 Discard 清空 | 2.3 |
| 2.5 | **故障检测与 ring 愈合** | 心跳（MetricsUpdate 双圈）与静默判定、ConnectionCheck/connCheckInterval、NodeFailed 判定链、换 next + previousNodeId 反查、NodeLeft 优雅离开、CrossRingMessageSendState 分段 | `kill -9` 一节点，其余节点在 failureDetectionTimeout 内收敛到新环；优雅 stop 触发 `EVT_NODE_LEFT` | 2.4 |
| 2.6 | **多播 IP finder** | `AddressSender`/`AddressReceiver`、UDP 请求/响应报文、joinGroup/TTL、降级路径、与 VmIpFinder 的合流 | 零预配地址：两节点仅靠多播互相发现 | 2.3（只依赖 spiStart 接缝，可与 2.4/2.5 并行） |
| 2.7 | **custom 通道与 IP finder 生命周期** | `TcpDiscoveryCustomEventMessage` 通道（verify/postpone/ackMessage）、`IpFinderCleaner` 清理-补注册、`StatusCheckMessage` 仲裁与 `EVT_NODE_SEGMENTED` | 环上跑一条 custom 事件 + 拔掉一个节点地址后 60s 内被 cleaner 清出 finder | 2.5、2.6 |

边界说明与"小并入"判断：

- **StatusCheck/分段（实现量小，但不并入 2.5 主体验收）**：其判定链（`STATUS_RECON`→segmented）横跨心跳与 custom 前置知识，放 2.5 讲判定、2.7 给 tracer 即可，避免 2.5 出现双验收簇。
- **`SocketTimeoutObject` 写超时（实现量小）**：并入 2.4（它是"发送"语义的一部分，且依赖 `GridTimeoutProcessor` 接缝），不值得独立课。
- **消息内嵌值对象（-100～-105，小）**：并入 2.4 的序列化双轨簇，与 MetricsUpdate 载荷一起实现。
- **join 拒绝族四消息（DuplicateId/AuthFailed/CheckFailed/LoopbackProblem，小）**：并入 2.3（它们只是 JoinRequest 的错误分支，复用直连通道）。
- **client 节点 discovery（ClientImpl + ClientMessageWorker）**：不进章 2 主体（§4.2），仅 `ClientMessageWorker` 的 socket 生命周期随 2.4 的 SocketReader 顺带落地；完整 client 协议留章 12。
- **SharedFs/JDBC finder**：非主线，附录提及即可（接口契约已在 2.6 定义）。

课间依赖形成有向链 2.3 → 2.4 → 2.5 → 2.7，2.6 挂在 2.3 侧支——这是全保真裁决带来的最小扩张：章 2 从 2 课扩为 5 课，与 ADR 0005 预告的"2.3/2.4 重切扩展"一致。

---

## 引用文件清单（均已逐一打开验证）

- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ServerImpl.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ClientImpl.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/TcpDiscoverySpi.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/TcpDiscoveryMessageSerializer.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/internal/TcpDiscoveryNodesRing.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/internal/TcpDiscoveryNode.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryAbstractMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryJoinRequestMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryNodeAddedMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryNodeAddFinishedMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryNodeLeftMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryNodeFailedMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryMetricsUpdateMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryConnectionCheckMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryStatusCheckMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryDiscardMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryCustomEventMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryServerOnlyCustomEventMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryClientReconnectMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryClientPingRequest.java`、`TcpDiscoveryClientPingResponse.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryClientMetricsUpdateMessage.java`、`TcpDiscoveryClientAckResponse.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryHandshakeRequest.java`、`TcpDiscoveryHandshakeResponse.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryPingRequest.java`、`TcpDiscoveryPingResponse.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryRingLatencyCheckMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryDuplicateIdMessage.java`、`TcpDiscoveryAuthFailedMessage.java`、`TcpDiscoveryCheckFailedMessage.java`、`TcpDiscoveryLoopbackProblemMessage.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryEnsureDelivery.java`（注解存在性与标注类核对）
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ipfinder/TcpDiscoveryIpFinderAdapter.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ipfinder/vm/TcpDiscoveryVmIpFinder.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ipfinder/sharedfs/TcpDiscoverySharedFsIpFinder.java`
- `modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/ipfinder/multicast/TcpDiscoveryMulticastIpFinder.java`
- `modules/core/src/main/java/org/apache/ignite/spi/IgniteSpiAdapter.java`
- `modules/core/src/main/java/org/apache/ignite/configuration/IgniteConfiguration.java`
- `modules/core/src/main/java/org/apache/ignite/internal/util/worker/GridWorker.java`（确认 `updateHeartbeat` 为 worker 心跳、非 ring 心跳）

---

## 对复刻课的启示

1. **复刻的正确切入点是 `RingMessageWorker` 单线程状态机**：2.18 把 metrics 产生、连接检查、失败兜底全部收进这一个线程的 poll 循环（`beforeEachPoll` + `noMessageLoop`），没有定时器线程森林。复刻若照 2.18 结构走，测试可以完全确定性驱动（往队列塞消息即推进时间），这是比"每功能一个线程"的旧版 Ignite 更可教的形态。
2. **环愈合的核心不变量只有三条**：internalOrder 全序且只追加、ensured 消息必达（pending + Discard）、失败判定必须经协调者 verify + internalOrder 匹配。2.5 课的验收应围绕这三条不变量写断言，而不是复刻日志文本。
3. **`previousNodeId` 反查是环愈合最精巧的一步**（A 怀疑 B 前，先让 C 证明 B 确实死了），它把"误判恢复"做进了协议；复刻时值得单独一个测试场景（慢节点不误杀）。
4. **两套序列化并存是历史包袱但必须保真**：directType 二进制族与 JDK 序列化族由一个 mode 字节切换。复刻课 2.4 若想省事合并成一套，将无法通过"消息全集逐类对照"验收——建议保留双轨并把差异显式写进 tracer。
5. **多播 finder 是独立的 UDP 小协议**（4 字节请求 + marshalled 响应），与 ring TCP 协议零耦合，天然适合切成侧支课 2.6；其"shared 靠协议而非存储"的设计（继承 VmIpFinder + setShared(true)）是很好的架构教学点。
6. **IP finder 清理是最终一致性补丁**（ping 注销 + 补注册）：复刻顺序上应放在环协议全部就绪之后（2.7），否则 cleaner 的 ping 依赖未实现，测不出来。
