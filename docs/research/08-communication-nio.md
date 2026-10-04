# 08 · 通信层：TcpCommunicationSpi 与 GridNioServer 全解

> 基于 `vendors/ignite/`（Apache Ignite 2.18.0 官方源码，只读）。本文覆盖点对点通信栈两层：底层通用 NIO 框架 `GridNioServer`（`internal/util/nio/`，下文缩写 `nio/`）与上层专用协议 `TcpCommunicationSpi`（`spi/communication/tcp/`，下文缩写 `tcp/`）。行号以当前 submodule 为准，**文末"引用文件清单"给出全部完整可定位路径**（均已逐一打开验证）。调研中若干"想当然"的类名（ServerSocketClient、TcpCommunicationConnectionReconnect、TrafficTracker、sendReceiveBudget 等）在 2.18 中**不存在**，正文以实际类名纠正并给出证据。

---

## 0. 两层结构与 2.18 重构事实

1. **`GridNioServer<T>` 是与 Ignite 语义无关的通用 TCP NIO 服务器**（`nio/GridNioServer.java` L115）：只认识"selector、session、filter 链、写队列"，不认识 node/cluster。`TcpCommunicationSpi` 是它的一个使用方，通过 `GridNioServerListener`（`nio/GridNioServerListener.java` L27–78）回调收消息。
2. **2.18 的 `TcpCommunicationSpi` 已从历史巨类瘦身为装配器**：本体仅 1193 行（`tcp/TcpCommunicationSpi.java`），逻辑拆入 `tcp/internal/` 27 个类。`spiStart`（L570–800）中按依赖序组装 6 个协作组件：`InboundConnectionHandler`（入站监听/握手，L615）、`TcpHandshakeExecutor`（出站握手 IO，L643）、`GridNioServerWrapper`（持有 NIO server 与三张 recovery 表，L649）、`ConnectionClientPool`（出站客户端池，L686）、`CommunicationWorker`（后台维护线程，L773）、`ConnectGateway`（client 断连闸门）。
3. 关键默认值集中在 `tcp/TcpCommunicationSpi.java` L215–265 与 `tcp/internal/TcpCommunicationConfiguration.java` L68–146：port 47100 / portRange 100、idleConnTimeout 10min、connTimeout 5s、maxConnTimeout 10min、reconCnt 10、selectorsCnt `max(4, cpus/2)`、connPerNode 1、ackSndThreshold 32、sockWriteTimeout 2s、msgQueueLimit 0（不限）、slowClientQueueLimit 0（不踢）、selectorSpins 0（L53）。

---

## 1. GridNioServer 架构（`nio/`）

### 1.1 selector 线程模型

- **三类角色**：1 个 `GridNioAcceptWorker`（线程名 `nio-acceptor[-tcp-comm]`，L398–412）只做 accept；`selectorCnt` 个 `AbstractNioClientWorker`（线程名 `grid-nio-worker-tcp-comm-N`，L419–439）做 read/write/connect 注册；`port == -1` 时不建 accept worker（纯客户端形态，L414–417）。每 worker **一个 selector**（`GridNioWorker` 接口，`nio/GridNioWorker.java` L1–43），accept 与 IO 用不同 selector。
- **分配**：accept 到的 channel 或新建的出站 channel 经 `offerBalanced`（L1086–1129）round-robin 派给 worker。若 `readWriteSelectorsAssign=true`（仅 paired connections 时启用，`tcp/internal/GridNioServerWrapper.java` L956），入站连偶数 worker、出站连奇数 worker（L1095–1111）。
- **再平衡**：accept 线程每轮循环顺带跑 `balancer.run()`（L3141–3142）。balancer 三选一（L446–463）：`ReadWriteSizeBasedBalancer`（L4230–4371，按 worker 周期收发字节差 >90% 阈值挑一条 session `moveSession` 迁移，L4298–4329）、`SizeBasedBalancer`（L4376）、`RandomBalancer`（L4471）。周期由 `IGNITE_IO_BALANCE_PERIOD` 控制默认 5s（L150–151）。

### 1.2 事件循环（`bodyInternal`，L2082–2371）

- 主循环先清空 `changeReqs` 队列（L2092–2260），操作码为内部枚举 `NioOperation`（L3241），语义如下：

| 操作码 | 触发者 | 语义 |
|---|---|---|
| `CONNECT` | `createSession(async=true)` | channel 注册 `OP_CONNECT`，异步出站连接；主数据路径不用它（握手的 connect 是阻塞的），仅 `TcpCommunicationConnectionCheckFuture`（L328）等检查类场景使用 |
| `CANCEL_CONNECT` | `cancelConnect`（L979–996） | 取消未完成的异步连接 |
| `REGISTER` | accept worker 派发 / 阻塞连接完成 | 建 session、注册 `OP_READ`、回调 `onSessionOpened` |
| `MOVE` | balancer | session 迁移到别的 worker（L2134–2185） |
| `REQUIRE_WRITE` | `sendSystem` 带 listener | 确保该 session 挂上 `OP_WRITE` |
| `CLOSE` | `close(ses)` | 关闭 session（future 化） |
| `PAUSE_READ`/`RESUME_READ` | `GridNioMessageTracker` | 摘掉/恢复 `OP_READ`（L2204–2242），入站背压的底层开关 |
| `DUMP_STATS` | 诊断 | 导出 session 统计 |

- 再做 `selectorSpins` 次 `selectNow()` 忙轮询（L2262–2293），然后退化为 `select(2000)`（L2310）——每 2s 醒一次检查关闭与 idle；
- 每 2s 调 `checkIdle(selector.keys())`（L2332–2338）：写挂起且超 `writeTimeout`（默认 5s，L120）触发 `onSessionWriteTimeout`（L2677–2687）；无读写且超 `idleTimeout` 触发 `onSessionIdleTimeout`（L2689–2699）；
- 性能关键点：反射把 selector 的 `selectedKeys/publicSelectedKeys` 换成数组型 `SelectedSelectionKeySet`（L2004–2022，Netty 同款优化），处理循环 L2537–2595。

### 1.3 GridNioSession 生命周期与 meta

- **创建**：accept/出站 connect 完成后进 `register()`（L2712–2814）——directMode 下按 socket 缓冲区大小分配每 session 读写 direct buffer（L2725–2733），构造 `GridSelectorNioSessionImpl`（L2735–2745），从 future 的 meta 拷入 session meta 并挂接 `GridNioRecoveryDescriptor`（L2753–2763，key=`RECOVERY_DESC_META_KEY` L138），注册 `OP_READ`，最后 `filterChain.onSessionOpened(ses)`（L2794）。
- **meta**：本质是 `Object[64]` 数组槽位（`nio/GridNioSessionImpl.java` L251–277），key 由 `GridNioSessionMetaKey.nextUniqueKey()` 全局递增分配，上限 64（`nio/GridNioSessionMetaKey.java` L48、L60–66）。comm SPI 占用 `CONN_IDX_META`/`CONSISTENT_ID_META`（`tcp/TcpCommunicationSpi.java` L242–245）。
- **关闭**：`close(ses, e)`（L2849–2943）幂等（`ses.setClosed()` L2879）；direct buffer 用 `GridUnsafe.cleanDirectBuffer` 释放（L2882–2888）；未发出的写请求逐个 `onError`（L2908–2922）；回调 `onSessionClosed`（L2925）；最后 release 出/入两个 recovery descriptor（L2931–2937）。
- **迁移**：balancer 触发 `moveSession`（L798–836）时 session 先脱离旧 worker、`pendingStateChanges` 暂存未投递请求，再到新 worker 重新注册 key（`nio/GridSelectorNioSessionImpl.java` L275–313）。

### 1.4 filter 链（`GridNioFilterChain`）

- 结构为双向链表：`(TailFilter=listener 代理) → filters[n] → … → filters[0] → HeadFilter=server`，构造顺序即数组顺序（`nio/GridNioFilterChain.java` L55–78）。**读路径**从 HeadFilter 向 Tail 传播 `onMessageReceived`；**写路径**从 Tail 向 Head 传播 `onSessionWrite`，最终 HeadFilter 把消息变成 `SessionWriteRequest` 塞进写队列（`nio/GridNioServer.java` L3757–3787）。
- comm SPI 实际装配的 filters（`tcp/internal/GridNioServerWrapper.java` L910–931，按序）：`GridNioTracerFilter`（仅 tracing 开启）→ `GridNioCodecFilter`（L915）→ `GridConnectionBytesVerifyFilter`（L916）→ `GridNioSslFilter`（可选，L918–931）。
- 逐个职责：
  - `GridNioCodecFilter`（`nio/GridNioCodecFilter.java` L34）：持 `GridNioParser`，出站 `encode`（L84–102），入站循环 `decode` 直到产出对象（L105–134）——comm SPI 的 parser 是 `GridDirectParser`；
  - `GridDirectParser`（`nio/GridDirectParser.java` L37）：读 framing=2 字节 little-endian 消息类型 + 流式字段（**无长度前缀**），半包消息连同 reader 状态存 session meta 续读（L68–116）；
  - `GridConnectionBytesVerifyFilter`（`nio/GridConnectionBytesVerifyFilter.java` L36）：入站 session 前 4 字节必须是 `U.IGNITE_HEADER`（`0x0049474E`，`internal/util/IgniteUtils.java` L353），吞掉 magic 后放行（L86 起）；
  - `GridNioSslFilter`（`nio/ssl/`）：SSL 包装，comm SPI 开 directMode（L926）；
  - `HeadFilter`/`TailFilter`：框架自身的链两端（L3729–3818）。

### 1.5 写请求队列与 backpressure

- 出站消息进入 session 的 `GridConcurrentLinkedDequeue`。普通消息 `offerFuture`（`nio/GridSelectorNioSessionImpl.java` L354–380）：若 `sndQueueLimit>0` 先 acquiring 信号量 `sem`（L140、L357–358）；系统消息（ack 等）`offerSystemFuture` 从**队头**插入且**不占信号量**（L321–342）。
- 入队后 `send0`（`nio/GridNioServer.java` L673–698）CAS `procWrite` 唤醒 worker 开始写；写空队列后 `stopPollingForWrite` 摘掉 `OP_WRITE`（L1134–1145）。
- **豁免机制**：正在处理消息的线程（消息处理线程再转发）不背压——`GridNioBackPressureControl` ThreadLocal 标记（`nio/GridNioBackPressureControl.java` L26–57），防止收发互锁造成死锁。
- **sessionClosed 语义**：close 时 `onClosed()` 一次性 `sem.release(1_000_000)` 放行所有阻塞在生产端的线程（`nio/GridSelectorNioSessionImpl.java` L495–498）；队列残留请求由 L2908–2922 错误完结。
- directMode 写路径 `processWrite0`（L1699–1775）：先把尽可能多的消息序列化进 session 级 write buffer（`writeToBuffer` L1803–1842），再一次 `socket.write`，未写完 compact 后挂 `OP_WRITE` 续写。

### 1.6 buffer 策略：没有池

2.18 **不存在** BufferPool/DirectPoolWorker 类 DirectBuffer 池。实际策略：directMode 下每 session 一对按 socket 缓冲区大小的 direct buffer（L2725–2733）；非 directMode 的 worker 级 8KB 读缓冲（L1176）；close 时 `GridUnsafe.cleanDirectBuffer` 直接释放（L2882–2888）。worker 有两个子类：`ByteBufferNioClientWorker`（L1155，REST 等用）与 `DirectNioClientWorker`（L1331，comm/thin-client 用，绕中间 ByteBuffer 直接序列化进 socket 缓冲）。

### 1.7 谁在复用 GridNioServer（grep 全部构造点）

| 使用方 | 位置 | 模式 |
|---|---|---|
| TcpCommunicationSpi（经 `GridNioServerWrapper`） | `tcp/internal/GridNioServerWrapper.java` L934–964 | directMode=true，LITTLE_ENDIAN |
| thin client / ODBC 服务端 `ClientListenerProcessor` | `internal/processors/odbc/ClientListenerProcessor.java` L191–209 | directMode=true，nativeOrder |
| REST TCP `GridTcpRestProtocol` | `internal/processors/rest/protocols/tcp/GridTcpRestProtocol.java` L235–252 | directMode=false，nativeOrder |
| `SocketStreamer` | `stream/socket/SocketStreamer.java` L186 | 裸 byte[] |
| thin client 端多路复用器（客户端角色） | `internal/client/thin/io/gridnioserver/GridNioClientConnectionMultiplexer.java` | port=-1 纯客户端 |

结论：复刻 `GridNioServer` + filter 链是**一次投入四处收益**的基础设施课。

---

## 2. TcpCommunicationSpi（`tcp/`）

### 2.0 协作组件职责表（类结构图）

`spiStart`（L570–800）装配的六个组件与它们之间的引用关系（→ 表示"持有/调用"）：

| 组件 | 职责 | 关键引用 |
|---|---|---|
| `InboundConnectionHandler`（extends `GridNioServerListenerAdapter<Message>`，`tcp/internal/InboundConnectionHandler.java` L77） | 作为 NIO server 的 listener：accept 后发身份声明、处理首条握手、收发 ack、消息上送 `GridIoManager`、断连善后 | → clientPool、commWorker、nioSrvWrapper |
| `GridNioServerWrapper` | 持有 `GridNioServer` 与三张 recovery 表；`resetNioServer` 建服（端口扫描）；`createNioSession` 出站建连全流程；slow-client 检查 | → nioSrv、recoveryDescs/out/in |
| `ConnectionClientPool` | 每节点 `GridCommunicationClient[]` 池；`reserveClient` 并发去重；节点离开清理；inverse 连接等待 | → nioSrvWrapper |
| `TcpHandshakeExecutor` | 出站握手的阻塞 IO（明文 `TcpTransport` / SSL `SslTransport`） | 独立 |
| `CommunicationWorker`（后台线程） | 空闲连接关闭、超时 ack 兜底、断线重连请求、NodeLeft 清理 | → clientPool、nioSrvWrapper |
| `ConnectGateway`（`tcp/internal/ConnectGateway.java` L29） | client 断连/重连闸门：断连期间 `enter()` 直接抛错阻止建连（L42 起） | 被各组件持有 |

### 2.1 发送路径与建连触发

- `sendMessage0`（`tcp/TcpCommunicationSpi.java` L1013–1079）：本机 nodeId 直接回调 listener（L1034–1035）；否则按消息选 `connIdx`（`TcpConnectionIndexAwareMessage` 可指定，否则 `connPlc.connectionIndex()`，L1039–1049）→ `clientPool.reserveClient(node, connIdx)`（L1055）→ `client.sendMessage`。`GridTcpNioCommunicationClient.sendMessage` 遇 IOException 关 session 并返回 `true`（`nio/GridTcpNioCommunicationClient.java` L107–129），外层 `do-while(retry)` 换掉坏 client 重试一次（L1052–1078）——**这就是断线时的同步重建路径**。
- `ConnectionClientPool.reserveClient`（`tcp/internal/ConnectionClientPool.java` L269–433）：每节点一个 `GridCommunicationClient[connPerNode]` 数组（L113）；槽位为空时用 `clientFuts`（key=`ConnectionKey`）+ `ConnectFuture` 去重并发建连（L304–369），等待者分片续期心跳（L377–400）。

### 2.2 谁主动连谁、地址从哪来

- **默认双方都可能主动**：任一端 sendMessage 缺 client 即主动出连。冲突消解在**入站端**：若本端正在向对方建连（存在 `ConnectFuture`）且 `locNode.order() < rmtNode.order()`，则拒绝对方入站、回 `ALREADY_CONNECTED`（`tcp/internal/InboundConnectionHandler.java` L631–640）——**order 小者（老节点）的出站连接胜出**，避免双连。另有"已有同 idx 的 TCP client"也直接拒绝（L560–578）。`connectCount` 更大者会 `closeStaleConnections` 清掉旧连接（L660–670）。
- **地址来源=discovery 节点属性**：`CommunicationTcpUtils.nodeAddresses`（L81 起）读 `comm.tcp.addrs/host.names/port/ext-addrs` 属性（属性名 `tcp/TcpCommunicationSpi.java` L200–212），本机地址跳过（`GridNioServerWrapper.java` L413–422）。
- **出站连接流程** `GridNioServerWrapper.createNioSession`（L364–677）：逐地址尝试；超时预算 `ExponentialBackoffTimeoutStrategy`（5s 起、指数退避到 maxConnTimeout、共 reconCnt 次，L383–407）；**阻塞式** `ch.socket().connect(addr, timeout)`（L475）→ 先 `recoveryDesc.reserve()` 独占建连权（L452–464）→ `safeTcpHandshake` 在阻塞通道上完成握手（L498–505）→ 成功后才 `nioSrv.createSession(ch, meta, false, null)` 把 channel 移交 NIO server 注册 selector（L538）。握手结果负数即错误码（L507–530）。
- **inverse connection（反向连接）**：server 节点连不上 client 节点（防火墙）时抛 `NodeUnreachableException`（L368–374、L662–670），`ConnectionClientPool.handleUnreachableNodeException`（L468–534）经 `ConnectionRequestor` 发**discovery 自定义消息** `TcpConnectionRequestDiscoveryMessage`（`tcp/internal/TcpConnectionRequestDiscoveryMessage.java` L38–54）请求对方反连；实现方是 `GridIoManager.TcpCommunicationInverseConnectionHandler`（`internal/managers/communication/GridIoManager.java` L4340 起，注入点 L469）。

### 2.3 connectionIndex 语义与连接数

- `ConnectionKey = (nodeId, idx, connCnt)`（`tcp/internal/ConnectionKey.java` L55–71），`connCnt` 仅入站连接携带（握手里的 connectCount），`dummy` 标记用于连通性检查的临时连接（L97）。
- `connectionsPerNode` 默认 1 → `FirstConnectionPolicy` 恒返 0（L23–27）；>1 → `RoundRobinConnectionPolicy` 原子递增取模（L25–45，上限 `MAX_CONN_PER_NODE=1024`，常量在 `GridNioServerWrapper.java` L144，上限校验在 `tcp/internal/TcpCommunicationConfigInitializer.java` L798–800）。消息可经 `TcpConnectionIndexAwareMessage` 自带 idx 实现定向绑定（`tcp/internal/TcpConnectionIndexAwareMessage.java`）。
- **paired connections**（`usePairedConnections`，默认 false）：每个 `(node, idx)` 拆成独立 in/out 两条连接、三张 recovery 表分立（`recoveryDescs/outRecDescs/inRecDescs`，L192–198），selector 侧偶读奇写（L956）。**channel 连接**（utility cache 直通道）复用同一 server，idx 从 1025 起（L295、`isChannelConnIdx` L1167）。

### 2.4 重连、关闭与"心跳"

- **没有周期性心跳消息**。存活检测三层：(1) socket 层 `setKeepAlive(true)`（accept 侧 `nio/GridNioServer.java` L3211，connect 侧 `GridNioServerWrapper.java` L437）；(2) `GridNioServer.checkIdle` 按 idleTimeout/writeTimeout 关静死连接（L2663–2705）；(3) 发送失败即重连（2.1 retry 循环）。
- **异步重连**：session 断开时 `onDisconnected`（`InboundConnectionHandler.java` L398–452）移除对应 client；若**对端仍在拓扑且本地有未确认消息**，投 `DisconnectedSessionInfo` 给 `CommunicationWorker`（L425–441），其 `processDisconnect`（`tcp/internal/CommunicationWorker.java` L364–414）调用 `reserveClient` 重建，失败但 ping 通则重新入队重试（L397）。
- **空闲关闭**：`CommunicationWorker` 以 `idleConnectionTimeout` 为周期轮询（L167），`processIdle`（L205–293）关闭无未确认消息且静默超时的 client（L257–280）；同函数还兜底补发 ack（L237–254、L299–318）并清理已离开节点的 recovery 描述符（L323–359）。
- **连接检查工具**：`checkConnection` API 建临时 dummy 连接（`tcp/TcpCommunicationSpi.java` L972–987；`tcp/internal/TcpCommunicationConnectionCheckFuture.java`；dummy 分支处理 `InboundConnectionHandler.java` L349–361）。

---

## 3. 通信协议（wire format）

### 3.1 协议消息清单（type code 定义于 `tcp/TcpCommunicationSpi.java` L268–277）

| 消息类 | type | 尺寸 | 字段 | 方向/用途 |
|---|---|---|---|---|
| `NodeIdMessage`（`tcp/messages/NodeIdMessage.java` L29–34、L69） | `-1` | 2+17B | nodeId | **服务端 accept 后立即主动发**，声明自己身份 |
| `RecoveryLastReceivedMessage`（`tcp/messages/RecoveryLastReceivedMessage.java` L31–50、L85） | `-2` | 2+≤10B | rcvCnt(long) | 双用：**ack**（rcvCnt=已收条数）与**握手应答/错误码**（`ALREADY_CONNECTED=-1` L31、`NODE_STOPPING=-2` L34、`NEED_WAIT=-3` L37、`UNKNOWN_NODE=-4` L40） |
| `HandshakeMessage`（`tcp/messages/HandshakeMessage.java` L29–34、L37–50、L139） | `-3` | 2+37B | nodeId、connectCnt、rcvCnt、connIdx | **客户端 connect 后发**的握手请求 |
| `HandshakeWaitMessage`（`tcp/messages/HandshakeWaitMessage.java` L28–30、L40） | `-28` | 2B（纯类型码） | 无 | 服务端 SPI 上下文未就绪时代替 NodeIdMessage，客户端等 200ms 重试（`NEED_WAIT` 路径，`GridNioServerWrapper.java` L516–527、L132） |
| 业务消息（`GridIoMessage` 等） | 正数（direct marshaller 注册表） | 流式 | — | 走同一 `GridDirectParser` framing，2 字节 LE 类型 + 字段流（`nio/GridDirectParser.java` L78–93）；type 由两字节 little-endian 合成（`makeMessageType`，`tcp/TcpCommunicationSpi.java` L1183–1185） |

### 3.2 建连时序（connect → 握手 → 可用）

```
A(出站端)                                B(入站端)
  ch.socket().connect(addr, timeout)  ──TCP SYN──▶  nio-acceptor accept(L3202-3222)
                                                        └ offerBalanced 派给 worker
  ◀──── NodeIdMessage(-1, B.nodeId) ────  onConnected: sendNoFuture(stateProvider.nodeIdMessage())
                                          (未就绪则 HandshakeWaitMessage(-28))
  IGNITE_HEADER(4B) + HandshakeMessage(-3,   ──▶     onMessage → connKey==null → onFirstMessage
    {A.nodeId, connectCnt, rcvCnt=A已收数,       校验 A 在拓扑(L486-516) / meta(CONN_IDX/CONSISTENT_ID)
    connIdx})                                    tryReserve(connectCnt) 入站 recovery 描述符
  ◀── RecoveryLastReceivedMessage(-2, B.rcvCnt)  connected(): onHandshake→resend→ack→addNodeClient
  nioSrv.createSession(ch) 注册 OP_READ
  recoveryDesc.onHandshake(B.rcvCnt) 计算需重发的未确认消息 → resend(ses)
```

出站端握手 IO 在**阻塞通道**上由 `TcpHandshakeExecutor`（`tcp/internal/TcpHandshakeExecutor.java` L83–121）完成：`receiveNodeId`（L149–180，识别 -28/-1）→ `sendHandshake`（L188–200，手写 4B magic + 消息）→ `receiveAcknowledge`（L207–255，逐字节读变长 long）；SSL 有平行实现 `SslTransport`（L319 起）。超时保护 `HandshakeTimeoutObject` 定时关 socket（`GridNioServerWrapper.java` L1211–1229）。

### 3.3 ack 语义

- **发 ack 的时机**（接收方 `InboundConnectionHandler.onMessage` L331–348）：每收 `ackSndThreshold`（默认 32）条消息，把当前 `rcvCnt` 用**系统消息**（队头、不占信号量）发回；`CommunicationWorker` 周期兜底补发（`CommunicationWorker.java` L237–254）。ack 自身 `skipRecovery`（`GridNioServerWrapper.java` L901），不进恢复队列。
- **收 ack 的效果**（发送方 L312–330 → `GridNioRecoveryDescriptor.ackReceived`，`nio/GridNioRecoveryDescriptor.java` L214–231）：按 `rcvCnt` 从 `msgReqs` 出队释放 future、触发 ackClosure。
- **恢复状态机**（`nio/GridNioRecoveryDescriptor.java`）：每 `(node, connIdx)` 一个描述符；`add` 把已发消息存入未确认队列、超 `queueLimit` 返回 false 触发关连重连（L193–209；溢出检测在 `GridSelectorNioSessionImpl.pollFuture` L412–426）；`onHandshake(rcvCnt)` 对端声明已收数并算出 `resendCnt`（L315–322）；`reserve/tryReserve/onConnected/release` 管理建连独占与握手排队（L286–310、L417–458、L327–346、L378–410）。`queueLimit` 默认 `max(msgQueueLimit, ackSndThreshold)*128`（`GridNioServerWrapper.java` L1035–1037）。
- 重连后 `GridNioServer.resend(ses)`（`nio/GridNioServer.java` L749–777）把未确认消息重灌写队列——**at-least-once 语义**，上层 `GridIoManager` 靠消息幂等性兜底。

### 3.4 首条身份声明

服务端 `onConnected` 对 accepted session **不等对方开口**先发 `NodeIdMessage`（L216–240），这是"首条身份声明"；客户端以之为握手第一步。若服务端 SPI 上下文未初始化（`ctxInitLatch`）则发 `HandshakeWaitMessage`（L230–235），配合 writerFactory 的特例序列化（`GridNioServerWrapper.java` L834–836、L880–881）实现"启动窗口期不拒绝连接"。

### 3.5 握手完成后业务消息如何上送

握手成功的 session 其 meta 里已有 `CONN_IDX_META`，此后 `onMessage`（`InboundConnectionHandler.java` L257）不再走握手分支：ack 消息（`RecoveryLastReceivedMessage`）交给出站 recovery 描述符（L312–330）；其余消息计入入站 recovery 并按阈值回 ack（L331–348），再经 `GridNioMessageTracker` 包装一个"处理完成"回调（L368–383），最终 `lsnr.onMessage(connKey.nodeId(), msg, c)` 上送 `GridIoManager`（L385；listener 桥接见 `tcp/TcpCommunicationSpi.java` L631–640/L666–680）。发送侧对称：`GridIoManager.sendToGridTopic` → SPI `sendMessage` → client → `session.sendNoFuture` → filter 链 → 写队列。因此**通信课的验收边界**可以定在"两个节点经真实 socket 互发一条 `GridIoMessage` 并在对面 listener 收到"。

---

## 4. 带宽限流与慢节点（纠正一个常见误解）

**2.18 的 GridNioServer/TcpCommunicationSpi 中不存在 TrafficTracker、sendReceiveBudget、reserveBudget 之类的字节级带宽预算机制**（对 `internal/util/nio/` 与 `spi/communication/tcp/` 全文 grep `budget|bandwidth|TrafficTracker` 零命中）。实际是**四层消息级限流**：

1. **出站背压**：per-session `sndQueueLimit`（= `messageQueueLimit`，默认 0 不限）信号量，生产线程满时阻塞（`nio/GridSelectorNioSessionImpl.java` L140、L354–380）；`msgQueueLsnr` 回调每次入队上报队列长度（`nio/GridNioServer.java` L696–697）。
2. **未确认上限**：recovery 队列 `queueLimit`，溢出关连重连（见 3.3）。
3. **入站背压**：`messageQueueLimit>0` 时接收方挂 `GridNioMessageTracker`（`InboundConnectionHandler.java` L368–383），未处理消息数达阈值即 `ses.pauseReads()` 暂停 `OP_READ`、处理完 `resumeReads()`（`nio/GridNioMessageTracker.java` L58–93、L104–139）——**收不动就让 TCP 窗口收缩**，是真正的端到端流控。
4. **慢 client 处理**：server 节点对 client 节点方向队列超过 `slowClientQueueLimit` 时 `failNode` 把 client 踢出集群（`GridNioServerWrapper.checkClientQueueSize` L1251–1272；`checkNodeQueueSize` L1280–1297 另做 `msgQueueWarningSize` 警告）。监听器装配于 L905–908。

汇总（配置项 → 作用层 → 超限行为）：

| 配置项（默认） | 作用层 | 超限行为 |
|---|---|---|
| `messageQueueLimit`（0=不限） | 出站 per-session 信号量 + 入站 tracker | 出站：生产线程阻塞；入站：pause reads |
| `unackedMsgsBufferSize`（0→自动 `max(msgQueueLimit,ackSndThreshold)*128`） | recovery 未确认队列 | 关闭 session、按未确认消息重连重发 |
| `ackSndThreshold`（32） | 接收方 ack 频率 | 无失败语义，仅影响 ack 流量 |
| `slowClientQueueLimit`（0=不踢） | server→client 方向队列 | `failNode` 踢除 client 节点 |
| `msgQueueWarningSize`（`IGNITE_TCP_COMM_MSG_QUEUE_WARN_SIZE`，0=关） | 所有出站队列 | 仅日志告警（30s 限频） |

---

## 5. 切课建议："通信层全保真"课弧

依据源码实际分层（通用 NIO 框架 → SPI 装配 → 线上协议 → 恢复语义 → 稳态治理），建议 **7 课主弧 + 1 课选做**：

| # | 课题 | 概念簇（单一新概念） | Tracer（验收实验） | 依赖 | 量级 |
|---|---|---|---|---|---|
| C1 | NIO 服务器骨架 | selector 线程模型（accept worker + N 个 IO worker、offerBalanced、changeReqs、select(2000)/checkIdle）、session 创建/注册/关闭 | 两个节点互发 echo 消息；jstack 观察 `nio-acceptor`/`grid-nio-worker` 线程；杀线程验证 onFailure | 无 | 大 |
| C2 | filter 链与消息编解码 | `GridNioFilterChain` 双向链、`GridNioCodecFilter`+`GridDirectParser` 2B-LE 类型流式 framing、meta 槽位 | 自定义 3 个消息类走通 encode/decode 半包续读 | C1 | 中 |
| C3 | 写队列与背压 | offerFuture/信号量、system 消息队头、procWrite、`GridNioBackPressureControl` 豁免、pause/resume reads | 慢消费者实验：入站暂停后出站队列封顶阻塞 | C2 | 中 |
| C4 | SPI 装配与出站连接 | `TcpCommunicationSpi.spiStart` 组件拼装、`ConnectionClientPool.reserveClient` 并发去重、`createNioSession` 阻塞 connect+退避、`ConnectionKey`/connIdx/policy | `IgniteConfiguration.setCommunicationSpi` 后两节点真实互发 `GridIoMessage` | C2 | 大 |
| C5 | 握手协议 | `NodeIdMessage/HandshakeMessage/HandshakeWaitMessage/RecoveryLastReceivedMessage` 四消息、`TcpHandshakeExecutor` 阻塞 IO、`onFirstMessage`、order() 双连消解、ALREADY_CONNECTED/NEED_WAIT 错误码 | 抓包/单测验证时序与错误码分支；并发双连只留一条 | C4 | 中 |
| C6 | 恢复与 ack | `GridNioRecoveryDescriptor` 全状态机（reserve/onHandshake/ackReceived/release）、ackSndThreshold=32、queueLimit 溢出、resend at-least-once | 断网注入：杀连接后未确认消息自动重发且不丢不重（配合幂等业务消息） | C5 | 大 |
| C7 | 稳态治理 | `CommunicationWorker`（idle 关闭、超时 ack、断线重连退避、NodeLeft 清理）、slowClientQueueLimit 踢除、msgQueue 警告、inverse connection | 10 分钟空闲自动关连；慢 client 被踢；client 防火墙场景反连成功 | C6 | 中 |
| C8（选做） | paired connections 与 channel | in/out 分连三表、偶读奇写 selector 分配、`Channel` 直通道（idx>1024） | paired 模式双连接各自 ack | C6 | 小 |

**可并入相邻课的小件**（全保真但实现量小）：`HandshakeWaitMessage` 及启动窗口特例序列化并入 C5；`SizeBasedBalancer/moveSession` 并入 C1（先只做 round-robin，迁移作为 C1 加分项）；`ConnectGateway`（client 断连闸门，`tcp/internal/ConnectGateway.java` L29）与 discovery 事件监听并入 C4；`checkConnection`/dummy 连接并入 C7；SSL filter 与 tracer filter 建议留同构空壳（接口在 C2 定义），不单独设课。**不建议裁剪**：ack/重发（C6）与背压（C3）是"全保真"决策的核心承诺，也是 Ignite 无中心协调下消息不丢的根基。

课间依赖图（箭头=前置）：

```
C1 NIO骨架 ──▶ C2 filter/codec ──▶ C3 写队列/背压
                   │
                   └──▶ C4 SPI装配/出站连接 ──▶ C5 握手协议 ──▶ C6 恢复/ack ──▶ C7 稳态治理
                                              (C8 paired/channel ◀─ 依赖 C6，选做)
```

---

## 引用文件清单

以下路径均相对 `vendors/ignite/modules/core/src/main/java/`（除特别注明），全部实际打开核验：

**NIO 框架（org/apache/ignite/internal/util/nio/）**
- `GridNioServer.java`（类 L115；常量 L120–151；字段 L184–302；构造 L332–485；start/stop L520–549；close L578–591；send0 L673–698；sendSystem L707–744；resend L749–777；createSession L941–973；offerBalanced L1086–1129；ByteBufferNioClientWorker L1155–1326（processRead L1187、processWrite L1255）；DirectNioClientWorker L1331–1848（processRead L1356、processWrite0 L1699、writeToBuffer L1803）；AbstractNioClientWorker L1878–3016（changeReqs L1881、SelectedSelectionKeySet L2004、bodyInternal L2082、select(2000) L2310、checkIdle 调用 L2332、processSelectedKeysOptimized L2537、checkIdle L2663、register L2712、closeKey L2819、close L2849、processConnect L2949）；GridNioAcceptWorker L3033–3235（accept L3124、keepAlive L3211）；HeadFilter L3729–3818；Builder/build L3824/3915；ReadWriteSizeBasedBalancer L4230；SizeBasedBalancer L4376；RandomBalancer L4471）
- `GridSelectorNioSessionImpl.java`（构造+信号量 L114–165；offerSystemFuture L321；offerFuture L354；pollFuture 溢出 L402–430；onClosed L495；systemMessage L501）
- `GridNioSessionImpl.java`（send/sendNoFuture L115–139；pauseReads L154；close L167；meta L251–277；accepted L280）
- `GridNioSessionMetaKey.java`（MAX_KEYS_CNT L48；nextUniqueKey L60）
- `GridNioFilterChain.java`（链构造 L55–78）
- `GridNioFilter.java`（L34–258 接口全貌）
- `GridNioCodecFilter.java`（L34；onSessionWrite L84；onMessageReceived L105）
- `GridDirectParser.java`（L37；decode L68；encode L119）
- `GridConnectionBytesVerifyFilter.java`（L36；onMessageReceived L86）
- `GridNioBackPressureControl.java`（L26–57）
- `GridNioMessageTracker.java`（run L58；onMessageReceived L104）
- `GridNioRecoveryDescriptor.java`（构造 L105；onReceived L148；add L193；ackReceived L214；onNodeLeft L245；reserve L286；onHandshake L315；onConnected L327；release L378；tryReserve L417）
- `GridTcpNioCommunicationClient.java`（L38；sendMessage L107；async L132；getIdleTime L137）
- `GridNioWorker.java`（L1–43）；`GridNioServerListener.java`（L27–78）

**SPI（org/apache/ignite/spi/communication/tcp/）**
- `TcpCommunicationSpi.java`（类 L198；属性名 L200–212；默认值 L215–265；消息类型码 L268–277；spiStart L570–800（组件装配 L610–711）；spiStop L850；sendMessage0 L1013–1079；checkConnection L972；makeMessageType L1183）
- `internal/GridNioServerWrapper.java`（常量 L132–144；三张 recovery 表 L192–198；msgQueueWarningSize L216；chIdx L295；createNioSession L364–677；createTcpClient L698；inRecoveryDescriptor L732；resetNioServer L805–1001（msgFactory L817、parser L897、skipRecoveryPred L901、queueSizeMonitor L905、filters L910、builder L934–964）；outRecoveryDescriptor L1008；recoveryDescriptor/queueLimit L1022–1037；isChannelConnIdx L1167；safeTcpHandshake L1211；checkClientQueueSize L1251；checkNodeQueueSize L1280）
- `internal/InboundConnectionHandler.java`（类 L77；onSessionWriteTimeout L202；onConnected L216；onMessage L257；onDisconnected L398；onFirstMessage L465（order 比较 L631、closeStaleConnections L660）；connected L681；connectedNew L726）
- `internal/ConnectionClientPool.java`（clients L113；reserveClient L269；handleUnreachableNodeException L468；createCommunicationClient L542）
- `internal/CommunicationWorker.java`（类 L57；body L154；processIdle L205；sendAckOnTimeout L299；cleanupRecovery L323；processDisconnect L364）
- `internal/TcpHandshakeExecutor.java`（L50；tcpHandshake L83；receiveNodeId L149；sendHandshake L188；receiveAcknowledge L207；TcpTransport L286；SslTransport L319）
- `internal/CommunicationTcpUtils.java`（nodeAddresses L81）
- `internal/ConnectionKey.java`（L55–71、dummy L97）
- `internal/FirstConnectionPolicy.java`（L23–27）；`internal/RoundRobinConnectionPolicy.java`（L25–45）
- `internal/ConnectGateway.java`（L29、enter L42）
- `internal/TcpConnectionRequestDiscoveryMessage.java`（L38–54）
- `internal/TcpCommunicationConfiguration.java`（默认值 L68–146；DFLT_SELECTOR_SPINS L53）
- `internal/TcpCommunicationConfigInitializer.java`（参数校验 L798–810）
- `messages/HandshakeMessage.java`（L29–34、L37–50、L139）
- `messages/NodeIdMessage.java`（L29–34、L69）
- `messages/RecoveryLastReceivedMessage.java`（错误码 L31–40、L47–50、L85）
- `messages/HandshakeWaitMessage.java`（L28–30、L40）

**复用方与其他**
- `org/apache/ignite/internal/processors/odbc/ClientListenerProcessor.java`（builder L191–209）
- `org/apache/ignite/internal/processors/rest/protocols/tcp/GridTcpRestProtocol.java`（builder L235–252）
- `org/apache/ignite/stream/socket/SocketStreamer.java`（L186）
- `org/apache/ignite/internal/client/thin/io/gridnioserver/GridNioClientConnectionMultiplexer.java`（客户端复用）
- `org/apache/ignite/internal/managers/communication/GridIoManager.java`（inverse 注入 L469；handler L4340）
- `org/apache/ignite/internal/util/IgniteUtils.java`（IGNITE_HEADER L353）

---

## 对复刻课的启示

1. **先框架后协议**：`GridNioServer` 对集群概念零依赖（C1–C3 可独立于 discovery 演进），这是天然的课程切割线，也让 C2 的 filter 抽象能同时服务后续 thin-client/REST 章节的复用讲解。
2. **握手发生在 NIO 之外**：出站连接先在阻塞 socket 上完成"身份声明→握手→ack"三轮，再把 channel 移交 selector（`GridNioServerWrapper.java` L475–538）。复刻时若把握手塞进 NIO 事件循环会与官方结构背离，且并发复杂度陡增。
3. **可靠性的核心是一个 64 字段都不到的状态机**：`GridNioRecoveryDescriptor` 的 rcvCnt/acked/resendCnt 三个计数器 + msgReqs 队列撑起了 at-least-once。C6 应以此类为唯一主角，其余代码都是它的搬运工。
4. **"全保真"的真正难点不是代码量而是并发语义**：信号量豁免（`GridNioBackPressureControl`）、descriptor reserve 握手排队、order() 双连消解这三处最容易被"简化"掉并引入死锁/双连 bug——切课时把它们分别钉在 C3、C6、C5 的验收 tracer 里。
5. **课弧总估**：C1–C7 主弧约 7 课 × 2–4h，代码量约 6–7k 行（含消息类与测试），是整个课程中仅次于持久化的大章；C8 与 SSL 空壳可作为弹性缓冲调节节奏。
6. **现成的 tracer 基础设施**：两层都内置了观测指标——`GridNioServer` 的 `outboundMessagesQueueSize/sentBytes/receivedBytes/ActiveSessionsCount`（`nio/GridNioServer.java` L154–182、L467–484），SPI 侧的 `sentMessagesCount/receivedMessagesByType` 等（`tcp/TcpCommunicationSpi.java` L283–319），以及 `dumpStats`/`dumpNodeStatistics` 诊断（L440–491）。复刻课的 tracer 可直接以"指标对齐官方命名"为验收项，省去自造观测面。
