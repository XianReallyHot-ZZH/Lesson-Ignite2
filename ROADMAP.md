# Lesson-Ignite2 路线图 —— 以课程形式从零复刻 Apache Ignite 2.18.0

> 本文档是项目的操作手册：总路线、课程划分原则、骨架草稿、待钉死的决策、进度追踪。
> 它由 2026-10-03 的 ask-matt 路由分析产生，后续各阶段（grill / wayfinder / spec / tickets）的产出会回填并细化它。
> 域词汇表见 [CONTEXT.md](CONTEXT.md)，已定架构决策见 [docs/adr/](docs/adr/)，调研结论见 [docs/research/](docs/research/)（已验收的引用可直接信任）。

## 0. 项目定义

- **目标**：从零复刻 Apache Ignite 2.18.0。以 lesson 教学项目的形式推进——每节课复刻一个功能切片，走完全部课程后自然得到完整的 Ignite 等价物。
- **硬约束**：课程划分必须服从学习者的认知负担与学习曲线。
- **`vendors/ignite/`**（git submodule）承载 Ignite 官方源码，是 **primary source（只读参照物）**，不是要构建的代码库本体。复刻代码写在仓库其他位置（具体布局是待决策项，见 §5）。
- **合规**：Apache 2.0——保留 `LICENSE`/`NOTICE` attribution；借用或改写 Ignite 的测试同样注明来源。

## 1. 现状实测（2026-10-03）

| 模块 | 主代码 Java 文件 | 行数 |
|---|---|---|
| `modules/core` | 4,241 | ~902,583 |
| `modules/indexing` | 218 | ~50,808 |

2.18 采用拆分后的模块布局，共 **39 个模块**（`core` / `binary` / `commons` / `unsafe` / `compress` / `calcite` / `clients` / `platforms` / ……）。
**结论**：规模远超单 session 容量（smart zone），路径当前不可见 → 必须走 wayfinder 型路线。

## 2. 总路线（技能流）

所有命令由用户显式调用（plugin skill 无法被 agent 代为触发）。

| 阶段 | 命令 | 产出 |
|---|---|---|
| **0. 前置** | `/mattpocock-skills:setup-matt-pocock-skills` | issue tracker、triage 标签、文档布局 |
| **A. 情报** | `/research` × 4（并行后台，见 §6） | 带引用的 Markdown 落盘：启动路径、模块依赖图、数据面主路径、持久化+SQL |
| **B. 定向** | `/grill-with-docs` → `/wayfinder` → `/to-spec` → `/to-tickets` | `CONTEXT.md` + ADR → 课程决策地图 → 课程 spec → **每课一张 tracer-bullet ticket** |
| **C. 实施** | 每课 `/implement`，课间 `/clear` | 每课内部驱动 `/tdd`（红-绿切片）+ `/code-review` 后提交 |

### 上下文纪律

- **Phase B 四步在同一不间断上下文窗口完成**——到 `/to-tickets` 为止不 clear；若逼近 smart zone，在最近的相位边界 `/compact` 后继续。
- 重读 `vendors/ignite` 源码永远交给 `/research` 后台 agent（产出文件而非上下文），主窗口只装结论。
- 每张课程 ticket 的 `/implement` 从全新窗口开始，ticket 自足。

### 按需技能

| 场景 | 技能 |
|---|---|
| 设计问题需要跑代码验证（原型） | `/prototype`（经 `/handoff` 进出） |
| Ignite 术语过载、沉淀词汇表 | `/domain-modeling` |
| 设计某模块的形状（接口/深度/接缝） | `/codebase-design` |
| 先学会某概念再动手实现 | `/teach` |
| 课中难缠 bug | `/diagnosing-bugs` |
| 课间保养代码库 | `/improve-codebase-architecture` |
| 合并冲突 | `/resolving-merge-conflicts` |
| 某条消息没听懂 | `/wait-what` |

## 3. 课程划分原则（认知负担）

1. **第 1 课就有可见完成**（tracer bullet）：课程开篇产出一个能 `Ignition.start()` 起来、能 put/get 的单机节点，先打通最薄的端到端，再逐课加深。
2. **每课只引入一个新概念簇**：复用之前全部概念，新概念一次一撮。**切课时认知负担优先于进度效率，学习曲线必须平滑——宁可多切课，不可一课塞两个概念簇**（2026-10-03 grill 用户强调）。
3. **螺旋式重访**：同一对象以递增深度重现——cache 依次经历 本地并发 → 分布式分区 → 事务 → 持久化 → SQL 索引。
4. **每课验收 = 测试全绿 + 与真 Ignite 行为对比**：尽量改写 Ignite 自己的（Apache 2.0）测试作为验收标准。

## 4. 课程地图 v1（wayfinder 定稿，2026-10-04）

**完整课表（97 主课 + 1 选做）见 [docs/course-map-v1.md](docs/course-map-v1.md)**——总课表、章间依赖 DAG、kernal 终态对齐核对（C-11）、附录（外围接缝/深挖 calcite/永久排除/可简化沉淀）。切课详情以 wayfinder 地图各决策票的 resolution 为准（[issue #1](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/1) 及其 16 张子票）。

| 章 | 主题 | 课数 | 章 tracer 里程碑 |
|---|---|---|---|
| 0 | 构建骨架与 Ignition 生命周期 | 4 | `Ignition.start()` + JdkMarshaller 往返 |
| 1 | 单节点缓存（退化形态 + 存储接缝） | 4 | 单机 put/get/scan |
| 2 | SPI + Discovery（ring 全保真 + 多播） | 7 | 2 节点互发现；愈合三不变量 |
| 3 | 通信（NIO 全保真 + binary v1） | 10+1 选 | 跨节点消息往返；任意对象过线 |
| 4 | Affinity 与 rebalance | 8 | 分区正确落位含备份 |
| 5 | near cache | 2 | 零额外消息的捎带更新 |
| 6 | 事务（3 隔离×2 并发 + 恢复 + store） | 8 | 悲观 2PC 提交/回滚；kill 共识 |
| 7 | 持久化（含 compaction/增量快照/historical 补课） | 12 | 重启数据存活（6 kill 注入矩阵） |
| 8 | H2 SQL 课弧（DML/DDL/join/治理+Lucene） | 8 | 跨节点 SELECT/UPDATE |
| 9 | 事件与消息（fog 毕业弧） | 4 | kill primary：CQ 不丢不重 |
| 10 | calcite 课弧（库为黑盒，灰区 9 条全保真化） | 6 | 双引擎一致 + hint 切换 |
| 11 | 计算与服务（compute/services/streamer/datastructures） | 12 | MR 作业；kill worker failover |
| 12 | 部署子系统（双 store/映射协议/UriDeployment） | 5 | p2p 派单类到达远端 |
| 13 | 客户端形态与收尾（thick/thin/binary 完整化） | 7 | 官方 thin client 连复刻节点 |

（编号重排：事件弧插为章 9，原 9/10/11/12 顺移为 10/11/12/13。）

## 5. 范围决策（已全部钉死，2026-10-03 grill，16 问；详见 docs/adr/）

| # | 决策 | 结论 | ADR |
|---|---|---|---|
| 1 | 复刻范围 | 内核闭包（5 模块）+ indexing（H2）+ calcite 集成层 + 部署子系统；platforms/外围显式排除（附录记接缝）；MVCC 不存在于 2.18（源码零 `MvccMode`） | 0001 |
| 2 | 兼容等级 | 包名 API 级 + 测试级准绳（改写自 Ignite 的测试子集可编译运行） | 0001 |
| 3 | 学习者 | 中高级 Java 工程师（用户本人 + agent 结对）；单课 2–4h；总课数不预设，wayfinder 反推 | — |
| 4 | 验收标准 | TDD 红绿切片全绿 + 每概念簇 ≥1 条改写测试（镜像 vendor 测试路径、Apache 2.0 attribution）；side-by-side 真节点对照降为可选抽查；分歧以 vendor 源码行为仲裁并记 issue | — |
| 5 | 产物形态 | 单一演进代码库 + 5 模块 reactor + `lesson-XX.YY` git tag；lessons/ 只放讲义 | 0003 |
| 6 | 工程基线 | Java 11 / Maven / JUnit 4 / H2 1.4.197，依赖版本跟随 vendor pom；消息序列化代码手写 | 0002 |
| 7 | 保真策略 | 接口忠实、实现渐进；存储接缝从第一课保留 | 0004 |
| 8 | 互操作边界 | 复刻集群同构；线协议与线程模型**全保真**（ADR 0005 修订，2026-10-04）；唯一例外 = thin client 应用层协议（双端实现，真 Ignite 与复刻 thin client 均须能连复刻节点） | 0001、0005 |
| 9 | 运维面 | metrics/JMX 最小实现、不设专门课；control 工具进附录（命令本体在 core `internal/management`，随课弧自然生长） | 0001 |
| 10 | 代码注释 | 每复刻类头 vendor 锚点；注释（javadoc 与行注）一律中文，由 vendor 英文原文翻译（2026-10-04 用户修订，原"javadoc 英文"作废） | — |
| 11 | calcite 深度 | 集成层复刻（库为黑盒依赖）；深挖（planner rules/metadata）留作课后可选扩展 | 0001 |

## 6. Research 底座（16 份全部验收）

- **Phase A 四份**：`01-startup-path.md`、`02-module-dependency-graph.md`、`03-data-plane-put-path.md`、`04-persistence-sql.md`。
- **grill 补研三份**（2026-10-03）：`05-compute-services.md`、`06-calcite-integration.md`、`07-deployment-p2p.md`（验收记录见 §8）。
- **wayfinder 期补研九份**（2026-10-04，各章切票证据，均验收 9–12 处抽查）：`08-communication-nio.md`（章 3 全保真，带宽限流/心跳两假设修正）、`09-discovery-ring.md`（章 2 重切+多播，四个旧名证伪）、`10-affinity-exchange-rebalance.md`（章 4，late 不可关闭等四纠错）、`11-near-cache.md`（章 5，无 withNearCache）、`12-transactions.md`（章 6，3 隔离值/savepoint 不存在/tx mapping 不存 utility cache）、`13-persistence-internals.md`（章 7，BPlusTree 并发四件套+四个名证伪）、`14-sql-dml-ddl-join.md`（章 8，DML=SELECT 改写+CAS、LazyQueryList 证伪、事件归属）、`15-events-continuous-messaging.md`（事件弧，四纠错）、`16-client-binary-thin.md`（章 13，metadata 三通道+compactFooter 落定）。

- **已验收**（引用 100% 核验，可直接信任）：`01-startup-path.md`、`02-module-dependency-graph.md`、`03-data-plane-put-path.md`、`04-persistence-sql.md`。
- **grill 补研**（2026-10-03 决定，覆盖 compute/calcite/部署三个原盲区）：
  - `05-compute-services.md` — compute task 生命周期、services、data streamer、部署概览（**已验收**：9 处抽查引用全真；还纠正了调研提示里过时的类名——2.18 无 `DataStreamDataPayload`，实为 `DataStreamerRequest/Response`；2.18 的 `UriDeploymentSpi` 只有 file/http 两个 scanner）；
  - `06-calcite-integration.md` — calcite 集成层最小闭包（**已验收**：10 处抽查引用全真；P0 闭包 12 项 + 可简化 9 条 + 深挖 6 向；关键发现：三明治结构（core 钩子/calcite 接线/自有执行内核）、双引擎共享 `TOPIC_QUERY` 与 `InlineIndex`、传 JSON 计划而非 SQL、`VolcanoUtils` 同包注入证明"库当黑盒"有现实例外）；
  - `07-deployment-p2p.md` — 部署子系统专项：p2p 管道、UriDeploymentSpi（**已验收**：9 处抽查引用全真；关键发现：marshaller 映射协议（discovery 总线）与类字节码 p2p 分发（communication 总线）是两条独立协议必须拆成两讲、`GridDeployment` 的 `AtomicStampedReference` 状态机是并发核心、部署课弧依赖顺序 ①本地骨架→②p2p→③marshaller 映射→④UriDeploymentSpi）。

## 7. 课程目录约定（grill 定稿）

- `lessons/XX-topic/`（章节）+ `XX.YY-name/`（课）；每课目录含 `explainer.md`、验收测试来源说明与 `assets/` 配图目录（PNG 引用进讲义 + HTML 可交互版，2026-10-04 起，archify 产出；不保存 SVG）；
- **讲义（explainer.md）是每课的硬性收尾产物**：实施完成（代码 + 测试全绿）后产出，中文、面向学习者、**配图充分**（mermaid 架构图/时序图/流程图），以降低学习难度为第一目标（2026-10-03 wayfinder 建图时用户追加）；
- 复刻代码本体在仓库根的 5 模块 reactor 中持续演进（ADR 0003），每课完成打 tag `lesson-XX.YY`，`git diff` 相邻 tag 即两课教学增量；
- 每个复刻类头部标注 vendor 锚点（`// 对应 vendor: .../Xxx.java`）；注释（javadoc 与行注）一律中文，由 vendor 英文原文翻译（2026-10-04 修订）；
- ~~problem/solution 目录约定~~ 废除——与单一演进代码库冲突（ADR 0003 的 Considered Options）。

## 8. 进度追踪

- [x] Phase 0：`/setup-matt-pocock-skills`（GitHub Issues / 默认 triage 标签 / single-context domain docs / CLAUDE.md）
- [x] Phase A：research #1 启动路径 → `docs/research/01-startup-path.md`（已验收：143 条源码引用 100% 真实）
- [x] Phase A：research #2 模块依赖图 → `docs/research/02-module-dependency-graph.md`（已验收：45 个 pom 核验，Mermaid 逐边有证据）
- [x] Phase A：research #3 数据面主路径 → `docs/research/03-data-plane-put-path.md`（已验收：10 步调用链，本地/分布式对照）
- [x] Phase A：research #4 持久化 + SQL → `docs/research/04-persistence-sql.md`（已验收：持久化四件套 + H2/map-reduce）
- [x] Phase B：`/grill-with-docs`（2026-10-03 完成：16 问钉死，ADR 0001–0004、CONTEXT.md（12 术语）、骨架 v1、§5 决策表、research #5/#6/#7 补研启动）
- [x] 验收 research #5 `docs/research/05-compute-services.md`（2026-10-03：9 处抽查引用全部核验为真）
- [x] 验收 research #6 `docs/research/06-calcite-integration.md`（2026-10-03：10 处抽查引用全部核验为真）
- [x] 验收 research #7 `docs/research/07-deployment-p2p.md`（2026-10-03：9 处抽查引用全部核验为真）——**research 底座 01–07 全绿，Phase B 情报就绪**
- [x] Phase B：`/wayfinder`（**完成，2026-10-04**：[地图 #1](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/1) 16/16 票关闭 + 9 份补研验收；产出 [docs/course-map-v1.md](docs/course-map-v1.md)——97 主课+1 选做、章间 DAG、kernal 终态对齐、附录四节）
- [x] Phase B：`/to-spec`（**完成，2026-10-04**：[课程 spec · #27](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/27)，已打 `ready-for-agent`——问题/方案/92 条用户故事/实现与测试决策（四接缝 S1–S4 已确认）/范围外；输入=course-map-v1+16 决议票+16 调研）
- [x] Phase B：`/to-tickets`（**完成，2026-10-04**：98 张课票 #28–#125 全部 `ready-for-agent`、挂为 [spec #27](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/27) sub-issue、107 条原生 blocking 边；frontier = [Lesson 0.1 · #28](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/28)）——**Phase B 四步全部完成，进入 Phase C**
- [x] Phase C：Lesson 0.1 · 构建骨架（2026-10-04，[#28](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/28)：五模块 reactor（provided+shade）+ parent/parent-internal/bom + JUnit4 基线；锚点类 IgniteCheckedException/X/GridUnsafe/BinaryNameMapper/IgniteState；tracer=mvn install 全绿（17/17）+ commons→core 冒烟；Maven 坐标 groupId=dev.lessonignite（包名仍 org.apache.ignite.*）；tag `lesson-0.1`）
- [x] Phase C：Lesson 0.2 · 入口与注册表（2026-10-05，[#29](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/29)：Ignition/IgnitionEx 注册表语义（重名抛/getOrStart 幂等/putIfAbsent 并发仲裁/startLatch 定局/finally 回滚）+ initializeConfiguration 0.2 切片（nodeId/consistentId/workDir/GridLoggerProxy）+ Spring 入口显式抛（ADR 0001 首次应用）+ 最小 IgniteKernal 标识面；新增 27 类全带 vendor 锚点；tracer=改写 GridGetOrStartSelfTest + 14 自写切片（全 reactor 33/33）；tag `lesson-0.2`）
- [x] Phase C：Lesson 0.3 · kernal 容器与最小闭包（2026-10-05，[#30](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/30)：IgniteKernal 容器本体（gateway 状态机读写锁守卫/GridKernalContextImpl comps 注册表/GridComponent 先注册后启动契约）+ vendor 全签名 start（启动序列=vendor §6 时间线子序列：步 6→7→9→P8→P12→26→31）+ 最小闭包（PoolProcessor P8 + GridTimeoutProcessor P12）+ marshaller 三件套（JdkMarshaller 完整含 ServiceLoader api/impl SPI 拆分、BinaryMarshaller delegate 占位、MarshallerContextImpl 空注册表）+ 最小停链（失败回滚与正式 stop 共用）；新增 43 主类 + 7 测试类全带 vendor 锚点；生长不变量子序列断言落测试（新组件强制登记 vendor 序号）；tracer=STARTED+JdkMarshaller 往返 + 改写 GridTimeoutProcessorSelfTest（全 reactor 66/66）；tag `lesson-0.3`）
- [ ] Phase C：Lesson 0.4 → 13.7 逐课推进（课票 #31–#125，frontier 由 issue 依赖图决定）
