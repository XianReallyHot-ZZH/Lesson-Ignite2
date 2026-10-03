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

## 4. 课程骨架 v1（2026-10-03 grill 定稿，交由 wayfinder 切课）

切课纪律：**认知负担优先、学习曲线平滑**（§3.2）——骨架只定章节顺序与每章概念簇，课的粒度由 wayfinder 按"每课一个新概念簇 + 2–4h agent 预算"反推；总课数不预设（30–50 仅作锚）。

| 章节 | 主题 | 引入的概念簇 | 里程碑（tracer bullet） |
|---|---|---|---|
| 0 | 构建骨架与 Ignition 生命周期 | 5 模块 reactor 空壳（provided+shade 协议）、`Ignition`/`IgniteKernal`/配置/logger、最小 kernal 逐课生长 | 单节点 `start()` 成功 |
| 1 | 单节点缓存（PARTITIONED+ATOMIC 退化形态） | Cache API 子集、存储接缝（entry 层 × CacheDataStore）、on-heap 简化实现、扫描/过期 | 单机 put/get/scan |
| 2 | SPI 抽象 + Discovery | SPI 框架、`TcpDiscoverySpi` 简化 | 2 节点互相发现 |
| 3 | 通信 | `GridIoManager`/NIO 简化（内部简化白名单）、消息 marshaller | 跨节点消息往返 |
| 4 | Affinity 与分布 | `RendezvousAffinityFunction`、partitioned/replicated、rebalance | 数据按分区正确落位 |
| 5 | near cache（独立课） | 读加速、reader 登记、primary 捎带回传 | near 命中率可观测 |
| 6 | 事务 | 隐式微事务、悲观 2PC | 跨节点事务提交/回滚 |
| 7 | 持久化 | page memory、WAL、checkpoint、baseline；替换存储接缝实现 | 重启后数据存活 |
| 8 | SQL 之 H2 课弧 | H2 集成、索引、分布式查询/join（H2 1.4.197） | 跨节点 SQL 查询 |
| 9 | SQL 之 calcite 课弧 | `QueryEngine` SPI、集成层、引擎切换/hint（Calcite 库为黑盒依赖） | 同一 SQL 双引擎跑通 + hint 切换 |
| 10 | 计算与服务 | `IgniteCompute`/MR、data streamer、services | 分布式 MR 作业跑通 |
| 11 | 部署子系统 | `GridDeploymentManager`、peer class loading 管道、`UriDeploymentSpi`（file/http） | p2p 模式下 compute 派单类到达远端 |
| 12 | 收尾 | binary marshaller 完整化、thin client 协议两端（真/复刻 client 均连复刻节点）、metrics 最小实现 | 真 thin client 连复刻节点 put/get |

附录（不设课）：外围 17 模块接缝清单、schedule、maven resolver、control 工具接缝（命令本体在 core，随课弧自然生长）、深挖 calcite 扩展路线（课后可选开发）。

骨架 v0 → v1 的变化依据：ADR [0001](docs/adr/0001-replication-scope-and-compatibility.md)（范围/互操作）、[0003](docs/adr/0003-single-evolving-codebase-5-module-reactor.md)（reactor 形态）、[0004](docs/adr/0004-interface-faithful-implementation-progressive.md)（阶段 1 重定义、near cache 拆分）；research 02（内核闭包）、03（LOCAL 已删/ATOMIC 分叉）、04（双引擎）。

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
| 8 | 互操作边界 | 复刻集群同构、线协议内部可简化；唯一例外 = thin client 应用层协议（双端实现，真 Ignite 与复刻 thin client 均须能连复刻节点） | 0001 |
| 9 | 运维面 | metrics/JMX 最小实现、不设专门课；control 工具进附录（命令本体在 core `internal/management`，随课弧自然生长） | 0001 |
| 10 | 代码注释 | 每复刻类头 vendor 锚点 + 中文教学注释；javadoc 英文 | — |
| 11 | calcite 深度 | 集成层复刻（库为黑盒依赖）；深挖（planner rules/metadata）留作课后可选扩展 | 0001 |

## 6. Research 底座（Phase A 4 份已验收 + grill 补研 3 份）

- **已验收**（引用 100% 核验，可直接信任）：`01-startup-path.md`、`02-module-dependency-graph.md`、`03-data-plane-put-path.md`、`04-persistence-sql.md`。
- **grill 补研**（2026-10-03 决定，覆盖 compute/calcite/部署三个原盲区）：
  - `05-compute-services.md` — compute task 生命周期、services、data streamer、部署概览（**已验收**：9 处抽查引用全真；还纠正了调研提示里过时的类名——2.18 无 `DataStreamDataPayload`，实为 `DataStreamerRequest/Response`；2.18 的 `UriDeploymentSpi` 只有 file/http 两个 scanner）；
  - `06-calcite-integration.md` — calcite 集成层最小闭包（**已验收**：10 处抽查引用全真；P0 闭包 12 项 + 可简化 9 条 + 深挖 6 向；关键发现：三明治结构（core 钩子/calcite 接线/自有执行内核）、双引擎共享 `TOPIC_QUERY` 与 `InlineIndex`、传 JSON 计划而非 SQL、`VolcanoUtils` 同包注入证明"库当黑盒"有现实例外）；
  - `07-deployment-p2p.md` — 部署子系统专项：p2p 管道、UriDeploymentSpi（**已验收**：9 处抽查引用全真；关键发现：marshaller 映射协议（discovery 总线）与类字节码 p2p 分发（communication 总线）是两条独立协议必须拆成两讲、`GridDeployment` 的 `AtomicStampedReference` 状态机是并发核心、部署课弧依赖顺序 ①本地骨架→②p2p→③marshaller 映射→④UriDeploymentSpi）。

## 7. 课程目录约定（grill 定稿）

- `lessons/XX-topic/`（章节）+ `XX.YY-name/`（课）；每课目录只含 `explainer.md` 与验收测试来源说明；
- **讲义（explainer.md）是每课的硬性收尾产物**：实施完成（代码 + 测试全绿）后产出，中文、面向学习者、**配图充分**（mermaid 架构图/时序图/流程图），以降低学习难度为第一目标（2026-10-03 wayfinder 建图时用户追加）；
- 复刻代码本体在仓库根的 5 模块 reactor 中持续演进（ADR 0003），每课完成打 tag `lesson-XX.YY`，`git diff` 相邻 tag 即两课教学增量；
- 每个复刻类头部标注 vendor 锚点（`// 对应 vendor: .../Xxx.java`）、中文教学注释、英文 javadoc；
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
- [ ] Phase B：`/wayfinder`（地图已建：[#1 课程地图](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/1) + 15 张子票 + 28 条依赖边；frontier = [marshaller 演进主线票 #2](https://github.com/XianReallyHot-ZZH/Lesson-Ignite2/issues/2)；每次 `/wayfinder` 调用解一票，同窗口进行到 frontier 清空）
- [ ] Phase B：`/to-spec`（课程 spec）
- [ ] Phase B：`/to-tickets`（课程 ticket DAG，blocking edges = 先修关系）
- [ ] Phase C：Lesson 01（由 tickets 生成后回填逐课清单）
