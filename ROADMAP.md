# Lesson-Ignite2 路线图 —— 以课程形式从零复刻 Apache Ignite 2.18.0

> 本文档是项目的操作手册：总路线、课程划分原则、骨架草稿、待钉死的决策、进度追踪。
> 它由 2026-10-03 的 ask-matt 路由分析产生，后续各阶段（grill / wayfinder / spec / tickets）的产出会回填并细化它。

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
2. **每课只引入一个新概念簇**：复用之前全部概念，新概念一次一撮。
3. **螺旋式重访**：同一对象以递增深度重现——cache 依次经历 本地并发 → 分布式分区 → 事务 → 持久化 → SQL 索引。
4. **每课验收 = 测试全绿 + 与真 Ignite 行为对比**：尽量改写 Ignite 自己的（Apache 2.0）测试作为验收标准。

## 4. 课程骨架 v0（原料，交由 wayfinder 细化）

估 30–50 课。**这是喂给 grill/wayfinder 的原料，不是终稿**——真正切几课、每课边界在哪，由 wayfinder 的决策票逐个解决。

| 阶段 | 主题 | 引入的概念簇 | 里程碑（tracer bullet） |
|---|---|---|---|
| 0 | 构建骨架与 Ignition 生命周期 | `Ignition` / `IgniteKernal` / 配置 / logger | 单节点 `start()` 成功 |
| 1 | 本地 cache | Cache API 子集、并发容器、扫描/过期 | 单机 put/get/scan |
| 2 | SPI 抽象 + Discovery | SPI 框架、`TcpDiscoverySpi` 简化 | 2 节点互相发现 |
| 3 | 通信 | `GridIoManager` / NIO 简化、消息 marshaller | 跨节点消息往返 |
| 4 | Affinity 与分布 | `RendezvousAffinityFunction`、partitioned/replicated、rebalance | 数据按分区正确落位 |
| 5 | 事务 | 悲观 2PC、near cache | 跨节点事务提交/回滚 |
| 6 | 持久化 | page memory、WAL、checkpoint、baseline | 重启后数据存活 |
| 7 | SQL | H2 集成、索引、分布式查询/join | 跨节点 SQL 查询 |
| 8 | 计算与服务 | `IgniteCompute`/MR、data streamer、services、schedule | 分布式 MR 作业跑通 |
| 9 | 周边完整化 | binary marshaller 完整化、metrics/JMX、control 工具、thin client | 运维面可用 |

## 5. 必须先钉死的范围决策（grill 第一批问题）

1. **"复刻 2.18.0" 的定义**：39 个集成模块、platforms（.NET/C++/Python）、calcite 新 SQL、MVCC——是课程目标，还是显式排除（或附录课）？
2. **兼容等级**：包名 API 级（`org.apache.ignite.*`）/ 行为级 / 能跑 Ignite 测试子集？
3. **学习者画像**与每课时长预算？
4. **每课验收标准**与测试来源？
5. **课程产物形态**：代码 + 讲义？复刻代码是一份持续演进的代码库，还是每课独立目录？（决定仓库布局）
6. **工程基线**：Java 版本 / 构建工具对齐 2.18 原版，还是现代化？

## 6. 就绪的 4 个 research 问题（Phase A，可直接粘贴给 `/research`）

1. 以 `vendors/ignite`（Apache Ignite 2.18.0 源码）为 primary source，梳理 Ignite 的启动路径：从 `Ignition.start()` 到 `IgniteKernal` 完成启动的完整生命周期，按启动顺序列出所有被初始化的 manager 与 SPI，说明每个组件的职责与关键源码文件（带路径引用），产出带引用的 Markdown。
2. 以 `vendors/ignite` 为 primary source，绘制 2.18 拆分后的模块依赖图：`core` / `binary` / `commons` / `unsafe` / `compress` / `indexing` / `calcite` / `clients` / `platforms` 等模块之间的 Maven 依赖关系与职责边界，指出哪些是内核必修、哪些是外围可选，产出带引用的 Markdown。
3. 以 `vendors/ignite` 为 primary source，追踪数据面主路径：一次 `cache.put()` 在 core 模块中依次经过的组件链（从 Cache API 入口到存储落地），对比本地路径与分布式路径的差异，附源码文件引用，产出带引用的 Markdown。
4. 以 `vendors/ignite` 为 primary source，梳理持久化与 SQL 的组件构成：page memory、WAL、checkpoint 的结构与交互；`indexing` 模块如何集成 H2、分布式查询如何切分与聚合，附源码文件引用，产出带引用的 Markdown。

## 7. 课程目录约定（提案，待 grill 确认）

借自 Matt `scaffold-exercises` 的约定（其本体依赖 ai-hero-cli，不直接使用，只借结构思路）：

- 课程按 `lessons/XX-topic/`（章节）+ `XX.YY-name/`（课）组织；
- 每课可含三个子目录：`problem/`（带 TODO 的学员工作区）、`solution/`（参照实现）、`explainer/`（概念讲解，无 TODO）；
- 复刻代码本体的布局（单一演进代码库 vs 每课快照）见 §5 第 5 条，未定。

## 8. 进度追踪

- [ ] Phase 0：`/setup-matt-pocock-skills`
- [ ] Phase A：research #1 启动路径
- [ ] Phase A：research #2 模块依赖图
- [ ] Phase A：research #3 数据面主路径
- [ ] Phase A：research #4 持久化 + SQL
- [ ] Phase B：`/grill-with-docs`（钉死 §5 的范围决策）
- [ ] Phase B：`/wayfinder`（课程地图）
- [ ] Phase B：`/to-spec`（课程 spec）
- [ ] Phase B：`/to-tickets`（课程 ticket DAG，blocking edges = 先修关系）
- [ ] Phase C：Lesson 01（由 tickets 生成后回填逐课清单）
