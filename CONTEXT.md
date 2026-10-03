# Lesson-Ignite2

以课程（lesson）形式从零复刻 Apache Ignite 2.18.0 的教学项目：走完全部课程后自然得到完整的 Ignite 等价物。`vendors/ignite/` 是官方源码只读参照，不是复刻本体。

## Language

### 复刻域

**复刻（Replica）**:
本仓库通过逐课实施产出的 Ignite 2.18.0 等价物；包名与类名对齐官方源码（`org.apache.ignite.*`）。
_Avoid_: 重写、仿写、vendor 代码

**内核闭包（Kernel Closure）**:
被 shade 进 ignite-core 发布产物的 5 个模块（`commons` / `binary/api` / `binary/impl` / `unsafe` / `core`）——节点能 `Ignition.start()` 的最小模块集。
_Avoid_: core 模块组、内核全家桶

**存储接缝（Storage Seam）**:
2.18 中 cache entry 层（`GridCacheMapEntry`）与数据存储层（offheap manager / `CacheDataStore`）之间的正式边界；复刻从第一课就保留此接缝，实现渐进替换。
_Avoid_: 存储抽象（泛称）、DAO

**接口忠实、实现渐进（Interface-faithful, implementation-progressive）**:
复刻保真策略：类/接口的形状、位置与公开签名对齐 2.18 源码，方法体允许先给简化实现、后续课在真实接缝内替换。
_Avoid_: 逐行翻译、自由发挥

**互操作边界（Interop Boundary）**:
复刻集群同构——replica 节点只与 replica 节点组网，线协议内部可简化；唯一例外是 thin client 应用层协议（真 Ignite 与复刻的 thin client 都须能连复刻节点）。
_Avoid_: 协议兼容、兼容模式

**部署子系统（Deployment Subsystem）**:
compute 派单所依赖的类到达机制全集：GridDeploymentManager 生命周期 + peer class loading 管道 + UriDeploymentSpi 类源。
_Avoid_: urideploy 模块（易误解为仅 SPI）、类加载（泛指 JVM 机制）

### 课程形态

**课（Lesson）**:
课程最小实施单元：一个新概念簇 + 一个 tracer bullet 里程碑 + 验收测试全绿；预算 2–4 小时 agent 结对实施。
_Avoid_: 练习（exercise）、任务（task，与 Ignite compute task 撞名）

**Tracer Bullet**:
一课里程碑处产出的最薄端到端功能切片（如「单节点 `Ignition.start()` + put/get 跑通」）。
_Avoid_: MVP、demo

**讲义（Explainer）**:
每课实施完成后产出的学习者文档，存于 `lessons/XX.YY-name/explainer.md`，配图充分（mermaid）、以降学习难度为第一目标。
_Avoid_: 教程、README、problem/solution 目录

**章节（Chapter）**:
`lessons/` 下按主题聚合的课组目录（`XX-topic/`）；对应 ROADMAP 骨架中"阶段"的落盘形态。
_Avoid_: 阶段（stage，仅指 ROADMAP §4 的骨架叙事，切课前的粗粒度分组）

**验收测试（Acceptance Test）**:
一课收官的准绳：TDD 红绿切片全绿 + 该课概念簇至少一条改写测试；语义分歧以 vendor 源码实际行为仲裁并记 issue。
_Avoid_: 单元测试（泛指）、冒烟测试

**改写测试（Adapted Test）**:
从 Ignite 官方测试套件（Apache 2.0）改写移植的验收测试：保留断言语义、注明来源，放镜像 vendor 的测试路径下。
_Avoid_: 抄测试、翻译测试
