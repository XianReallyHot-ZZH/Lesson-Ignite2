# 0001 · 复刻范围与兼容等级

> 注：本文"Considered Options"中"线协议内部允许简化"子句已被 [ADR 0005](0005-wire-protocol-full-fidelity.md) 修订为全保真（2026-10-04）。

课程复刻范围 = **内核闭包**（commons / binary-api / binary-impl / unsafe / core，即 shade 进 ignite-core 的 5 模块）+ **indexing**（H2 SQL）+ **calcite 集成层**（Apache Calcite 库作黑盒依赖，只复刻 Ignite 侧集成代码）+ **部署子系统**（GridDeploymentManager + peer class loading 管道 + UriDeploymentSpi 的 file/http resolver）。platforms（.NET/C++）与外围 17 模块显式排除（附录只记接缝、不设课）；实验性 MVCC 不在范围内——2.18 源码已将其整体删除（全 core 无 `MvccMode`）。兼容等级 = **包名 API 级**（`org.apache.ignite.*` 原包原类名、签名尽量一致，未复刻 API 显式 `UnsupportedOperationException`），以**测试级准绳**验收（能编译运行改写自 Ignite 的测试子集）。

## Considered Options

- 范围扩大：曾考虑仅内核+H2；采纳双 SQL 引擎（H2 课弧教 SQL-on-cache 概念，calcite 课弧教引擎可插拔 + 第二种分布式执行模型）与部署子系统（只做 UriDeploymentSpi 学不到机制本质，p2p 管道才是核心）。
- 互操作边界：复刻集群**同构**（replica 节点只与 replica 节点组网），线协议内部允许简化、消息类名与字段形状对齐；**真 Ignite 节点互操作显式排除**（字节级线协议兼容成本爆炸）。唯一例外 = **thin client 应用层协议**：真 Ignite thin client 与复刻自己的 thin client 都必须能连上复刻节点完成 put/get（协议两端都做）。
- 深挖 calcite（planner rules / metadata 逐个复刻）：留作课程结束后的可选扩展开发，不入课。
- 运维面：metrics/JMX 只做 manager + 注册表最小实现（启动链绕不开），不设专门课；control 工具进附录（命令本体在 core `internal/management`，壳在 control-utility 模块——排除的只是壳）。

## Consequences

- 部署子系统入范围 ⇒ `GridMarshallerMappingProcessor` 的映射交换（p2p 类加载管道）从可选变必修。
- calcite/部署的最小闭包细节由 research #6/#7 报告钉死后交 wayfinder 切课。
