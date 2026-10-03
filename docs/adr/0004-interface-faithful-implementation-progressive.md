# 0004 · 接口忠实、实现渐进

复刻保真策略：类/接口的**形状、位置与公开签名**对齐 2.18 源码，方法体允许先给简化实现、后续课在真实接缝内替换。核心应用是**存储接缝**：`GridCacheMapEntry`（entry 层）与 offheap manager / `CacheDataStore`（存储层）的边界从第一课就保留；阶段 1 的"本地 cache"据此重新定义为「单节点 PARTITIONED+ATOMIC、无备份、affinity 退化为全本机」——2.18 已删除 `CacheMode.LOCAL`（research 03），不存在字面意义的本地模式；off-heap B+ 树虽是 2.18 权威存储路径，其完整实现（page memory）推迟到持久化课弧，先以 on-heap 简化实现填充同一接缝。

## Considered Options

- 教学脚手架路线（阶段 1–5 用 ConcurrentHashMap 式存储、阶段 6 推倒重写）：否决——"螺旋重访"变"推倒重来"，且前期代码与 vendor 源码对不上号。
- 全真路线（阶段 1 直接做 off-heap）：否决——认知负担爆炸。

## Consequences

- 阶段 1–5 与 vendor 的差异仅存在于方法体内部，接缝与调用方一致；持久化课只替换实现、不改调用方。
- near cache 与事务正交（ATOMIC 亦有 near），拆为独立课，置于 affinity 课弧之后、事务课之前。
