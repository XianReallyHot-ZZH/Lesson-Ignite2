# 0003 · 单一演进代码库 + 5 模块 reactor

复刻代码本体是一份**持续演进的代码库**：第一天起就是镜像 2.18 拆分的 5 模块 Maven reactor 空壳（commons / binary-api / binary-impl / unsafe / core），随课填充，indexing / calcite / urideploy 模块到各自课弧再加入 reactor。每课打 git tag（`lesson-XX.YY`）作快照；`lessons/XX.YY-name/` 只放讲义（explainer），不放代码拷贝。

## Considered Options

- 每课快照目录（problem/solution 结构）：否决——螺旋式课程中后课持续修改前课代码（如持久化课替换存储实现），独立拷贝会指数膨胀。
- 单模块起步、后期拆 5 模块：否决——模块边界（provided + shade 协议）本身就是 2.18 架构的一课，后期拆分是纯 pom 工程活且打乱包名布局。

## Consequences

- 任何时刻 `git diff lesson-X lesson-Y` 即两课之间的教学增量。
- 讲义与代码分离：讲义讲"为什么"，代码演进史讲"怎么长出来的"。
