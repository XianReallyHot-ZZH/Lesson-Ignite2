# 0002 · 工程基线对齐 2.18 原版

复刻工程栈全面对齐 vendor：**Java 11**（`maven.compiler.release=11`，vendor parent pom 锁 11）、**Maven**、**JUnit 4**（vendor 锁 4.12）、**H2 锁 1.4.197**（`GridH2Table` 等 split-package 补丁类绑死该版本）、第三方依赖版本跟随 vendor pom。消息序列化代码**手写**，不复刻 codegen2 注解处理器（它是构建期工具，复刻它学的是注解处理不是 Ignite；research 02 已给此结论）。

## Considered Options

- 现代化栈（Java 21 / JUnit 5 / 新版 H2）：否决——0001 的测试级准绳依赖"借 Ignite 测试零摩擦"，基线偏移会让这条捷径全程失效；升级 H2 会直接破坏 SQL 课弧的补丁类。

## Consequences

- 开发体验偏旧（JUnit 4、Java 11）；本机用 Maven toolchains 在任意新 JDK 上运行即可。
