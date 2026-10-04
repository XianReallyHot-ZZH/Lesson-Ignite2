# 0005 · 线协议与线程模型全保真

修订 ADR 0001 互操作边界条款中的"线协议内部允许简化"子句：**通信层（TcpCommunicationSpi/GridNioServer：NIO 线程模型、握手、ack、重连、心跳、连接池、带宽限流）与发现层（TcpDiscoverySpi/ServerImpl：ring 消息全集、故障检测、ring 愈合）一律全保真复刻，不做简化**。同构集群的边界不变（复刻节点不与真 Ignite 互操作），thin client 例外不变；变化的是：即便无人要求互通，协议与线程模型也不砍。

**Why**：用户的学习目标包含真实的网络编程与协议工程（NIO selector 体系、握手/心跳/限流协议、ring 故障检测）；这些是 Ignite 工程价值的核心部分而非可替换细节。2026-10-04 wayfinder 章 3 切票时用户两次推翻简化建议后确认。

**Consequences**：通信课弧 4→约 6–8 课、发现课弧 2.3/2.4 重切扩展（等 research #08/#09 证据）；"线程模型可简化"判例（原章 2 决议附带）作废。`TcpDiscoveryMulticastIpFinder` **一并全保真复刻**（用户同日确认）——多播 socket 的地址请求/响应交换协议属课程主线，非附录。
