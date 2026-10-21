# 兼容性与验证范围

## 支持状态与证据

SDK 版本为 `0.1.0-SNAPSHOT`。矩阵中的“已验证”只覆盖该行精确 JDK、服务端与能力范围；“理论兼容 / 待验证”不能用于生产支持承诺；“不支持”是 DriverKit 当前支持策略。原生驱动的上游声明不能自动提升本 SDK 的状态。

<!-- BEGIN COMPATIBILITY MATRIX -->

| SDK 模块 | 原生驱动 | 服务端 / 范围 | JDK | 状态 | 证据或限制 |
| --- | --- | --- | --- | --- | --- |
| driverkit-core | JDK 8+ | 无服务端 / 核心运行 | 8 | 已验证 | Java 8 smoke；17/21 生命周期、并发和期限回归。 [CI](https://github.com/Samhom/driverkit/actions/runs/39602884827) |
| driverkit-core | JDK 8+ | 无服务端 / 核心运行 | 17 | 已验证 | Java 8 smoke；17/21 生命周期、并发和期限回归。 [CI](https://github.com/Samhom/driverkit/actions/runs/39602884827) |
| driverkit-core | JDK 8+ | 无服务端 / 核心运行 | 21 | 已验证 | Java 8 smoke；17/21 生命周期、并发和期限回归。 [CI](https://github.com/Samhom/driverkit/actions/runs/39602884827) |
| driverkit-redis-jedis | jedis 8.0.2 | Redis 8.2.3 / 单节点认证与恢复 | 17 | 已验证 | 真实单节点：认证拒绝、网络暂停恢复、重启恢复及凭据轮换通过；不包含多节点/TLS/消费位点认证。 [CI](https://github.com/Samhom/driverkit/actions/runs/39605732975) |
| driverkit-redis-jedis | jedis 8.0.2 | Redis 8.2.3 / 单节点认证与恢复 | 21 | 理论兼容 / 待验证 | JDK 21 构建通过；该服务组合尚未运行真实集群回归。 |
| driverkit-redis-jedis | jedis 8.0.2 | Redis 8.2.3 / 适配模块运行 | 8 | 不支持 | 适配模块使用 Java 17 字节码；core 的 Java 8 能力不代表本模块支持 Java 8。 |
| driverkit-elasticsearch | elasticsearch-java 9.5.5 | Elasticsearch 9.5.5 / 单节点认证与恢复 | 17 | 已验证 | 真实单节点：认证拒绝、网络暂停恢复、重启恢复及凭据轮换通过；不包含多节点/TLS/消费位点认证。 [CI](https://github.com/Samhom/driverkit/actions/runs/39605732975) |
| driverkit-elasticsearch | elasticsearch-java 9.5.5 | Elasticsearch 9.5.5 / 单节点认证与恢复 | 21 | 理论兼容 / 待验证 | JDK 21 构建通过；该服务组合尚未运行真实集群回归。 |
| driverkit-elasticsearch | elasticsearch-java 9.5.5 | Elasticsearch 9.5.5 / 适配模块运行 | 8 | 不支持 | 适配模块使用 Java 17 字节码；core 的 Java 8 能力不代表本模块支持 Java 8。 |
| driverkit-couchbase | java-client 3.13.0 | Couchbase Community 7.6.2 / 单节点认证与恢复 | 17 | 已验证 | 真实单节点：认证拒绝、网络暂停恢复、重启恢复及凭据轮换通过；不包含多节点/TLS/消费位点认证。 [CI](https://github.com/Samhom/driverkit/actions/runs/39605732975) |
| driverkit-couchbase | java-client 3.13.0 | Couchbase Community 7.6.2 / 单节点认证与恢复 | 21 | 理论兼容 / 待验证 | JDK 21 构建通过；该服务组合尚未运行真实集群回归。 |
| driverkit-couchbase | java-client 3.13.0 | Couchbase Community 7.6.2 / 适配模块运行 | 8 | 不支持 | 适配模块使用 Java 17 字节码；core 的 Java 8 能力不代表本模块支持 Java 8。 |
| driverkit-rocketmq | rocketmq-client 5.5.1 | RocketMQ 5.5.0 / 单节点认证与恢复 | 17 | 已验证 | 真实单节点：认证拒绝、网络暂停恢复、重启恢复及凭据轮换通过；不包含多节点/TLS/消费位点认证。 [CI](https://github.com/Samhom/driverkit/actions/runs/39605732975) |
| driverkit-rocketmq | rocketmq-client 5.5.1 | RocketMQ 5.5.1 / 单节点认证与恢复 | 21 | 理论兼容 / 待验证 | JDK 21 构建通过；该服务组合尚未运行真实集群回归。 |
| driverkit-rocketmq | rocketmq-client 5.5.1 | RocketMQ 5.5.1 / 适配模块运行 | 8 | 不支持 | 适配模块使用 Java 17 字节码；core 的 Java 8 能力不代表本模块支持 Java 8。 |
| driverkit-redis-jedis | jedis 8.0.2 | Redis 7.x / 受管单节点 | 17 | 理论兼容 / 待验证 | RESP2 协议测试通过；7.x 真实服务组合待验证。 |
| driverkit-elasticsearch | elasticsearch-java 9.5.5 | Elasticsearch 7.x / 跨主要版本 | 17 | 不支持 | 主线仅维护 9.x 客户端组合；7.x 需独立代际适配。 |
| driverkit-couchbase | java-client 3.13.0 | Couchbase Server 8.x / KV / collections | 17 | 理论兼容 / 待验证 | 未执行该服务端版本的真实回归。 |
| driverkit-rocketmq | rocketmq-client 5.5.1 | RocketMQ 4.9.x / Remoting 协议 | 17 | 理论兼容 / 待验证 | 协议兼容不能代替认证、消费恢复及 Broker 版本实测。 |
| driverkit-rocketmq-transport | rocketmq-remoting 5.5.1 | 本地 TLS 服务 / 证书链与服务器身份 | 17 | 已验证 | 固定输入重建；可信链、未知 CA、过期、错误 DNS 与 IP SAN 握手验证。 [CI](https://github.com/Samhom/driverkit/actions/runs/39602884827) |
| driverkit-rocketmq-transport | rocketmq-remoting 5.5.1 | RocketMQ 5.5.1 TLS / 真实 Broker TLS | 17 | 理论兼容 / 待验证 | 本地真实握手通过；未运行真实 Broker TLS/mTLS 拓扑。 |
| driverkit-rocketmq-grpc | rocketmq-client-java-noshade 5.2.2 | 本地 gRPC 服务 / 实验模块传输 | 17 | 已验证 | 认证拒绝、无启动重试、断线重连、未知 CA 拒绝；默认隔离。 [CI](https://github.com/Samhom/driverkit/actions/runs/39602884827) |
| driverkit-rocketmq-grpc | rocketmq-client-java-noshade 5.2.2 | RocketMQ 5.5.1 Proxy / 实验模块消费 | 17 | 理论兼容 / 待验证 | 尚无真实 Proxy 消费恢复和位点证据，需人工安全评审。 |
| driverkit-redis-jedis3 | jedis 3.10.0 | Redis 6.x/7.x / 隔离迁移适配 | 17 | 理论兼容 / 待验证 | 具体生命周期适配已实现；原生依赖由应用提供，真实旧集群未验证，需人工安全评审。 |
| driverkit-elasticsearch7 | elasticsearch-rest-high-level-client 7.17.29 | Elasticsearch 7.17.x / 隔离迁移适配 | 17 | 理论兼容 / 待验证 | 具体生命周期适配已实现；原生依赖由应用提供，真实旧集群未验证，需人工安全评审。 |
| driverkit-couchbase2 | java-client 2.7.23 | Couchbase Server 6.x/7.x / 隔离迁移适配 | 17 | 理论兼容 / 待验证 | 具体生命周期适配已实现；原生依赖由应用提供，真实旧集群未验证，需人工安全评审。 |
| driverkit-rocketmq4 | rocketmq-client 4.9.8 | RocketMQ 4.9.x / 隔离迁移适配 | 17 | 理论兼容 / 待验证 | 具体生命周期适配已实现；原生依赖由应用提供，真实旧集群未验证，需人工安全评审。 |

<!-- END COMPATIBILITY MATRIX -->

矩阵源为 [compatibility.json](compatibility.json)。`python3 tools/check_compatibility.py --render` 生成此表；CI 验证模块、SDK/原生依赖版本、重复组合和证据链接，依赖升级后必须同步矩阵。将真实服务行改成已验证前，须记录实际通过的不可变 CI run 与复现命令。

本地 RESP/HTTP/gRPC/TLS 协议测试不等同于真实服务端认证。真实单节点恢复通过也不能推导出 Sentinel/Cluster、多节点故障转移、事务、重平衡、位点、P99 或 mTLS 已验证。

## 依赖组合

主线 BOM 管理 Netty 4.1.139、Jackson 2.22.4 / 3.1.8、OpenTelemetry 1.67、gRPC 1.85 及 LZ4 1.12 等安全修复基线。Couchbase 自有内嵌依赖保持上游隔离，由审查工具另行识别；不能通过普通 BOM 替换这些包。

主线排除 Apollo 的旧 YAML 解析器和 Remoting 客户端携带的旧独立日志后端。运行日志实现由应用选择，SDK 不随包绑定默认日志后端。

同一进程同一产品默认选择一个原生驱动代际。旧版本通过独立适配或 FactoryAdapter 渐进接入；跨代际共存需要额外的依赖、类加载、SPI 和原生库验证。
