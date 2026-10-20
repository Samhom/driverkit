# 兼容性与验证范围

## 当前基线

| 模块 | 原生版本 | 运行要求 | 默认构建中的验证 |
| --- | --- | --- | --- |
| core | JDK 标准库 | Java 8+ | 生命周期、并发、超时、撤销、消费组控制、Java 8 smoke |
| Redis | Jedis 8.0.2 | Java 17+ 适配模块 | 原生 RESP 认证、KV、连接池切换、Pipeline、本地协议测试 |
| Elasticsearch | Java API Client 9.5.5 | Java 17+ | 原生 HTTP 认证、文档操作、429、Bulk 部分失败与请求限制 |
| Couchbase | Java SDK 3.13.0 | Java 17+ 适配模块 | 编译和参数边界；实际集群联调由接入方执行 |
| RocketMQ | Remoting Client 5.5.1 | Java 17+ 适配模块 | 编译、参数边界、共用消费组控制器；实际 Broker 联调由接入方执行 |
| Apollo | Client 2.5.0 | Java 17+ 集成模块 | 原子单文档、字段白名单、坏配置拒绝、订阅释放 |
| Spring Boot | 3.5.16 | Java 17+ | 属性绑定、Bean 创建、容器关闭 |
| gRPC 试验模块 | RocketMQ Java Client 5.2.2 | Java 17+ | 仅显式试验配置；有未解决的上游内嵌组件风险 |

JDK 17 与 21 由 Linux CI 执行主线构建；core 使用 Java 8 字节码，并提供独立的 Java 8 运行检查。

Remoting 适配器拒绝上游未经服务器身份验证的 TLS 模式；当前仅支持显式选择的受保护网络部署。TLS 接入需专用适配并经人工安全评审，详见 [传输限制](../SECURITY.md#rocketmq-remoting-传输限制)。

本地协议测试使用真实客户端连接受控协议服务，不等同于真实服务端完整兼容性认证。TLS、实际角色授权、拓扑切换、集群级事务、位点恢复、负载与故障恢复仍需目标环境联调。

## 依赖组合

主线 BOM 管理 Netty 4.1.139、Jackson 2.22.4 / 3.1.8、OpenTelemetry 1.67、gRPC 1.85 及 LZ4 1.12 等安全修复基线。Couchbase 自有内嵌依赖保持上游隔离，由审查工具另行识别；不能通过普通 BOM 替换这些包。

主线排除 Apollo 的旧 YAML 解析器和 Remoting 客户端携带的旧独立日志后端。运行日志实现由应用选择，SDK 不随包绑定默认日志后端。

同一进程同一产品默认选择一个原生驱动代际。旧版本通过独立适配或 FactoryAdapter 渐进接入；跨代际共存需要额外的依赖、类加载、SPI 和原生库验证。
