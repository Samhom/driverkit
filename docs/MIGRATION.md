# 迁移与产品扩展

## 先迁移治理

1. 固定现有驱动和服务端版本，记录公开 API、超时、重试和指标基线。
2. 使用 FactoryAdapter 包住现有客户端工厂，明确创建、就绪检查、关闭及线程安全约束。
3. 为不同应用、环境和别名建立 ResourceId，接入已授权的配置订阅。
4. 保持现有业务 API，通过稳定句柄委托调用，校验指标、资源回收和动态切换。
5. 单独升级原生驱动，逐个验证 API 和服务端差异。

核心是 Java 8 库，FactoryAdapter 可在旧应用中使用。当前厂商专用适配器使用 Java 17；旧应用先使用 core 桥接，避免被迫同时升级 JDK 与全部驱动。

## 主要迁移点

| 产品 | 迁移关注点 |
| --- | --- |
| Jedis 3 → 4+ | Binary 类、连接池类型、命令返回值、Pipeline、分片 API 的变化 |
| Jedis 7 → 8 | 客户端类调整、RESP 协商与 TLS 默认值；当前适配器默认保持无需 HELLO 的 RESP2 行为 |
| ES HLRC 7 → Java API Client | 请求模型、序列化、Transport 和异常体系；7.17 兼容模式不是通用跨版本保证 |
| Couchbase 2 → 3 | RxJava/Reactor、Document/Result、Scope/Collection、懒初始化、重试与 Environment 所有权 |
| RocketMQ Remoting 4 → 5 | 生产消费配置、回调、认证、超时与监控挂载点；gRPC 另属一种客户端体系 |

参考上游：[Jedis](https://redis.github.io/jedis/migration-guides/v3-to-v4/)、[ES](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/8.19/migrate-hlrc.html)、[Couchbase](https://docs.couchbase.com/java-sdk/current/project-docs/migrating-sdk-code-to-3.n.html)、[RocketMQ](https://rocketmq.apache.org/docs/sdk/01overview/)。

## 原生会话

Pipeline、事务、Pub/Sub、PIT/Scroll 和流式结果应固定客户端代际直到会话结束。不要缓存已经退出租约范围的原生客户端或其子对象。

原生 Jedis 单连接、事务和 Pipeline 仍有各自的线程安全限制。共享的 SDK 句柄不会改变这些原生约束。普通 RedisClient 的线程安全能力来自其原生连接池。

## 扩展一致性

按产品增加读写分离、就近路由、双写、慢操作和大文档检测。每项扩展应说明它支持的产品、操作、执行模型及失败语义。

双写需要定义主写结果、镜像失败、修复持久化与重放顺序。进程内有界队列不能承担可靠修复日志。跨资源切换也不提供数据迁移事务。

SDK 的并发上限和 deadline 不能替代服务端事务、唯一约束、消费幂等与资源授权。
