# 迁移与产品扩展

## 先迁移治理

1. 固定现有驱动和服务端版本，记录公开 API、超时、重试和指标基线。
2. 使用 FactoryAdapter 包住现有客户端工厂，明确创建、就绪检查、关闭及线程安全约束。
3. 为不同应用、环境和别名建立 ResourceId，接入已授权的配置订阅。
4. 保持现有业务 API，通过稳定句柄委托调用，校验指标、资源回收和动态切换。
5. 单独升级原生驱动，逐个验证 API 和服务端差异。

核心是 Java 8 库，FactoryAdapter 可在旧应用中使用。当前厂商专用适配器使用 Java 17；旧应用先使用 core 桥接，避免被迫同时升级 JDK 与全部驱动。

## 具体代际模块

`-Plegacy-drivers` 构建以下独立迁移模块。它们保留原生请求、结果及异常模型，共用 core 的稳定句柄、版本配置、租约、期限、指标和清理机制。模块使用 Java 17，Java 8 应用继续先使用 core 的 FactoryAdapter。

| 模块 | 编译验证的驱动 | 受管原生对象 | 边界 |
| --- | --- | --- | --- |
| driverkit-redis-jedis3 | Jedis 3.10.0 | JedisPool | 在租约内借还 Jedis；TLS 显式启用 HTTPS 端点识别 |
| driverkit-elasticsearch7 | HLRC 7.17.29 | RestHighLevelClient | 保留 HLRC API，固定单一可信端点，禁重定向；就绪响应缓存 16 MiB |
| driverkit-couchbase2 | Java SDK 2.7.23 | Couchbase2Client | 同时拥有 Bucket/Cluster/Environment；仅默认 scope/collection；TLS 暂拒绝 |
| driverkit-rocketmq4 | Remoting 4.9.8 + ACL 4.9.8 | DefaultMQProducer / DefaultMQPushConsumer | 原生确认/位点；消费者接入 ExclusiveConsumer；TLS 暂拒绝 |

旧原生依赖均为 `provided + optional`，不通过迁移模块传递给应用，也不进入默认 BOM。应用须显式提供表中的驱动，并导入该产品的 `driverkit-redis-jedis3-bom`、`driverkit-elasticsearch7-bom`、`driverkit-couchbase2-bom` 或 `driverkit-rocketmq4-bom`。这些 BOM 共用 `driverkit-legacy-bom` 的安全补丁，分别管理各自的原生代际，避免只迁移 Redis 时改动另一个产品的驱动版本。同一产品的两个代际不能放入同一应用类路径。

legacy BOM 固定 Fastjson `1.2.83_noneautotype`、Log4j `2.26.1`、BeanUtils `1.11.0`、Commons Lang `3.20.0`、SnakeYAML `2.4`；ES 7 / Remoting 4 排除旧 LZ4 并使用维护中的 `at.yawk.lz4`。Remoting 4 适配器仅使用显式 RPC 凭据，不支持其旧 YAML 凭据文件加载 API。原生签名测试验证补丁链接。四个模块也分别确认新代际类不存在，防止运行时选择错误版本。

`driverkit-settings` 只包含不可变配置和 core 依赖，不绑定任何厂商驱动。现有配置类包名及 API 保持一致，主线与旧代际可分别使用它。业务继续按原生代际 API 操作，不把 ES HLRC 请求强转成新 Java API Client 请求，不模拟 SDK 2 的 collections。

例如 Jedis 3 在受管调用中借还连接：

```java
ManagedClient<JedisPool, RedisSettings> client = runtime.register(
    new ConfigSnapshot<>(resource, 1, settings), new Jedis3Adapter(), ClientPolicy.defaults());
String value = client.execute(Operation.read("get", Duration.ofSeconds(2)), (pool, context) -> {
    try (Jedis jedis = pool.getResource()) { return jedis.get(key); }
});
```

Pipeline、事务和长会话使用 `client.session()` 固定代际，并在结束后归还连接。原生 API 扩展点的请求规模、响应限制及业务幂等仍由调用方遵守。

这些模块提供迁移能力，不代表旧厂商版本重新获得上游维护。ES 7.17 已于 2026-01-15 结束支持，Couchbase SDK 2 也已 EOL；旧组件安全风险和目标集群组合**需人工安全评审**。详见 [安全边界](../SECURITY.md#旧代际迁移模块)。

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
