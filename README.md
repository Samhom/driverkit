# DriverKit

面向 Java 的数据库与消息客户端治理 SDK。公共运行时管理配置版本、客户端生命周期、并发、截止时间与观测；Redis、Elasticsearch、Couchbase、RocketMQ 保留各自的操作语义。

这是 `0.1.0-SNAPSHOT` 开发版本，尚未发布到 Maven Central。请先构建并安装到本地 Maven 仓库。生产接入前，需要针对实际服务端、权限和负载完成联调。

## 已实现

- 稳定客户端句柄、不可变配置快照、单调递增版本及资源身份校验。
- 候选实例创建与预热、原子切换、在途操作排空、失败候选清理。
- 有界调用线程池、等待队列、客户端数、在途操作数及本地令牌桶限流。
- 同步、Future 和冷 Mono 调用；超时或取消后保留尚未结束的原生请求租约。
- 写入结果不确定的错误分类；公共层不自动重放原生请求。
- Apollo 单文档配置订阅，以及可接入其他平台的推送配置 SPI。
- Micrometer 指标、OpenTelemetry span、可组合观测器。
- Redis、ES、Couchbase、RocketMQ Remoting 适配器与常用操作门面。
- RocketMQ 消费者的暂停接收、排空、独占重建和失败恢复。
- Spring Boot 自动配置、统一 BOM、依赖及内嵌组件漏洞审查工具。

## 模块

| 模块 | 作用 |
| --- | --- |
| `driverkit-core` | Java 8 公共运行时、SPI、配置、限流、独占消费者控制器 |
| `driverkit-config-apollo` | 已授权 Apollo `Config` 对象上的单文档订阅 |
| `driverkit-observe-micrometer` | 使用应用提供的 MeterRegistry，限制指标标签基数 |
| `driverkit-observe-opentelemetry` | 使用应用提供的 OpenTelemetry，省略查询及凭据 |
| `driverkit-reactor` | 每次订阅创建一次受管调用的 Mono 桥接 |
| `driverkit-redis-jedis` | Jedis 8，常用 KV API、原生 Pipeline/事务会话 |
| `driverkit-elasticsearch` | Java API Client 9，文档操作、搜索、受限 Bulk |
| `driverkit-couchbase` | Java SDK 3，异步 KV、CAS 和原生 API |
| `driverkit-rocketmq` | Remoting 5，异步生产、受管 PushConsumer |
| `driverkit-spring-boot` | Spring Boot 3.5 自动配置与应用生命周期集成 |
| `driverkit-examples` | 无外部服务即可运行的生命周期示例 |
| `driverkit-bom` | SDK 模块及安全修复后的依赖组合 |

`driverkit-rocketmq-grpc` 位于独立的 `experimental-grpc` 配置中，默认不构建、不纳入正式 BOM。当前上游发布物内嵌旧组件，需人工安全评审；详见 [SECURITY.md](SECURITY.md)。

Remoting 适配器当前拒绝 `tls=true`：上游 TLS 默认设置不能保证服务器身份校验。`tls=false` 仅适用于接入方明确配置的受保护网络；需要 TLS 的部署应使用经过安全评审的传输适配实现，不能直接启用上游测试模式。

## 构建

构建需要 JDK 17+、Maven 3.9+；公共核心使用 `--release 8` 编译，其余当前适配器及集成模块使用 Java 17。

```bash
mvn -B -ntp -s .mvn/settings.xml clean verify
mvn -B -ntp -s .mvn/settings.xml install
```

运行离线示例：

```bash
java -cp driverkit-examples/target/driverkit-examples-0.1.0-SNAPSHOT.jar:driverkit-core/target/driverkit-core-0.1.0-SNAPSHOT.jar io.github.samhom.driverkit.examples.OfflineDemo
```

Windows classpath 分隔符为 `;`。也可以使用 [verify.ps1](verify.ps1)；本机 Windows Unix-domain Pipe 不可用时可显式传入 `-WindowsTcpPipe`，该选项仅作用于测试 JVM。

## 使用 Redis

应用应导入 BOM，保留经过验证的安全补丁版本。应用自己的依赖管理仍可能覆盖这些版本，集成后需要重新解析并审查依赖树。

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.samhom.driverkit</groupId>
      <artifactId>driverkit-bom</artifactId>
      <version>0.1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
<dependencies>
  <dependency>
    <groupId>io.github.samhom.driverkit</groupId>
    <artifactId>driverkit-redis-jedis</artifactId>
  </dependency>
</dependencies>
```

```java
ResourceId resource = new ResourceId("orders", "production", "order-cache");
RedisSettings settings = RedisSettings.defaults(
    new Endpoint("cache.example.org", 6380, true),
    CredentialsProvider.environment("REDIS_USERNAME", "REDIS_PASSWORD"));

try (ClientRuntime runtime = new ClientRuntime()) {
    ManagedClient<RedisClient, RedisSettings> handle = runtime.register(
        new ConfigSnapshot<>(resource, 1, settings),
        new JedisAdapter(), ClientPolicy.defaults());
    ManagedRedis redis = new ManagedRedis(handle, Duration.ofSeconds(2), 1024 * 1024);
    redis.set("order:example", "value", Duration.ofMinutes(5));
    String value = redis.get("order:example");
}
```

Endpoint 必须来自应用已授权的资源目录；HTTP 请求参数不能直接成为客户端地址、凭据引用或资源身份。`tls=true` 使用驱动正常的证书链及主机名校验，信任配置由应用和平台管理。

## 动态配置

```java
PushConfigurationSource<RedisSettings> source =
    new PushConfigurationSource<>(new ConfigSnapshot<>(resource, 1, settings));
handle.bind(source);

// 使用更高版本发布一个完整、经过授权校验的新快照。
source.publish(new ConfigSnapshot<>(resource, 2, replacementSettings));
```

业务继续持有原来的 `handle` 或门面。新实例预热成功后才发布；旧实例等已有操作结束后再关闭。持续变更合并到最新待处理快照，最多保留一个活动代际、一个退休代际及一个候选实例。

`handle.status()` 可查看生效版本、已尝试版本、被拒绝更新数、在途操作及退休代际数。回滚需要用更高版本重新发布先前的配置内容。

单独关闭 handle 后，注册项会在原生客户端、候选及配置订阅实际清理完成后自动释放。也可调用 runtime.unregister(resource) 显式注销；清理尚未完成或失败时，资源 ID 继续保留，不能同名重建。status().ownedResources() 与 cleanupFailures() 可用于诊断；旧句柄不会影响之后重新注册的同名资源。

Apollo 集成接收应用已经初始化并授权的 `Config`：

```java
PropertiesSnapshotDecoder<RedisSettings> decoder = new PropertiesSnapshotDecoder<>(
    resource, Set.of("host", "port"), values -> RedisSettings.defaults(
        new Endpoint(values.get("host"), Integer.parseInt(values.get("port")), true),
        CredentialsProvider.environment("REDIS_USERNAME", "REDIS_PASSWORD")));
ApolloConfigurationSource<RedisSettings> source =
    new ApolloConfigurationSource<>(apolloConfig, "driverkit.order-cache", decoder);
handle.bind(source);
```

Apollo 属性 `driverkit.order-cache` 的值是一份原子发布的 Properties 文档：

```properties
revision=2
host=cache.example.org
port=6380
```

应用、环境和资源别名绑定在订阅代码中。文档不能改变它们。凭据不放入文档。该集成只读取单属性文档，已排除上游旧 YAML 解析器；不提供 YAML 命名空间集成。

## 指标与追踪

```java
Observer observations = new CompositeObserver(
    new MicrometerObserver(applicationMeterRegistry),
    new OpenTelemetryObserver(applicationOpenTelemetry));
ClientRuntime runtime = new ClientRuntime(128, 16, 256, observations);
```

逻辑调用产生 `driverkit.client.operations` 和 `driverkit.client.operation.duration`。标签包括应用、环境、资源、驱动、操作和结果；版本、Key、消息 ID、查询、异常正文不会成为指标标签。

这些指标表示 SDK 逻辑调用。原生驱动内部的网络尝试、服务端延迟、消息重投及连接池细节有不同统计口径，需要驱动/服务端原生指标配合；SDK 不把一次逻辑调用误报成多次业务请求。

## 消费者

```java
RocketMqConsumerAdapter adapter = new RocketMqConsumerAdapter((messages, context) -> {
    // 完成业务事务及幂等检查后返回成功；失败由原生 SDK 及 Broker 重投。
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
try (ExclusiveConsumer<DefaultMQPushConsumer, RocketMqSettings> consumer =
    new ExclusiveConsumer<>(snapshot, adapter, ClientPolicy.defaults(), observations)) {
    consumer.bind(configurationSource);
}
```

重建先关闭业务回调入口，再排空、关闭旧消费成员并创建新成员。排空超时会恢复旧成员接收；新成员启动失败会尝试恢复先前配置。权限撤销会锁定入口，配置更新不能撤销这个状态。

消息仍具有上游的至少一次交付语义，业务处理需要幂等。当前受管 Remoting 消费者封装并发消费，顺序消费等能力通过产品专用适配实现。

## 高级操作与旧版本接入

普通操作通过 `execute` / `executeAsync` 获取统一并发、deadline、观测和生命周期。事务、Pipeline、订阅或流式结果使用 `ClientSession` 固定代际，并在实际使用结束后关闭。

`FactoryAdapter` 可承接应用现有驱动工厂，核心不链接厂商类型。它适合先迁移配置与治理，再升级驱动。旧驱动兼容性、线程安全、预热及关闭保证由对应工厂适配实现并验证；见 [迁移说明](docs/MIGRATION.md)。

原生访问需要遵守租约范围，保留驱动的线程安全及事务约束。高级原生操作的请求限制、独立命令埋点和重试语义由其调用代码负责。

更多说明：[架构](docs/ARCHITECTURE.md)、[兼容性](docs/COMPATIBILITY.md)、[测试](docs/TESTING.md)、[安全](SECURITY.md)。

License: [Apache-2.0](LICENSE).
