# 安全边界与依赖审查

## 主线控制

- 资源身份由可信应用装配提供；配置快照不能改变应用、环境或别名。
- 配置中心、资源目录和凭据服务应认证访问并在服务端执行具体资源授权。
- Endpoint 只接收已授权资源目录的地址。SDK 不是任意 URL 下载或代理接口；HTTP 重定向关闭。
- Redis、ES、Couchbase TLS 保留驱动正常的证书链及主机名验证。生产接入须配置正确的信任和最小权限身份。
- 凭据通过提供器取得，短期材料可关闭；驱动可能保留自己的必要副本。配置和指标不包含凭据。
- Properties 文档最多 64 KiB，执行字段白名单、重复字段和正数版本检查。没有动态脚本或不可信反序列化入口。
- 执行器、队列、客户端、在途操作、配置候选和退休代际均有上限；产品门面限制请求规模。
- ES Bulk 限制条数、单项序列化大小及 16 MiB 总预算，HTTP 响应缓存限制为 16 MiB。
- 写调用超时按可能已生效处理；SDK 不盲目重试，取消也不会提前回收物理在途额度。
- 观测标签有基数限制，不采集查询、消息体、Key 和异常正文。

原生会话与自定义适配器是可信应用代码扩展点，其原生操作限制、线程安全、就绪及关闭超时契约需要相应实现者验证。

## 审查工具

```bash
mvn -B -ntp -s .mvn/settings.xml -Dmaven.repo.local=.m2-cache package -DskipTests org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree -DoutputType=json -DoutputFile=target/dependencies.json
python3 tools/audit_dependencies.py
```

工具只把公开 Maven 包名和版本发送到 OSV。它也检查解析到的 JAR 中的 pom.properties 与 Netty 版本元数据，以避免普通依赖树漏掉内嵌组件。元数据识别和公开漏洞库不是完整的供应链或可利用性证明。

主线依赖已锁定，排除了未使用的旧 YAML 解析器和旧日志后端。已知补丁版本需要随上游安全公告维护。完整应用可能引入其他 BOM 或覆盖版本，发布应用前应再次解析及审查其实际依赖组合。

## gRPC 试验模块

`driverkit-rocketmq-grpc` 改用官方 `rocketmq-client-java-noshade:5.2.2`，不再使用内嵌旧依赖的 fat JAR。固定 SHA-256 的二进制/源码输入重建 `RpcClientImpl`，移除 trust-all TLS，将入站消息限制为 16 MiB，并适配现代 gRPC 的解析器 scheme。域名按原始 authority 校验；IP 通道使用目标 IP 验证证书 IP SAN，TLS 多 IP 共用通道因无法逐一绑定身份而明确拒绝。

实际依赖由 gRPC/OTel BOM 与固定的 Protobuf 4.36.2、Guava 33.7.2、Zstd 1.5.7-24、LZ4 1.12 管理，不携带日志后端。独立 CI 执行 `-Pexperimental-grpc` 构建、认证拒绝、原生通道重连、未知 CA 拒绝与内嵌组件审查。应用不可再引入旧 fat JAR 或原版 noshade，否则产生同名类歧义。

模块仍默认隔离，不进入主线 BOM。受管消费者的真实 Proxy 认证、重平衡、位点和恢复需要真实集群回归后才能获得生产支持；当前为**需人工安全评审**。执行 `python3 tools/audit_dependencies.py --include-experimental` 前必须重新生成相同配置的依赖清单。

## RocketMQ Remoting 传输限制

5.5.1 的 `TlsSystemConfig` 默认开启 TLS 测试模式、关闭客户端验证服务器；`TlsHelper` 对这些配置使用信任全部证书的管理器。仅设置 `setUseTLS(true)` 不能作为安全保证；关闭测试模式也不足以证明完整的主机名校验。

`driverkit-rocketmq-transport` 使用官方 5.5.1 二进制与源码的固定 SHA-256 重建 Remoting。构建保留原生类，仅重新编译 `NettyRemotingClient` 并替换 TLS 初始化和 DNS 解析前的目标属性传递；不使用反射，不修改全局上游 TLS 参数。构建输入改变即失败，升级须重新审查。补丁依照 JDK 信任库验证证书链，按 NameServer/Broker 各自原始目标执行 HTTPS 身份验证，允许 TLS 1.2/1.3，握手期限受连接超时限制。初始化失败直接拒绝；没有降级为明文的路径。

适配器排除原版 `rocketmq-remoting`，应用不得再从其他依赖引入该原版构件，否则同名类会产生类路径歧义。私有 CA 由部署配置 JDK truststore；补丁不读取上游的 `tls.test.mode.enable`、`tls.client.authServer` 等不安全开关，也暂不支持客户端证书。回环真实握手测试覆盖可信链、未知 CA、错误 DNS、缺少 IP SAN 与过期证书。真实 Broker TLS、客户端证书及目标网络策略仍需目标环境验证。`tls=false` 必须由可信配置显式选择并使用受保护网络。

## 旧代际迁移模块

`legacy-drivers` 默认关闭，Jedis 3 / HLRC 7 / Couchbase SDK 2 / Remoting 4 的原生依赖仅用于隔离编译，声明为 `provided + optional`；应用必须显式提供并审查旧驱动。主线 BOM 不传递这些迁移模块。它们不会修补或恢复已 EOL 厂商版本的长期支持。

按产品提供独立 BOM，共用 legacy 安全补丁；它们不会同时替换所有产品的原生代际。已修复可替换的 Fastjson、日志、反射工具、压缩和 YAML 依赖，构建与发布 BOM 的版本一致性及原生依赖隔离由 CI 校验。扫描器拒绝旧于 POM 的依赖清单，避免使用失败构建留下的旧结果。

ES 7.17 的支持已于 [2026-01-15 结束](https://www.elastic.co/support/eol)，[Couchbase 文档](https://docs.couchbase.com/java-sdk/3.12/howtos/encrypting-using-sdk.html)也明确 SDK 2 已 EOL。旧嵌入组件不能用普通 BOM 升级。Couchbase 2 和 Remoting 4 的未经验证 TLS 路径在解析凭据前被拒绝；仅明确选择的受保护网络可用于迁移，TLS 优先迁往主线适配器。

2026-10-21 的独立扫描剩余 20 个风险组件坐标，均来自旧 Couchbase 组件的 Netty 4.1.73、Jackson 2.12.6、Snappy 0.4 元数据；普通 BOM 无法替换这些重定位内容。完整发现保存在 [legacy-dependency-audit.json](docs/legacy-dependency-audit.json)，报告明确 `production_approved=false`。主线及实验 gRPC 的独立扫描没有这些旧构件。

审查旧配置须重新生成 `-Plegacy-drivers` 的依赖树并执行 `python3 tools/audit_dependencies.py --include-legacy`。该命令报告全部发现并返回失败，不用允许列表隐藏漏洞，也不能以主线审查通过替代旧应用审查。旧代际生产接入当前均**需人工安全评审**。

## 接入方复核

配置中心服务端授权、资源目录来源、凭据轮换/撤销、内网出口策略、真实 TLS、服务端资源权限、实际消息幂等与集群级并发语义需要在目标环境复核。此仓库没有引入或验证任何特定企业的内部权限实现。

发现漏洞时请使用 GitHub 的私密漏洞报告功能（仓库启用后）或先联系维护者，不要把凭据或业务数据放入公开 Issue。

Guava is managed at 33.7.2-jre in the build and published BOMs. The October 27 advisory update GHSA-xxph-c9ww-hj94 / CVE-2026-102554 covers earlier native-deserialization implementations; the SDK does not expose Java object deserialization, and retains the dependency audit rather than suppressing this finding. Reference: https://github.com/advisories/GHSA-xxph-c9ww-hj94

Couchbase core-io 3.13.0 embeds vulnerable Guava 33.4.0. driverkit-couchbase-core pins the official core binary and replaces the embedded relocated component with official Guava 33.7.2-jre via Maven Shade. The adapter excludes the original core-io JAR; applications must not reintroduce duplicate core classes. The audit inspects built SDK JAR metadata as well as external dependencies, so embedded components in rebuilds remain checked. Native API version remains 3.13.0; OSGi bundle deployment is not supported by this rebuild.
