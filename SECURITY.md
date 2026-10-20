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

当前 RocketMQ gRPC 上游 5.2.2 发布 JAR 内嵌了旧 gRPC/Netty，包含 Netty 4.1.79 的原生组件元数据。普通依赖管理覆盖不了重定位进 JAR 的类。相关安全公告包括 [GHSA-prj3-ccx8-p6x4](https://github.com/advisories/GHSA-prj3-ccx8-p6x4)。

因此 `driverkit-rocketmq-grpc` 默认不构建、不纳入主线 BOM，也没有生产可用声明。需要上游发布修复后的构件或完成独立补丁与集群验证；当前状态为**需人工安全评审**。

研究构建可显式使用 `-Pexperimental-grpc`；审查此配置必须重新生成其依赖树，并执行 `python3 tools/audit_dependencies.py --include-experimental`。不能将主线审查结果用于该配置。

## RocketMQ Remoting 传输限制

5.5.1 的 `TlsSystemConfig` 默认开启 TLS 测试模式、关闭客户端验证服务器；`TlsHelper` 对这些配置使用信任全部证书的管理器。仅设置 `setUseTLS(true)` 不能作为安全保证；关闭测试模式也不足以证明完整的主机名校验。

本适配器在解析凭据和发起网络连接前拒绝 `tls=true`，不修改上游进程全局 TLS 参数。`tls=false` 必须由可信部署配置显式选择，并配合经过评审的受保护网络。TLS 部署需提供完成证书链、服务器身份校验及握手失败测试的专用传输适配，当前为**需人工安全评审**。核心 `FactoryAdapter` 可以承接此类实现。

## 接入方复核

配置中心服务端授权、资源目录来源、凭据轮换/撤销、内网出口策略、真实 TLS、服务端资源权限、实际消息幂等与集群级并发语义需要在目标环境复核。此仓库没有引入或验证任何特定企业的内部权限实现。

发现漏洞时请使用 GitHub 的私密漏洞报告功能（仓库启用后）或先联系维护者，不要把凭据或业务数据放入公开 Issue。
