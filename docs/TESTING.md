# 测试与验证

## 默认测试

`mvn verify` 执行核心并发/生命周期、配置订阅、指标、Reactor、Spring Boot 和原生协议测试。协议测试绑定本机回环地址和随机端口，使用运行时生成的临时凭据；无需实际数据库或 Broker。

重点不变量：旧代际在租约结束前保持可用；过期配置不覆盖新配置；失败候选被清理；取消与超时不提前释放物理调用；拒绝请求不执行操作；重建不重叠消费成员；权限撤销不能被新配置恢复。

RESP 测试通过真实 Jedis 连接池和 socket 执行认证、KV 与 Pipeline。HTTP 测试通过真实 Elasticsearch Java API Client 执行认证、CRUD、429 和部分失败 Bulk，并验证过大请求不会发送。

它们不能代替真实集群测试。默认测试还通过现场生成的临时 CA/叶证书验证 Remoting 原生管道的 TLS 握手，包括未知 CA、名称错误、过期和缺少 IP SAN；无硬编码私钥或密码。Couchbase 集群、RocketMQ Broker/Proxy 不由默认单元测试启动。

## Windows

某些 Windows 环境的 AF_UNIX 支持会导致 JDK NIO Pipe 回环连接初始化失败。可显式使用：

```powershell
.\verify.ps1 -WindowsTcpPipe
```

这使测试 JVM 使用 JDK 自带的 TCP Pipe 回退，不改变 SDK 的网络、认证或 TLS 设置。Linux CI 按 JVM 默认配置运行。

## 目标环境测试

### 独立真实服务回归

`Real cluster recovery` CI 在独立 Linux runner 上分别启动 Redis 8.2.3、Elasticsearch 9.5.5、Couchbase Community 7.6.2、RocketMQ 5.5.1。执行 `python3 tools/real_clusters.py redis`（或其他产品名）可复现；要求专用 Linux Docker 环境和空闲的标准服务端口。它先构建 `-Preal-clusters`，再通过 Failsafe 执行 `NativeClustersIT`，缺少 fixture 环境会失败。

每个产品使用真实适配器和同一个受管句柄，验证错误凭据拒绝、容器暂停时调用失败、解除暂停后恢复、容器重启后恢复、旧凭据在新连接上被拒绝及新版本配置轮换后恢复。恢复探测使用有期限的测试轮询，不是 SDK 自动重试承诺。镜像版本固定，成功后记录实际 digest、JDK 和场景至 `target/real-cluster-evidence.json` 并输出至 CI。

容器位于本次专属内部网络，公开到宿主机的端口只绑定 127.0.0.1。密码现场随机生成，配置存放在私有临时目录，不进入仓库或证据。Redis/ES/CB/RocketMQ 启用认证；CB 首次初始化前的本地管理入口用于创建认证，Remoting NameServer 本身无 ACL。测试的明文连接仅限隔离 fixture，不是生产部署示例。Docker 控制先验证本次唯一运行标签，finally 只清理本次创建的容器。

这些是单节点真实服务回归，不能推导出多节点故障转移、TLS Broker/Proxy、消息重平衡或位点恢复已经通过。实际结果以对应提交的 CI 作业为准；编译通过不记为集群通过。

接入方需要补充目标服务端版本矩阵、读写权限拒绝、凭据轮换、服务发现变化、节点切换、连接/读取超时、大请求/响应、部分失败、长事务、PIT/Scroll、重平衡及位点恢复。

性能应与相同版本原生客户端比较，分别测量关闭观测、仅指标和启用采样追踪时的吞吐、P99、分配量、线程、连接及内存。当前仓库不宣称已经完成这些实际负载测试。

Java 8 核心运行检查位于 `.ci/Java8Smoke.java`，CI 在构建后切换到 JDK 8，独立编译并运行它。
