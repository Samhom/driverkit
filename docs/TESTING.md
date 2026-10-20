# 测试与验证

## 默认测试

`mvn verify` 执行核心并发/生命周期、配置订阅、指标、Reactor、Spring Boot 和原生协议测试。协议测试绑定本机回环地址和随机端口，使用运行时生成的临时凭据；无需实际数据库或 Broker。

重点不变量：旧代际在租约结束前保持可用；过期配置不覆盖新配置；失败候选被清理；取消与超时不提前释放物理调用；拒绝请求不执行操作；重建不重叠消费成员；权限撤销不能被新配置恢复。

RESP 测试通过真实 Jedis 连接池和 socket 执行认证、KV 与 Pipeline。HTTP 测试通过真实 Elasticsearch Java API Client 执行认证、CRUD、429 和部分失败 Bulk，并验证过大请求不会发送。

它们不能代替真实集群测试。Couchbase 集群、RocketMQ Broker/Proxy、真实 TLS 和认证服务不由默认单元测试启动。

## Windows

某些 Windows 环境的 AF_UNIX 支持会导致 JDK NIO Pipe 回环连接初始化失败。可显式使用：

```powershell
.\verify.ps1 -WindowsTcpPipe
```

这使测试 JVM 使用 JDK 自带的 TCP Pipe 回退，不改变 SDK 的网络、认证或 TLS 设置。Linux CI 按 JVM 默认配置运行。

## 目标环境测试

接入方需要补充目标服务端版本矩阵、读写权限拒绝、凭据轮换、服务发现变化、节点切换、连接/读取超时、大请求/响应、部分失败、长事务、PIT/Scroll、重平衡及位点恢复。

性能应与相同版本原生客户端比较，分别测量关闭观测、仅指标和启用采样追踪时的吞吐、P99、分配量、线程、连接及内存。当前仓库不宣称已经完成这些实际负载测试。

Java 8 核心运行检查位于 `.ci/Java8Smoke.java`，CI 在构建后切换到 JDK 8，独立编译并运行它。
