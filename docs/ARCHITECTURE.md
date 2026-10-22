# 架构与运行契约

## 公共边界

公共核心只依赖 JDK。产品适配器负责原生客户端创建、配置映射、就绪检查、异常分类与资源关闭。API 保留产品语义，SDK 的版本号独立于驱动版本号。

`ResourceId(application, environment, alias)` 标识受管资源。该身份来自可信应用装配和平台授权，不承担最终用户认证。数据库/Broker 的身份与授权仍由上游服务端执行。

## 代际与租约

`ManagedClient` 使用一个状态锁协调租约获取、实例发布、退休和撤销，使用独立更新锁串行化候选实例创建。

1. 校验资源域、版本和产品迁移约束。
2. 创建候选实例并执行驱动就绪检查。
3. 在状态锁中发布新实例，旧实例转为退休状态。
4. 等旧租约释放，再在独立清理执行器关闭旧资源。

候选创建失败会清理候选资源，活动实例保持可用。配置创建所使用的凭据提供器必须有界并正确释放其临时材料；适配器须保证原生启动和就绪检查的超时契约。

创建与预热共用一个截止时间，覆盖生命周期队列等待；DriverAdapter 的带 Duration 重载接收剩余预算。生命周期工作在有界后台执行器运行，调用方超时或被中断后不会发布迟到候选。原生驱动若忽略中断，候选所有权仍被占用，返回后自动关闭，防止重复超时导致无限创建。每个运行时最多持有 maxClients × 4 份生命周期所有权；清理失败会隔离其容量，避免未知存活实例不断累积。

配置重建协调、创建/预热和原生清理使用各自独立的有界执行器。创建/预热与清理各有两个工作线程，队列受所有权额度约束；即使两个创建任务都不响应中断，清理仍能继续。退休代际直接异步交给清理执行器，不再绕经配置维护队列，也不会在拒绝执行时改到状态锁内执行原生关闭。

关闭使用同一个绝对截止时间，原生 close 的带 Duration 重载收到剩余预算；截止时请求中断，调用方按期返回。Java 不能强制安全终止不响应中断的第三方代码，此时后台所有权与容量仍保留，实际关闭结束后才回收。配置订阅的创建、迟到结果和解除订阅同样受管，外部平台代码不在客户端状态锁内执行。

只有一个退休代际可存在。它未释放时，后续推送合并为最新快照。长会话可能延迟后续配置生效；应用通过状态信息观察这一点。

## 资源注销与同名重建

关闭句柄会立即关闭准入，所有原生代际、尚未返回的候选和配置订阅实际清理完成后，自动移除注册项并回收 maxClients 额度。ClientRuntime.unregister(resource) 显式发起同样的有界关闭；返回 true 表示捕获到的注册项已释放，false 表示资源不存在或清理仍未完成/失败。可结合原句柄的 status().ownedResources() 和 cleanupFailures() 区分状态。

清理期间资源 ID 仍被占用，禁止同名重建；清理失败则保留注册项隔离，防止未知存活资源被静默覆盖。注册项删除比较句柄对象身份，旧句柄再次 close、旧租约释放和迟到回调均不能删除新句柄。关闭和强制退休后的旧会话不能再次取得原生客户端；已经缓存到 SDK 外部的原生对象仍须由调用方遵守租约契约。

同名重建不撤销旧实例已经发送的请求，也不提供跨实例写入的顺序屏障；结果不确定的旧写入仍需业务幂等机制处理。

Redis 数据库编号、Couchbase Bucket/Scope/Collection、RocketMQ Topic/Group 的迁移需要新资源绑定。连接切换不提供跨数据集迁移、一致性或顺序屏障。

## 超时与取消

调用在有界执行器开始，deadline 包含 SDK 队列等待时间。进入原生调用前检查 deadline，驱动适配调用可使用 `CallContext.remaining()` 继续传递预算。

同步接口也是受管执行器上的调用。返回值超时、Future 取消或调用线程中断，都不能证明服务端没有执行操作。SDK 在底层 CompletionStage 真正结束前继续持有其租约及在途额度。驱动内部等待、事务及协议处理还受各自的超时与取消能力约束。

尚未开始执行的排队请求在超时或取消时移出调用队列，并立即释放租约与在途额度。提交和开始执行与此回收过程互斥，已经回收的请求不会再执行原生操作；已经开始执行的请求仍遵守上述物理租约规则。

已进入原生写调用的外层超时返回 `WRITE_OUTCOME_UNKNOWN`。上层应根据业务幂等机制决定后续处置。核心不增加自动重试。

显式关闭到达排空截止时间后，会完成未结束的 SDK Future 并强制请求适配器释放资源；这仍不能撤销服务端已经发生的效果。

## 完成回调隔离

接受调用前会预留一份完成交付额度。原生结果、超时和关闭结果由独立的有界完成线程池交付，原生完成时先释放物理租约；队列满时不会回退到 I/O 或计时线程执行用户回调。额度覆盖待交付结果和正在执行的同步回调，耗尽时新调用在原生执行前返回 OVERLOADED。显式由调用方执行的 Future.cancel 保持即时取消语义，其同步回调由该调用方承担。

可使用 ClientRuntime 的扩展构造器设置 callbackThreads 和 callbackCapacity。默认线程数为 min(4, callThreads)，容量为 max(64, min(100000, queuedCalls + callThreads))。单个慢回调不占住原生请求额度；如果所有完成线程被用户代码阻塞，交付会延迟，并对新调用施加背压。回调应使用异步组合或应用自己的执行器；在完成回调内调用 SDK 同步 execute 会快速拒绝，避免线程池自等待。

关闭不会丢弃已接纳结果；完成线程池在所有已预留交付结束后退出。关闭等待使用同一个截止时间，不强行终止用户代码。

## 消费组

消费者使用 `ExclusiveConsumer`，与普通连接客户端分别管理。回调入口关闭后返回原生重试结果；已有回调保留其业务执行时间，排空完成后才关闭旧成员。

重建失败尝试重新创建上一个有效配置。源和目标 Group/Topic 必须一致。权限撤销不可由回退或配置更新恢复。业务幂等、位点语义和消息顺序由 Broker、原生客户端及业务逻辑共同保证。

消费者启动、关闭和控制操作也受生命周期期限约束。新成员启动超时后，必须确认该候选实际关闭，才允许恢复旧配置；否则进入 FAILED，保持业务回调入口关闭，防止迟到启动与恢复成员重叠。

## 观测与扩展

Observer 接收不含载荷的逻辑操作元数据；每次调用或订阅仅完成一次观测。Micrometer 观测器限制身份数，超限后汇聚到固定 overflow 序列。OpenTelemetry 的应用采样与导出配置由应用提供。

AdmissionPolicy 是快速本地准入扩展；内置按资源配置的令牌桶。DriverAdapter、ConsumerAdapter、ConfigurationSource、CredentialsProvider、Observer 为公开扩展点。配置只选择应用预先注册的实现，不加载远端代码或执行表达式。

原生请求、资源池和逻辑业务调用有独立观测层次。当前核心不推测原生内部重试次数。产品特有路由、双写、事务或降级应提供明确的操作与一致性契约后接入。

## 类型化能力契约

`DriverAdapter.capabilities()` 返回不可变 `AdapterCapabilities`，通过枚举区分 `MANAGED`、`NATIVE_SESSION_ONLY` 和 `UNSUPPORTED`。`ManagedClient.capabilities()` 暴露当前适配器契约；调用方可用 `requireManaged` 在执行前拒绝未支持能力。该签名替代快照版本的字符串集合。

`ASYNC` 表示原生异步阶段由受管句柄持有租约，不占用调用线程等待结果。同步 API 放入线程池不算原生异步。事务、流式结果、原生取消没有受管实现和验证证据，当前统一返回不支持；原生库本身具有这些方法不构成 SDK 支持承诺。取消受管结果只取消结果交付，已发出的原生工作继续持有租约直到完成。

`CapabilityContractTest` 验证异步释放调用线程、取消后租约保留及迟到完成回收，并验证事务、流式结果和原生取消默认拒绝。产品模块的协议测试验证具体原生调用。

## Elasticsearch 原生异步

`ManagedSearch` 的 `getAsync/indexAsync/deleteAsync/bulkAsync/searchAsync` 直接返回受管 Future。同步方法使用 `ManagedClient.executeAwait`，只阻塞调用者，不让调用线程池等待 HTTP 响应。异步视图通过原生 `ElasticsearchAsyncClient` 共享代际 Transport 及认证、响应大小和禁止重试配置；视图不单独关闭。批量验证和逐项错误保持原语义。

原生异步共享 Transport 的依据：[Elastic 官方说明](https://www.elastic.co/docs/reference/elasticsearch/clients/java/api-conventions/blocking-async)。HTTP 协议测试以单个调用线程验证慢请求不阻塞后续请求，并验证切换代际、取消结果后旧 Transport 仍持有到真实完成。

## 调用剩余预算

`CallContext.remainingMillis(ceiling)` 统一使用单调时钟，并向上取整到至少 1ms，过期时直接拒绝，防止原生 API 把 0 解释为无限等待。Redis 门面按每次调用的预算借连接，不修改共享池配置；连接新建时使用线程局部预算限制连接与认证读取，命令读取再次扣除借连接时间，归还前恢复超时，坏连接作废。预算线程局部值在 finally 中恢复。

ES 每次同步或异步调用复制 TransportOptions，保留认证、响应上限和禁止重试，限制连接池等待、请求连接和响应超时。动态设置与门面初始上限取更小值。协议测试验证池耗尽及不返回响应时，物理租约在短预算附近回收。

预算是原生 API 可表达的阶段上限；JVM DNS、TLS 提供者、操作系统阻塞写入及原生会话自定义代码不保证精确中断。各阶段可能存在毫秒取整和调度延迟，超时不代表写入回滚。外层截止仍负责结果交付，真实完成之前仍保留租约。旧版迁移适配器只保证初始化预算，旧版原生会话须由调用者传递操作预算。

Rest5 9.5.5 的 `RequestContext` 创建 HttpClientContext 时遗失 RequestOptions 中的 RequestConfig。`driverkit-elasticsearch-transport` 使用官方二进制和源码 SHA-256 固定输入，只重新编译 Rest5Client，将 RequestConfig 复制到实际 HTTP 上下文；适配器排除原包。部署时不可再次引入原始同名类的 JAR。升级原生驱动必须重新审查补丁和协议测试。

## 上下文传播

核心使用 `ContextPropagation` 捕获不可变快照并在每次进入线程时打开/恢复作用域。`ClientRuntime` 的八参数构造器接受扩展传播器，默认仅传播 `CallContexts` 的受信任服务端元数据；调用方可通过 `CallContexts.open(Map)` 建立请求作用域，最多 32 个字段、每值 512 字符，且不允许换行。此机制不验证身份或替代资源授权，不得直接把客户端自报身份 Header 填入后当作鉴权凭据。元数据不自动写入日志或指标。

追踪模块提供 `OpenTelemetryContextPropagation` 和显式 MDC 字段白名单 `MdcContextPropagation(Set)`，可用 `ContextPropagation.combine` 组合并传给运行时。MDC 需要应用自身的 SLF4J 提供者；SDK 不安装日志后端，SLF4J API 为可选依赖。捕获、调用工作、原生完成处理、结果交付及同步取消均有作用域；线程原有值在 finally 中恢复。原生 API 内业务自行注册的回调或额外执行器须通过 `CallContext.wrap` 包装，SDK 无法接管任意第三方线程。

Reactor 在请求作用域中用 `.contextWrite(ReactorCalls.context(propagation))` 捕获快照，订阅时恢复再捕获到受管调用；`ReactorCalls.mono` 将订阅者 Reactor Context 继续传给原生 Mono。无需安装全局 Hooks。额外 `publishOn` 后的业务执行器仍应使用对应框架的上下文桥接。

测试覆盖成功、错误、超时、取消、线程复用、传播器打开失败回滚、原生迟到完成和 Reactor 调度切换。

## 异常及重试边界

`ClientException.Kind` 进一步区分 `AUTHENTICATION`、`AUTHORIZATION`、`RATE_LIMITED`、本地容量 `OVERLOADED`、`CONNECTION_FAILURE`、`TLS_FAILURE`、`SERVER_REJECTED` 和 `WRITE_OUTCOME_UNKNOWN`。限流策略及 ES 429 改为 `RATE_LIMITED`；调用方应更新原有 OVERLOADED 分支。分类器最多检查 32 层原因并识别循环，保留原生 cause，SDK 顶层消息和观测事件不复制原生错误文本。应用如输出整个堆栈，仍须自行处理 cause 中的查询和标识符。

`failure.retryAdvice(operation)` 只给建议，不发起重试：明确只读且遇到暂时错误返回 `RETRY_READ_WITH_BACKOFF`，认证/权限/证书/请求错误不重试，结果不确定返回 `RECONCILE_WRITE`。所有写操作均不会得到自动重放建议。应用必须在同一个总截止时间内限制尝试次数、指数退避和抖动；`Operation.read` 必须真正无副作用，SDK 不解析 SQL/查询来证明它只读。写入结果不确定时先用业务幂等键或查询核对，不能把异常解释为回滚。

Redis 使用原生 ACL 异常和标准协议错误前缀，连接原因区分连接失败与读超时；ES 401/403/429 分别分类，5xx 对写入保守视为结果不确定；Couchbase 保留 unambiguous timeout 与 ambiguous/durability-ambiguous timeout 的差别。Remoting 的 NO_PERMISSION 无法可靠区分认证与权限，统一归权限拒绝；5 代 USER_NOT_EXIST 可归认证失败，刷盘/副本超时归写入结果不确定。gRPC 支持原生类型及 Status，实验模块隔离保持不变。旧代模块在各自依赖图中提供对应分类。

SDK 核心不自动重放。Redis 单节点不加应用重试；ES 同时关闭 HTTP 和 Java Client 重试；Remoting 发送重试为 0，gRPC maxAttempts=1。Couchbase 受管 KV 门面通过公开 RetryStrategy 返回 noRetry，避免普通暂时故障重放；原生 mandatory/alwaysRetry 路由恢复仍可能发生，受剩余预算限制。原生会话和旧版 Couchbase 保留厂商默认语义，应用应检查原生配置，避免再叠加无界 retry/retryWhen。参考 [Couchbase 官方错误与重试说明](https://docs.couchbase.com/java-sdk/current/howtos/error-handling.html)。

Couchbase 代际发布前还须确认配置中的目标 Bucket 已就绪。settings-aware `DriverAdapter.warmup(client, settings, remaining)` 默认兼容旧 warmup，实现可在同一初始化截止内验证实际资源；Couchbase 先等待 Cluster，再扣除已消耗时间等待 Bucket 的 KV 服务（受管门面不依赖 Views/Query；原生会话使用其他服务时须自行检查），避免 noRetry 下首请求遇到 BUCKET_OPEN_IN_PROGRESS。该行为由真实服务门面场景覆盖。
