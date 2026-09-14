<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind · 锐风" width="128">

# RushWind · 锐风

[English](./README_en.md) | **中文** | [日本語](./README_ja.md)

</div>

---

## 设计哲学

> **不是全家桶，而是积木盒。**

RushWind 只做一件事：**可靠的多服务器生命周期编排**。核心定义契约——传输 trait、停机信号、实例模型——每一个具体协议栈都是独立的适配器 crate，由使用者按需拼装。核心不含日志、不含注册中心、不含配置中心：那些是积木，不是底板。

对照 Go 前作 [go-wind](https://github.com/tx7do/go-wind)（同一哲学的 Go 表达）：RushWind 不是移植，而是按 Rust 的所有权、取消与错误模型重新表达的实现。语义差异清单见 [docs/architecture.md](./docs/architecture.md)。数据访问层走同一套路：[go-crud](https://github.com/tx7do/go-crud) 的「一套 Repository 契约驾驭多种存储引擎」被重新表达为 `rushwind-storage` 契约 + 每引擎一个适配器 crate。

## 当前状态

**P0（骨架）**：生命周期核心、传输契约、跨适配器一致性测试套件已就位并通过全套验证。适配器按路线图推进：

| 阶段 | 内容 |
|:---|:---|
| P0 | 生命周期核心、传输契约、一致性套件 |
| P1 | `rushwind-transport-axum`（管理面/API）、`rushwind-transport-ws`（会话中间件链：门链 + 准入策略 + 会话停机总线，见 [session-middleware.md](./docs/session-middleware.md)） |
| P2 | `rushwind-transport-quic`（裸 QUIC 会话 + 全套会话链：门/refuse、原子准入、握手截止）；`rushwind-transport-mqtt`（外部 broker 消费桥）；注册-only registry 薄片（`rushwind-registry` + etcd 适配器，线格式与 go-wind 字节对齐）——全部已交付 |
| storage | `rushwind-storage`（契约）+ 四引擎：内存参考、SeaORM（SQLite/PostgreSQL/MySQL）、MongoDB；横切层 `rushwind-storage-cache` / `rushwind-storage-soft-delete` / `rushwind-storage-observe`（透明装饰器）；算法积木 `rushwind-storage-tree`（树形遍历）；`rushwind-storage-proto`（proto 定义契约 + protojson + AIP 文本语法）；`rushwind-storage-macros`（DTO 映射 derive）；对位前作 [go-crud](https://github.com/tx7do/go-crud) |
| auth | `rushwind-authn` / `rushwind-authz`（契约）+ 引擎矩阵：认证七引擎（apikey / basicauth / hmac / jwt / noop / presharedkey / session）、鉴权三引擎（acl / rbac / noop）；`AuthenticationGate` 把认证引擎接入会话门链，契约见 [docs/security-authn-authz.md](./docs/security-authn-authz.md) |
| config | `rushwind-config`（契约：`Source` trait + 能力默认方法 + `FallbackSource` 优先级合成与变更流合并）+ 双引擎：env（环境变量 + 前缀）、file（单文件 + 父目录监视，突发合并 + 陈旧值抑制）；对位前作 `go-wind-plugins/config` |
| metrics | `rushwind-metrics`（契约：`Metrics` trait——counter/histogram/gauge，标签规范化排序）+ 三引擎：prometheus（拉：注册表懒注册 + 文本格式暴露）、otel（推：OTLP gRPC/HTTP 导出）、datadog（推：手写 DogStatsD over UDP + 批量缓冲）；对位前作 `go-wind-plugins/metrics` |
| script | `rushwind-script`（契约：`ScriptEngine` 生命周期核心 + 独立能力 trait 族，probe 方法对位 Go `As*` 断言，`FullEngine` 聚合毯实现；`ScriptValue` 数据桥；名称键工厂注册表、固定与自动扩容引擎池、`Manager`；本地源框架层：memory、file（mtime 轮询监视）、静态树 + 前缀、双策略多源聚合、TTL 与失效监视缓存、变换链）；对位前作 `go-scripts` |
| http | `rushwind-http`（HTTP 边缘：gRPC 对齐的错误信封 `HttpError`——code/reason/message/details，`AuthnError`/`StorageError` 内置转换；请求中间件栈 recovery / request-id / logging / CORS / timeout 与 `HttpEdge` 装配器；`with_authn` / `with_authorization` 把认证鉴权契约接上 axum 路由，`Authenticated` 提取器；feature 门控的 `/healthz`+`/readyz` 与 `/metrics` 挂载）；对位前作 `go-wind-plugins/transport/http/middleware`，设计见 [docs/http-edge.md](./docs/http-edge.md) |
| P3 | `rushwind-bootstrap`（serde YAML 配置驱动装配：存储工厂 + 路由包 + 服务器工厂三注册表）——已交付 |
| 后续 | h3/webtransport 叠加、mqtt handler 注册、kcp（存量互操作时）、rushwind-protocols 独立仓 |

## 仓库布局

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-core` | 生命周期编排：并发启动、级联停机、分阶段限时、结果观察 |
| `crates/rushwind-transport` | 契约层：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum 适配器：`Router` 接入生命周期，优雅停机映射见架构文档 |
| `crates/rushwind-transport-ws` | WS 会话路由构建器：门链 + 准入策略 + 会话停机总线，契约见会话中间件文档 |
| `crates/rushwind-transport-quic` | QUIC 适配器：quinn 接受循环接入生命周期，全套会话链；`stop()` 为真实释放（Endpoint::close） |
| `crates/rushwind-transport-mqtt` | MQTT 消费桥：订阅外部 broker，重连退避 + 订阅重建，串行泵入 handler |
| `crates/rushwind-http` | HTTP 边缘：错误信封 + 请求中间件栈（recovery / request-id / logging / CORS / timeout）+ 认证鉴权桥 + 健康/指标挂载，见 [docs/http-edge.md](./docs/http-edge.md) |
| `crates/rushwind-bootstrap` | 配置驱动装配：YAML → 存储引擎 + HTTP 服务器 + 路由包，汇入单一生命周期 |
| `crates/rushwind-registry` | 注册-only registry 契约：`Registrar` trait + 与 go-wind 字节对齐的键布局/线格式（golden 钉死） |
| `crates/rushwind-registry-etcd` | etcd 适配器：租约 TTL + 自愈 keepalive，句柄 Drop 回退过期；live 套件（CI etcd 容器）钉死互操作 |
| `crates/rushwind-authn` | 认证契约：`Authenticator` trait（提取/验证两半）、`AuthClaims` 声明包、错误分类学、`AuthenticationGate` 门链接入层，见 [docs/security-authn-authz.md](./docs/security-authn-authz.md) |
| `crates/rushwind-authn-apikey` | API-key 引擎：静态 key 集 / 每键 claims / 验证回调 |
| `crates/rushwind-authn-basicauth` | Basic-Auth 引擎：RFC 7617 凭证对静态用户表或验证回调 |
| `crates/rushwind-authn-hmac` | HMAC 引擎：keyID.timestamp.signature 签名校验，时钟偏移窗口 |
| `crates/rushwind-authn-jwt` | JWT 引擎：HS/RS/PS/ES/EdDSA 族的铸造与验证，golang-jwt v5 校验默认对齐 |
| `crates/rushwind-authn-noop` | Noop 引擎：全放行、铸造空凭证 |
| `crates/rushwind-authn-presharedkey` | 预共享 key 引擎：集合成员校验、铸造为随机抽取 |
| `crates/rushwind-authn-session` | 会话引擎：不透明会话 ID + 可插拔 SessionStore |
| `crates/rushwind-authz` | 鉴权契约：`Engine` trait（单裁决 + 三批量过滤）、Subject/Action/Resource/Project 模型、策略 JSON 互通，见 [docs/security-authn-authz.md](./docs/security-authn-authz.md) |
| `crates/rushwind-authz-acl` | ACL 引擎：有序 allow/deny 规则 + 通配匹配，默认拒绝、拒绝优先 |
| `crates/rushwind-authz-rbac` | RBAC 引擎：角色→权限、用户→角色双表，传递继承带环检测 |
| `crates/rushwind-authz-noop` | Noop 引擎：单裁决全通过、批量过滤全空 |
| `crates/rushwind-config` | 配置源契约：`Source` trait（load + 可选 watch/watch_value 能力默认方法）、`SignalStream`/`ValueStream` 流契约、`FallbackSource` 优先级合成（首答胜出 + 有效值变更流合并，无任务边界） |
| `crates/rushwind-config-env` | 环境变量引擎：默认键 + 前缀解析，未设变量为"缺席"而非错误 |
| `crates/rushwind-config-file` | 文件引擎：整文件读取 + 父目录监视（编辑器原子重命名安全），事件突发合并、陈旧值内容抑制，流 Drop 即停 |
| `crates/rushwind-metrics` | 指标契约：`Metrics` trait（counter 累加 / histogram 记录 / gauge 设置），标签规范化排序，记录永不失败调用方 |
| `crates/rushwind-metrics-prometheus` | Prometheus 引擎：按名懒注册 + 每类缓存表，`encode()` 渲染文本格式挂 /metrics 路由 |
| `crates/rushwind-metrics-otel` | OTel 引擎：OTLP 导出（gRPC/HTTP 二进制 protobuf），仪表懒创建缓存，gauge 以 up-down counter 代位 |
| `crates/rushwind-metrics-datadog` | Datadog 引擎：手写 DogStatsD 线协议 over UDP，标签排序、采样率后缀、可选批量缓冲 |
| `crates/rushwind-storage` | 存储契约：`Repository` trait、三种分页（Page/Offset/Token）、过滤器树、Viewer 五级租户、FieldMask、审计钩子 |
| `crates/rushwind-storage-memory` | 内存参考引擎：过滤器/排序/游标的语义基准，零依赖 |
| `crates/rushwind-storage-seaorm` | SeaORM 引擎：SQLite/PostgreSQL/MySQL 三后端同启，三方言 SQL 快照钉死渲染，SQLite 过一致性套件，live 套件跑 CI 容器 |
| `crates/rushwind-storage-cache` | Cache-Aside 装饰器：SingleFlight 合并击穿、缓存键含 viewer 作用域、generation 防陈旧回填 |
| `crates/rushwind-storage-proto` | proto 契约线格式：`proto/rushwind/storage/v1/query.proto` 生成（prost + pbjson），29 操作符映射 + AIP 文本解析 |
| `crates/rushwind-storage-mongodb` | MongoDB 引擎：FilterExpr→BSON 翻译离线单测，LIKE 族编译为转义正则，live 套件跑 CI 容器 |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch 引擎：REST + refresh-on-write，`.keyword` 精确匹配，bulk 原子批写 |
| `crates/rushwind-storage-opensearch` | OpenSearch 引擎：ES 线格式薄复用（wire 兼容） |
| `crates/rushwind-storage-cassandra` | Cassandra 引擎：bucket 固定分区 + 契约求值器过滤，LWT 原子批写 |
| `crates/rushwind-storage-influxdb` | InfluxDB 引擎：measurement 即表，id 为 series tag，InfluxQL 删除 |
| `crates/rushwind-storage-clickhouse` | ClickHouse 引擎：SQL over HTTP，mutations_sync 读己之写，探针式冲突检测 |
| `crates/rushwind-storage-soft-delete` | 软删除装饰器：墓碑写入、全读路径过滤、restore/purge，引擎无关 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive 宏：DTO↔Record 映射编译期生成（对位 go-utils/mapper） |
| `crates/rushwind-storage-tree` | 树形查询：children/roots/ancestors/subtree，契约级遍历 + 环检测，任意引擎可用 |
| `crates/rushwind-storage-observe` | 观测装饰器：每调用一个 `tracing` span（table/op/outcome），OTel 导出交由 subscriber 选型 |
| `crates/rushwind-storage-axum` | HTTP 端点层：任意 Repository 挂成 CRUD 路由，列表查询双入口（protojson `q` / AIP `filter`），viewer 钩子收口租户 |
| `crates/rushwind-script` | 脚本引擎契约：能力拆分 trait 族（loader / executor / global / function / module / watch 六能力聚合，sandbox / runtime-hook / sync / quota 四能力独立），probe 方法即能力探测面，`ScriptValue` 数据桥，名称键工厂注册表，`EnginePool` / `AutoGrowEnginePool`（队列 + 计数信号量，`forget` 语义保持许可数与队列长度一致，`Semaphore::close` 对位 Go `close(chan)` 唤醒），`Manager`；源契约 `ScriptSource` / `SignalStream` 与本地载体、组合（MemSource、FileSource mtime 轮询、FileSystemSource + StaticTree 前缀拼接、MultiSource 的 fallback 顺序走查与 first-ok 同 future 竞速、CachedSource 惰性失效排水 + TTL、TransformSource 变换链） |
| `crates/rushwind-testkit` | 跨适配器一致性测试套件——任何传输/引擎必须整套通过 |
| `examples/multi-server` | 双服务器生命周期演示（级联停机、阶段顺序） |
| `examples/axum-admin` | axum 适配器演示：健康路由 + 信号驱动的优雅停机 |
| `examples/ws-gateway` | WS 网关演示：门拒绝 + 会话上限 + 生命周期级联的会话关闭 |
| `examples/quic-gateway` | QUIC 网关演示：环回门 + 会话上限 + 握手截止 + 端点级联关闭 |
| `examples/mqtt-ingest` | MQTT 消费演示：对接外部 broker，信号驱动的干净退出 |
| `examples/bootstrap-demo` | 装配演示：一份 YAML + memory 引擎工厂 + 路由包，起完整服务 |
| `examples/storage-basics` | 同一段 Repository 代码跑内存与 SQLite 双引擎，输出逐行一致 |

## 生命周期

```text
┌─ 启动：全部 Server 并发运行 ────────────────────────────┐
│  触发集合：OS 信号 / 内部 stop() / 外部信号 / 任一 Server 退出 │
└────────────────────────┬──────────────────────────┘
                         ▼
        阶段 2：before 钩子（顺序执行，各自独立预算）
                         ▼
        阶段 3：全部 Server.stop 并发执行（各自独立预算，panic 隔离）
                         ▼
        阶段 4：after 钩子（顺序执行，各自独立预算）
                         ▼
        终局：outcome() / subscribe_done() 对外可观察
```

每一阶段的预算都在**该阶段开始的时刻**现取现造，绝不继承自更早的上下文——这是 Go 前作修过的两个 bug 的直接教训。忽略停机信号的 Server 在排水截止时被**丢弃**（Rust 的 drop 即取消），挂死的 `stop()` 被截止切断并记录 `Timeout`。规范细节见 [docs/architecture.md](./docs/architecture.md)。

## 给适配器作者

新传输 = 实现 `Server` + 过一致性套件，两件事：

```rust
// crates/rushwind-transport-<你的栈>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

新存储引擎同理 = 实现 `Repository` + 过存储套件：

```rust
// crates/rushwind-storage-<你的引擎>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

`cargo test` 全绿即合规，CI 对每个适配器 crate 强制执行。契约语义与套件用例清单见 [docs/architecture.md](./docs/architecture.md)。

## 开发门禁

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI 在 Linux/Windows/macOS 三平台矩阵上执行同一套门禁。全 workspace `#![forbid(unsafe_code)]`。

## 安全

- 恶意的 `stop()` 拖不死进程：每个阶段有硬预算
- 服务器 panic 被隔离为记录，绝不跳过兄弟服务器的清理
- 漏洞报告流程见 [SECURITY.md](./SECURITY.md)，威胁模型见 [docs/threat-model.md](./docs/threat-model.md)，认证/鉴权层契约与威胁面速记见 [docs/security-authn-authz.md](./docs/security-authn-authz.md)

## 许可

[MIT License](./LICENSE)
