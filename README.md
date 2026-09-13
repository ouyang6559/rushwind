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
| `crates/rushwind-bootstrap` | 配置驱动装配：YAML → 存储引擎 + HTTP 服务器 + 路由包，汇入单一生命周期 |
| `crates/rushwind-registry` | 注册-only registry 契约：`Registrar` trait + 与 go-wind 字节对齐的键布局/线格式（golden 钉死） |
| `crates/rushwind-registry-etcd` | etcd 适配器：租约 TTL + 自愈 keepalive，句柄 Drop 回退过期；live 套件（CI etcd 容器）钉死互操作 |
| `crates/rushwind-storage` | 存储契约：`Repository` trait、三种分页（Page/Offset/Token）、过滤器树、Viewer 五级租户、FieldMask、审计钩子 |
| `crates/rushwind-storage-memory` | 内存参考引擎：过滤器/排序/游标的语义基准，零依赖 |
| `crates/rushwind-storage-seaorm` | SeaORM 引擎：SQLite/PostgreSQL/MySQL 三后端同启，三方言 SQL 快照钉死渲染，SQLite 过一致性套件，live 套件跑 CI 容器 |
| `crates/rushwind-storage-cache` | Cache-Aside 装饰器：SingleFlight 合并击穿、缓存键含 viewer 作用域、generation 防陈旧回填 |
| `crates/rushwind-storage-proto` | proto 契约线格式：`proto/rushwind/storage/v1/query.proto` 生成（prost + pbjson），29 操作符映射 + AIP 文本解析 |
| `crates/rushwind-storage-mongodb` | MongoDB 引擎：FilterExpr→BSON 翻译离线单测，LIKE 族编译为转义正则，live 套件跑 CI 容器 |
| `crates/rushwind-storage-soft-delete` | 软删除装饰器：墓碑写入、全读路径过滤、restore/purge，引擎无关 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive 宏：DTO↔Record 映射编译期生成（对位 go-utils/mapper） |
| `crates/rushwind-storage-tree` | 树形查询：children/roots/ancestors/subtree，契约级遍历 + 环检测，任意引擎可用 |
| `crates/rushwind-storage-observe` | 观测装饰器：每调用一个 `tracing` span（table/op/outcome），OTel 导出交由 subscriber 选型 |
| `crates/rushwind-storage-axum` | HTTP 端点层：任意 Repository 挂成 CRUD 路由，列表查询双入口（protojson `q` / AIP `filter`），viewer 钩子收口租户 |
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
- 漏洞报告流程见 [SECURITY.md](./SECURITY.md)，威胁模型见 [docs/threat-model.md](./docs/threat-model.md)

## 许可

[MIT License](./LICENSE)
