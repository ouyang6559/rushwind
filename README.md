<div align="center">

# RushWind · 锐风

[English](./README_en.md) | **中文** | [日本語](./README_ja.md)

</div>

---

## 设计哲学

> **不是全家桶，而是积木盒。**

RushWind 只做一件事：**可靠的多服务器生命周期编排**。核心定义契约——传输 trait、停机信号、实例模型——每一个具体协议栈都是独立的适配器 crate，由使用者按需拼装。核心不含日志、不含注册中心、不含配置中心：那些是积木，不是底板。

对照 Go 前作 [go-wind](https://github.com/tx7do/go-wind)（同一哲学的 Go 表达）：RushWind 不是移植，而是按 Rust 的所有权、取消与错误模型重新表达的实现。语义差异清单见 [docs/architecture.md](./docs/architecture.md)。

## 当前状态

**P0（骨架）**：生命周期核心、传输契约、跨适配器一致性测试套件已就位并通过全套验证。适配器按路线图推进：

| 阶段 | 内容 |
|:---|:---|
| P0 | 生命周期核心、传输契约、一致性套件 |
| P1 | `rushwind-transport-axum`（管理面/API，已交付）；`rushwind-transport-ws`（会话中间件链）← 当前 |
| P2 | `rushwind-transport-quic`（QUIC/http3/webtransport）、`rushwind-transport-mqtt`（外部 broker 消费桥）、注册-only registry 薄片 |
| P3 | `rushwind-bootstrap`（serde 配置驱动装配） |

## 仓库布局

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-core` | 生命周期编排：并发启动、级联停机、分阶段限时、结果观察 |
| `crates/rushwind-transport` | 契约层：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum 适配器：`Router` 接入生命周期，优雅停机映射见架构文档 |
| `crates/rushwind-testkit` | 跨适配器一致性测试套件——任何传输必须整套通过 |
| `examples/multi-server` | 双服务器生命周期演示（级联停机、阶段顺序） |
| `examples/axum-admin` | axum 适配器演示：健康路由 + 信号驱动的优雅停机 |

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
