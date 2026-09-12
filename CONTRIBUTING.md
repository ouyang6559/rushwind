# 贡献指南

## 门槛（CI 强制，无例外）

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

三条全绿是合并的前提。CI 在 Linux/Windows/macOS 三平台矩阵上执行同一套门禁。

## 硬性规则

1. **零 unsafe**。workspace 级 `forbid(unsafe_code)`，不接受 `#[allow]` 例外。
2. **新传输 = 实现 `Server` + 通过一致性套件**。两者缺一，适配器不算完成。套件接入方式见 [testkit 文档](./crates/rushwind-testkit/src/lib.rs)与 [架构文档](./docs/architecture.md)。
3. **适配器不依赖 `rushwind-core`**。编排是被调用的，不是被引用的。依赖方向：适配器 → `rushwind-transport`（契约）+ 各自协议栈。
4. **预算语义不可协商**。每阶段预算现取现造、排水截止、panic 隔离——这些是套件固定的语义，改动它们需要先改套件并说明理由。

## 结构

- 新适配器：`crates/rushwind-transport-<栈>/`，Cargo.toml 声明 `[lints] workspace = true`
- 新示例：`examples/<名称>/`，`publish = false`
- 设计文档：`docs/`——契约语义变更必须同步更新 `architecture.md`

## 提交流程

主干开发，feature 分支合 PR。commit 信息用英文小写祈使句（`feat:`, `fix:`, `docs:`, `chore:` 前缀）。
