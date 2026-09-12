# 架构

本文是 RushWind 的权威设计文档：写给两类读者——实现适配器的人（需要精确理解契约语义），以及审阅核心安全性的人。

## 工作区布局与职责边界

| crate | 依赖 | 职责 |
|:---|:---|:---|
| `rushwind-transport` | `tokio-util`（仅 `CancellationToken` 包装） | 契约：`Server` trait、`StopSignal`、`Instance`、`ServerError`。**唯一允许"只有接口"的 crate** |
| `rushwind-core` | `tokio`（signal/time/sync）、`futures`（`FuturesUnordered`、`catch_unwind`） | 生命周期编排。不持有任何业务概念 |
| `rushwind-testkit` | 上两者 + `tokio::time`（探针延迟） | 一致性套件。见文末清单 |
| `examples/*` | 按需 | `publish = false` 的演示程序 |

适配器 crate（P1 起）依赖 `rushwind-transport`（契约）与它们各自的协议栈（axum、quinn、rumqttc……），**永不依赖 `rushwind-core`**——编排是被调用的，不是被引用的。

## 分发模型：对象安全 trait + 借用生命周期装箱

`Server` 的三个方法里，`start`/`stop` 返回 `ServerFuture<'a>`——绑定到借用 `&self` 的装箱 future。这个形状是三个约束的交点：

1. **异构集合**。一个 App 持有不同具体类型的服务器，需要 `Vec<Arc<dyn Server>>`，因此 trait 必须对象安全，方法必须返回装箱的 `dyn Future`。
2. **无任务边界**。装箱 future 携带借用生命周期（`'a`），编排器用 `FuturesUnordered` 在**同一个任务内**并发推进所有服务器——不需要 `'static`，不需要 `tokio::spawn`，因此也不存在 abort 语义、JoinError 语义和任务泄漏问题。借用检查器静态保证没有一个服务器 future 逃出 `run` 的作用域。
3. **适配器作者零样板成本**。每个生命周期方法体是一行 `Box::pin(async move { ... })` 加普通 async 代码；类型其余部分完全是惯用的。

代价：每次 `start`/`stop` 调用一次堆分配（每服务器每生命周期各一次，非每请求）。被否决的备选：

| 备选 | 否决理由 |
|:---|:---|
| `#[async_trait]` | 引入 proc-macro 依赖，生成物与本手写形状完全相同——多一层魔法，无收益 |
| RPITIT（`-> impl Future`） | trait 不可对象安全，异构集合需要 enum 分发，适配器集合退化为闭集 |
| 泛型 App（单型服务器） | 无法表达"管理面 + 会话面混跑"这一立项场景 |
| JoinSet + spawn | 强制 `'static` 与所有权转移，触发 abort/JoinError 语义分支，且服务器句柄被任务边界割裂 |

## 生命周期规范

`App::run` 的行为是固定且可测试的：

| 阶段 | 行为 | 预算 |
|:---|:---|:---|
| 启动 | 全部 `Server::start` 并发推进（`FuturesUnordered`） | 无 |
| 触发等待 | 等待四个触发源之一：OS 终止信号、`App::stop()`、调用方外部信号、任一服务器退出（错误/自退/panic 均算） | 无 |
| 排水 | 触发后，信号已发出；仍在运行的服务器获得**全额预算**协作退出；截止时仍悬挂的 future 被 `clear()` **丢弃**（drop 即取消） | `stop_timeout`，从触发时刻起算 |
| 阶段 2 | before 钩子顺序执行，结果刻意丢弃 | 每钩子独立 `stop_timeout`，钩子开始时现取现造 |
| 阶段 3 | 全部 `Server::stop` 并发执行，每个独立包 `tokio::time::timeout`；panic 被 `catch_unwind` 捕获转为记录 | 每服务器独立，同上 |
| 阶段 4 | after 钩子，同阶段 2 | 同上 |

**预算规则**：任何阶段的截止时刻都在该阶段开始时才计算，绝不复用更早的时刻。这条规则来自 Go 前作的两个已修 bug（停机上下文从运行上下文派生导致超时形同虚设；钩子超时在运行开始时创建导致正常运行期间耗尽）。

**错误聚合**：`Cancelled`（协作退出）在聚合时被过滤；其余的启动侧首个错误优先，否则停止侧首个错误。panic 与超时分别记录为 `Panicked` / `Timeout`。

**丢失语义说明**：排水循环的 `select!` 使用 `biased;` 且流分支列首。原因：非 biased 的 select 在多分支同时就绪时随机选择，落选的 `starts.next()` 分支携带的已完成项会被整体丢弃——错误记录随之丢失。biased + 列首消除了这一竞态。这是 `tokio::select!` 文档中"分支顺序在 biased 模式下决定优先级"语义的正确用法，无饥饿风险（信号分支就绪时流分支必然 Pending，反之亦然）。

## 适配器停机映射（以 axum 为例）

契约只规定可观察语义（endpoint 先于启动解析、启动在信号时返回、stop 释放资源），不规定适配器如何把协议栈自身的能力映射到语义上。axum 适配器（`crates/rushwind-transport-axum`）选择的映射：

| 协议栈能力 | 契约语义 |
|:---|:---|
| 构造期 eager bind + `local_addr` | `endpoint()` 返回真实 `scheme://host:port`（含 `:0` 的 OS 端口分配）；绑定失败在注册时暴露而非启动时 |
| `axum::serve(...).with_graceful_shutdown(stop.wait())` | 信号触发停止接受并排空在途请求——排空发生在 `start` 内部；完成后 `start` 返回 `Cancelled` |
| 排空完成后栈自身关闭监听 | `stop()` 为空操作：编排层停止阶段到达时监听已关闭，无物可释放 |
| （兜底）| 未完成的连接随 start future 一起被排水截止丢弃——适配器无需自实现硬超时 |

留给后续适配器的两条规则：

1. **资源释放必须穷尽栈的原生机制**。axum 的优雅停机就是 hyper 的原生能力；适配器自排空、自管理连接池都是重复造轮子，且排空语义不可能比栈更正确。
2. **`stop()` 为空不代表停机可选**。axum 之所以空，是因为它的栈把全部停机语义内置到了 serve 的返回路径里。没有原生排空机制的栈（会话型传输普遍如此）必须在 `stop()` 里实现真实释放，编排层的截止是兜底而非替代。

## 取消模型

三层，从软到硬：

1. **协作信号**：`StopSignal`（`CancellationToken` 薄包装）。契约要求服务器实现竞速 `stop.wait()` 并以 `Cancelled` 退出。这是唯一"优雅"的路径，服务器有机会自行清理。
2. **drop 弃置**：排水截止后，未退出的启动 future 被 `FuturesUnordered::clear()` 丢弃。Rust 中丢弃 future 即取消——不执行任何代码，不运行析构外的清理，直接消失。这是对不合作服务器的硬约束。
3. **超时截断**：阶段 3 的每个停止 future 被 `timeout` 包裹，截止即被丢弃并记录 `Timeout`。

注意层 2 和层 3 的清理是**进程级兜底**——被 drop 的 future 里的 socket 等资源由各自的 `Drop` 实现释放（tokio 原语均正确实现），但服务器自己的"优雅清理逻辑"（排空队列、通知对端）不会运行。因此适配器作者应当把"信号 → 退出"路径实现得尽可能短。

## Panic 语义

`futures::FutureExt::catch_unwind` 包裹每个生命周期 future。panic 在服务器边界被捕获、字符串化为 `Panicked` 记录、兄弟服务器的清理照常执行——这是套件中两个用例（panic 隔离、级联不因 panic 中断）验证的语义。

边界：`panic = abort` 的发布构建中 `catch_unwind` 无效，进程直接终止。RushWind 不对此做假设；文档化的立场是：**运行 RushWind 的进程应使用 unwind panic 策略，或接受 panic 即进程死亡的语义**。

## 与 go-wind 的语义差异

| Go（go-wind） | Rust（RushWind） | 理由 |
|:---|:---|:---|
| `context.Context` 贯穿所有签名 | `StopSignal` 单独传参，超时由编排器施加、服务器不可见预算 | Rust 无 ctx 对应物；预算是编排器的职责，服务器只需响应信号 |
| `errgroup` + `go` 例程 | `FuturesUnordered`，借用 future，无 spawn | 见分发模型一节 |
| `recover()` 兜底 | `catch_unwind` + `Panicked` 记录 | panic 语义显式化、可测试 |
| `eg.Wait()` 无界等待退出 | 排水有截止，悬挂 future 被 drop | 对不合作服务器有硬边界 |
| 钩子返回错误记日志 | 钩子结果一律丢弃 | 无日志门面；且失败钩子绝不能阻塞后续清理（语义取自 Go 版的"记录但继续"并加强） |
| 信号集含 SIGQUIT | SIGTERM + SIGINT（Unix），Ctrl+C（Windows） | SIGQUIT 在现代部署中语义是 core dump，不适合优雅停机 |
| `Done()`/`Err()` 经 channel + 数据竞争窗口 | `watch` 通道关闭 + `OnceLock`，发布在关闭前完成 | 发布/观察之间有明确的先行发生关系 |

## 一致性套件清单

`rushwind_conformance_suite!` 生成的全部用例（CI 对每个适配器强制）：

| 用例 | 验证 |
|:---|:---|
| `shutdown_cascades_on_server_failure` | 一台服务器失败 → 兄弟服务器仍被清理，终局为错误 |
| `shutdown_phases_run_in_order` | 事件序严格为 before 钩子 → teardown → after 钩子 |
| `stop_phase_deadline_is_enforced` | 挂死的 `stop()` 被截止切断，终局为 `Timeout` |
| `ill_behaved_server_is_abandoned_at_deadline` | 无视信号的服务器在排水截止被弃置，生命周期正常完结 |
| `panicking_server_is_isolated_and_siblings_still_tear_down` | panic 被隔离为记录，兄弟清理不受影响 |
| `endpoint_is_uri_shaped` | 适配器：`endpoint()` 返回 `scheme://authority` |
| `cooperative_shutdown_completes` | 适配器：真实服务器在信号下干净退出 |

套件自检：`crates/rushwind-testkit/tests/conformance_self.rs` 以内置探针跑通全部用例，验证套件本身有效。
