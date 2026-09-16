# 01 · 生命周期与第一个服务

> 本章目标：理解 RushWind 的三个生命周期原语 —— `Server`、`StopSignal`、`App`，并用三十行代码跑起第一个带统一中间件栈的 HTTP 服务。

前置：无。这是全系列的起点。

## 三个原语

RushWind 把"一个正在运行的服务"拆成三件事：

- **`Server`（`rushwind-transport`）**——传输契约。任何能"绑定端点、在启动后持续服务、响应停机"的东西都实现它：HTTP 服务器、QUIC 网关、定时任务调度器、MQTT 消费桥……契约只有三个方法：

  ```rust
  pub trait Server: Send + Sync {
      fn endpoint(&self) -> Result<String, ServerError>;
      fn start(&self, stop: StopSignal) -> ServerFuture<'_>;
      fn stop(&self) -> ServerFuture<'_>;
  }
  ```

- **`StopSignal`**——停机信号，是对 `CancellationToken` 的封装。`signal()` 发出停止、`wait().await` 等待停止；每个 `Server` 的 `start` 都会拿到一份，负责把自己的循环跑到体面结束。

- **`App`（`rushwind-core`）**——编排者。它把一个或多个 `Server` 跑起来，监听外部停止信号，超时兜底，最后汇合所有服务的退出结果：

  ```rust
  let app = App::builder()
      .name("hello-rushwind")
      .version("0.1.0")
      .erased_server(Arc::new(server))   // 任意多个 server/erased_server
      .stop_timeout(Duration::from_secs(5))
      .build();

  app.run(StopSignal::new()).await?;     // Ctrl+C / SIGTERM / app.stop() 都会走到这里
  ```

`run` 返回 `Result<(), ServerError>`。`ServerError::Cancelled` 表示"因停机信号而退出"——这是体面停机的正常路径，不是失败。编排器会把这一档过滤掉，只有真正的故障（`Failed`/`Timeout`/`Panicked`）才以错误收场。

## 第一个服务

`HttpEdge`（`rushwind-http`）是 HTTP 请求的默认中间件栈：panic 恢复、请求 ID、访问日志。把一个 axum `Router` 交给它 `wrap`，再交给 `AxumServer`（`rushwind-transport-axum`），就得到一个完整服务：

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use rushwind_core::App;
use rushwind_http::HttpEdge;
use rushwind_transport::StopSignal;
use rushwind_transport_axum::AxumServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = HttpEdge::new().wrap(
        Router::new().route("/hello", get(|| async { "hello rushwind" })),
    );

    let server = AxumServer::new(SocketAddr::from(([127, 0, 0, 1], 0)), router)?;
    println!("listening on {}", server.endpoint()?);

    let app = App::builder()
        .name("hello-rushwind")
        .version("0.1.0")
        .erased_server(Arc::new(server))
        .build();

    app.run(StopSignal::new()).await?;
    Ok(())
}
```

把它放进一个 bin crate（依赖 `rushwind-core`、`rushwind-http`、`rushwind-transport`、`rushwind-transport-axum`、`axum`、`tokio`），`cargo run` 后：

```console
listening on 127.0.0.1:54321
^C
```

```console
$ curl -i http://127.0.0.1:54321/hello
HTTP/1.1 200 OK
x-request-id: 9f8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d
```

注意 `x-request-id`：这不是你写的，是 `HttpEdge` 默认栈里的请求 ID 中间件加的——16 字节 CSPRNG 随机数、32 个十六进制字符，响应头 `x-request-id` 回显，处理器里可用提取器 `RequestId` 拿到。

## `HttpEdge` 默认栈里有什么

`HttpEdge::new()` 默认开启三件套，builder 方法负责增删：

```rust
HttpEdge::new()
    .without_logging()                    // 关掉访问日志
    .with_request_id_generator(Arc::new(|| my_id()))  // 自定义请求 ID
    .with_timeout(Duration::from_secs(3)) // 加超时层，超时返回 504
    .wrap(router)                         // 最后 wrap
```

请求经过的顺序是固定的：**recovery → request-id → logging → CORS（若配置）→ timeout**。无论 builder 以什么顺序调用，`wrap` 之后恢复层永远在最外层——panic 不会穿透到客户端，只会变成 `500 INTERNAL_PANIC`，细节留在日志里。

## 踩坑清单

- **绑定失败在构造时暴露**。`AxumServer::new` 急切地绑定端口，端口被占的错误在注册现场就能拿到，不会拖到 `start`。`bind 127.0.0.1:0` 让 OS 分配端口，用 `local_addr()`/`endpoint()` 读回真实地址。
- **`start` 只能调一次**。第二次调用确定性失败（`"start called more than once"`）。直接使用 `Server` 契约时由 `App` 保证这一点。
- **`Cancelled` 不是错误**。写外层监控时不要把 `ServerError::Cancelled` 当故障告警。
- **TLS 不在这一层**。`AxumServer` 只讲明文 HTTP；TLS 终结交给前置代理，或者改用 QUIC/H3/WebTransport 传输（见[第 12 章](12-transports.md)）。
- **层只包住"已经加进来的路由"**。这是 axum 的语义：先装配完整路由集，最后 `layer`/`wrap`。做公开/保护路由的白名单时靠子树拆分与 `merge`，而不是中间件里的路径判断——详见[第 05 章](05-http-edge.md)。

## 动手练习

1. 给 `/hello` 加一条 `/boom` 路由，处理器里 `panic!("boom")`，观察恢复层返回的信封与响应码。
2. 用 `.with_timeout(Duration::from_millis(50))` 加一个 `tokio::time::sleep` 一秒的处理器，确认拿到 504。
3. 把 `.erased_server(Arc::new(server))` 换成注册两个不同端口的 `AxumServer`，体会"一个 App 编排多个服务"。

---
[系列目录](README.md) | 下一章：[02 · 装配器与配置](02-bootstrap-and-config.md)
