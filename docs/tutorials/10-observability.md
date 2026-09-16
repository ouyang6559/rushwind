# 10 · 可观测性

> 本章目标：掌握统一指标契约与三种出口（Prometheus/OTLP/DogStatsD）、OTLP 追踪装配、健康检查聚合，以及把健康与指标挂上 HTTP 面。

前置：[05 · HTTP 服务栈](05-http-edge.md)。

## 指标契约：三个动词

```rust
pub trait Metrics: Send + Sync {
    fn counter(&self, name: &str, value: f64, labels: &[(&str, &str)]);
    fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);
    fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);
}
```

契约纪律：**记录永不失败**（引擎内部错误吞掉，不打断业务）；标签集按字典序规范化——调用点写标签的顺序不影响序列聚合。

```rust
fn handle(metrics: &dyn Metrics) {
    metrics.counter("requests_total", 1.0, &[("method", "GET"), ("route", "/widgets")]);
    metrics.histogram("latency_seconds", 0.042, &[("route", "/widgets")]);
}
```

## 三种出口

| 引擎 | 模式 | 协议/出口 | 要点 |
|:---|:---|:---|:---|
| `metrics-prometheus` | 拉取 | `/metrics` 文本 | 惰性注册；`namespace`/`subsystem` 前缀 |
| `metrics-otel` | 推送 | OTLP（gRPC 默认，可 HTTP-proto） | 周期 reader；默认 60s；gauge 落成 up-down-counter |
| `metrics-datadog` | 推送 | DogStatsD（UDP 本机代理） | 计数器截断为 i64；可批量缓冲 + 后台刷写线程 |

```rust
// Prometheus：拿到 registry 即可挂 /metrics（见下文 mount_metrics）
let metrics = Arc::new(PrometheusMetrics::new(
    PrometheusOptions::new().with_namespace("myapp").with_subsystem("api"),
));
let body = metrics.encode()?;    // 或者走 HTTP 挂载，别手动 encode

// OTLP：必须在 tokio 运行时内构造（gRPC exporter 要起连接任务）
let otel = OtelMetrics::new(OtelOptions::new()
    .with_endpoint("http://localhost:4317")
    .with_service_name("order-service")
    .with_export_interval(Duration::from_secs(15)))?;

// DogStatsD：UDP 尽力而为，代理不在也不报错（静默丢样）
let dd = DatadogMetrics::new(DogStatsDOptions::new().with_namespace("myapp"))?;
```

业务代码只依赖 `Arc<dyn Metrics>`，出口是装配决定——测试注入内存实现，生产注入三者的组合。

## 追踪：caller 持有 provider

`rushwind-tracer` 只做两件事：OTLP provider 装配 + W3C trace-context 载体：

```rust
use rushwind_tracer::{TracerProviderBuilder, OtlpOptions, Transport, MapCarrier, inject, extract};

let provider = TracerProviderBuilder::new(OtlpOptions {
    endpoint: "127.0.0.1:4318".into(),        // host:port，builder 按 insecure 补协议
    transport: Transport::Http,               // 默认 Grpc
    insecure: true,
    service_name: "order-service".into(),
    ..OtlpOptions::default()
}).build()?;

// 跨进程传播：MapCarrier 就是 HashMap 的 W3C 载体
let mut carrier = MapCarrier::from_map(request_headers);
inject(&context, &mut carrier);
let ctx = extract(&carrier);
```

**框架不注册全局 provider**——`SdkTracerProvider` 归调用方持有（可 Clone），生命周期与停机由你掌控。

## 健康检查：聚合与两档端点

```rust
use rushwind_health::{self, HealthOptions, tcp, http, all, any};

let health = rushwind_health::new(HealthOptions { timeout: Duration::from_secs(5) });
health.register("db", tcp("127.0.0.1:5432", None)).await;
health.register_ping("cache", || async { redis_ping().await.map_err(|e| e.to_string()) }).await;
health.register("deps", all(vec![tcp("kafka", None), http("http://sidecar/health", None)])).await;

let agg = health.check().await;    // { status, message, checks: {...} }
```

聚合规则：**任一 `down` ⇒ down；否则任一 `unknown` ⇒ unknown；空注册表 ⇒ up**（"没有检查项"不是病）。单检查超时按 down 计。两档 HTTP 端点语义分明：`liveness_handler` 恒 200（进程活着就行），`readiness_handler` 随聚合（down ⇒ 503）。

## 挂上 HTTP 面

`rushwind-http` 的两个特性门挂载点一步到位：

```toml
rushwind-http = { version = "*", features = ["health", "metrics"] }
```

```rust
let router = mount_health(router, health);     // /healthz + /readyz
let router = mount_metrics(router, metrics);   // /metrics
// 与第 05 章一致：挂载发生在 wrap 之前，或直接并入公开子树
```

## 踩坑清单

- **Prometheus 标签键按首次定形**：同名指标第一次带 `method` 标签后，后续不带该键的调用会落成空串值——同名指标的标签集要稳定。
- **`OtelMetrics::new` 必须在 tokio 运行时里**（gRPC exporter）；周期导出意味着进程退出前丢最后一批是常态，要精确请在停机钩子里 `shutdown()`。
- **DogStatsD 是 UDP**：代理死了没有错误，只有指标消失——用 `with_buffer_size` 批量时 `Drop` 会先刷后停。
- **追踪 `insecure: false` 是 TLS**；gRPC 的 headers 必须是合法 ASCII metadata，否则构建 panic（fail-loud）。
- **readiness 与 liveness 别用反**：依赖抖动该摘的是 readiness（摘流量），liveness 恒 200 防止被编排器误杀重启。
- **`register` 同名覆盖**：健康检查项按名字替换，重注册即热更。

## 动手练习

1. 把业务里的 `Arc<dyn Metrics>` 换成三个出口同时注入（组合一个 fan-out 的 `Metrics` 实现不过十几行），本地起一个 OTLP collector 看三路数据。
2. 用 `mount_health` + `readiness_handler` 接入第 02 章的 bootstrap 装配，重启数据库容器观察 `/readyz` 从 503 恢复 200。
3. 写一个中间件：从请求头 extract 出上游 context，处理完把 traceparent 注入响应头。

---
[系列目录](README.md) | 上一章：[09 · 注册发现与韧性](09-governance.md) | 下一章：[11 · 脚本引擎](11-script-engines.md)
