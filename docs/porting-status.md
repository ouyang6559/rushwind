# go-wind-plugins → rushwind 移植状态

全部 20 个 Go 插件域的移植收口记录。日期：2026-09-14。

## 已完成域

| Go 域 | Rust crates | 状态 |
|:---|:---|:---|
| transport | rushwind-transport + axum/mqtt/quic/ws/webtransport/h3 | 完成（原有；webtransport/h3 后补） |
| storage | rushwind-storage + 12 引擎 | 完成（原有） |
| registry | rushwind-registry + 8 引擎 | 完成 |
| security (authn/authz) | rushwind-authn(-*) / rushwind-authz(-*) | 完成（@tx7do） |
| transaction | rushwind-transaction + dtm | 完成（@tx7do） |
| ai | rushwind-ai + openai | 完成（@tx7do） |
| config | rushwind-config + env/file/http/etcd/consul | 完成（契约+env/file @tx7do；http/etcd/consul 后补） |
| metrics | rushwind-metrics + prometheus/otel/datadog | 完成（@tx7do） |
| cache | rushwind-cache + local/redis | 完成 |
| health | rushwind-health | 完成 |
| ratelimit | rushwind-ratelimit + tokenbucket/bbr | 完成 |
| circuitbreaker | rushwind-circuitbreaker + vegas/sres/hystrix | 完成 |
| oss | rushwind-oss + s3 | 完成 |
| broker | rushwind-broker + mqtt/redis/nats/rabbitmq/stomp/kafka/pulsar | 完成 |
| retry | rushwind-retry | 完成 |
| tracer | rushwind-tracer | 完成 |
| testing | rushwind-testkit | 覆盖主用途（Go 侧为 tracer 测试用的 graphql/thrift/protobuf 生成夹具） |

## 明确推迟域

| Go 域 | 推迟原因 |
|:---|:---|
| workflow | 契约仅 Client.Close + Worker.Stop/IsRunning；四个引擎（argo/conductor/goworkflows/temporal）都是重型外部系统的 SDK 适配器，各自独立成项 |
| pprof | Go runtime 特有（net/http/pprof 暴露的是 Go 运行时剖析器）；Rust 生态对应物是 pprof-rs（Linux perf，本仓 CI 含 Windows 无法验证）与 tokio-console |
| log | zap 包装器；Rust 侧 tracing 已是工作区标准，重复封装无收益 |
| ratelimit/sentinel | sentinel-golang 为 Go-only SDK；直接阈值+拒绝规则可映射到 token-bucket 引擎 |
| circuitbreaker/sentinel | 同上，sentinel-golang 适配无 Rust 对应物 |
| config/apollo | 携程 Apollo 配置中心 SDK，中国区专用，按需再移植（契约已可插拔） |

## 备注

- config 域的 etcd/consul/http 源已移植；fs（Go embed.FS）源为 Go 特有（Rust 用 include_bytes!/include_str!），推迟。
- broker 域的 kafka 已移植：samsa 纯 Rust 协议客户端对位 segmentio/kafka-go，免 librdkafka C 构建工具链（消费侧 headers 取不到、无 leave-group 与分区扩容监视等差异见 crate 文档清单）。pulsar 已移植：pulsar-rs（6.9，仅启用 tokio-runtime + compression 特性以规避其默认特性里的 async-std 与 native-tls）对位 apache pulsar-client-go，载荷经 crate 的字节透传、headers 映射 user properties、key 映射 partition key（无 unsubscribe 请求、无生产者轮换重试等差异见 crate 文档清单）。仍推迟：nsq（唯一 Rust 客户端 0.0.3 止于 2017，无维护）、azuresb（azure_messaging_servicebus 0.21 止于 2024-10，源自已归档的 azure-sdk-for-rust）、rocketmq（crates.io 上唯一客户端 5.0.0 止于 2023-08；Go 侧的 aliyun ONS 变体无 Rust SDK 对应物）、sqs/gcpubsub（官方 Rust SDK 均存在且活跃——aws-sdk-sqs、google-cloud-pubsub 1.4——但 localstack / gcloud emulator 容器各 2–3GB 超出 CI 拉取预算；契约已可插拔，需要时按需再移植）。
- ratelimit 的 clock 注入（WithClock）未移植：测试用真实短时长。
