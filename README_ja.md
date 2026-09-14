<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

[English](./README_en.md) | [中文](./README.md) | **日本語**

</div>

---

## 設計哲学

> **オールインワンではなく、レゴブロックの箱。**

RushWind が行うのは一つだけ：**信頼性の高いマルチサーバー・ライフサイクル編成**。コアは契約のみを定義します——トランスポート trait、シャットダウンシグナル、インスタンスモデル——そして、個々のプロトコルスタックはすべて独立したアダプター crate として、利用者が必要に応じて組み合わせます。コアにはロギングも、レジストリも、設定センターも含まれません。それらはブロックであって、土板ではありません。

Go の前身 [go-wind](https://github.com/tx7do/go-wind)（同一哲学の Go による表現）と比較して、RushWind は移植ではなく、Rust の所有権・キャンセル・エラーモデルの下で再表現した実装です。意味論的な差異一覧は [docs/architecture.md](./docs/architecture.md)（中国語）を参照してください。データアクセス層も同じレシピです：[go-crud](https://github.com/tx7do/go-crud) の「一つの Repository 契約で多数のストレージエンジンを驾驭する」を、`rushwind-storage` 契約 + エンジンごとのアダプター crate として再表現しました。

## 現状

**P0（スキャフォールド）**：ライフサイクル・コア、トランスポート契約、クロスアダプターの適合性スイートが揃い、全検証を通過しています。アダプターは以下のロードマップに沿って進行します：

| フェーズ | スコープ |
|:---|:---|
| P0 | コア・ライフサイクル、トランスポート契約、適合性スイート |
| P1 | `rushwind-transport-axum`（管理/API 面）、`rushwind-transport-ws`（セッションミドルウェアチェーン：ゲート + アドミッション + セッションシャットダウンバス、[session-middleware.md](./docs/session-middleware.md) 参照） |
| P2 | `rushwind-transport-quic`（素の QUIC セッション + フルセッションチェーン：ゲート/refuse、原子アドミッション、ハンドシェイク期限——提供済み；h3/webtransport は後から積み上げ）；`rushwind-transport-mqtt`（外部ブローカー消費ブリッジ）、登録のみのレジストリ薄片 ← 現在 |
| storage | `rushwind-storage`（契約）+ 4 エンジン：インメモリ参照、SeaORM（SQLite/PostgreSQL/MySQL）、MongoDB；横断層 `rushwind-storage-cache` / `rushwind-storage-soft-delete` / `rushwind-storage-observe`（透明デコレーター）；アルゴリズム積木 `rushwind-storage-tree`（木走査）；`rushwind-storage-proto`（proto 定義契約 + protojson + AIP テキスト構文）；`rushwind-storage-macros`（DTO マッピング derive）；[go-crud](https://github.com/tx7do/go-crud) の対位 |
| auth | `rushwind-authn` / `rushwind-authz`（契約）+ エンジン行列：認証 7 エンジン（apikey / basicauth / hmac / jwt / noop / presharedkey / session）、認可 3 エンジン（acl / rbac / noop）；`AuthenticationGate` が認証エンジンをセッションのゲートチェーンに接続——契約は [docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語） |
| config | `rushwind-config`（契約：`Source` trait + 既定の watch 能力メソッド、`FallbackSource` 優先順位合成と変更ストリーム統合）+ 2 エンジン：env（接頭辞付き環境変数）、file（単一ファイル + 親ディレクトリ監視、バースト統合と陳腐値抑制）；`go-wind-plugins/config` の対位 |
| metrics | `rushwind-metrics`（契約：`Metrics` trait——counter/histogram/gauge、ラベル正規化ソート）+ 3 エンジン：prometheus（プル：レジストリ遅延登録 + テキスト形式露出）、otel（プッシュ：OTLP gRPC/HTTP エクスポート）、datadog（プッシュ：手書き DogStatsD over UDP + バッチバッファ）；`go-wind-plugins/metrics` の対位 |
| script | `rushwind-script`（契約：`ScriptEngine` ライフサイクル核心 + 独立能力 trait 族——probe メソッドで Go の `As*` アサーションに対応、`FullEngine` 集約ブランケット実装；`ScriptValue` データブリッジ；名前キーのファクトリーレジストリ、固定・自動拡張の両エンジンプール、`Manager`；ローカルソースフレームワーク層——メモリ、ファイル（mtime ポーリング監視）、静的ツリー + 接頭辞結合、二戦略マルチソース集約、TTL と監視駆動失効付きキャッシュ、変換チェーン）；`go-scripts` の対位 |
| http | `rushwind-http`（HTTP エッジ：gRPC 整列のエラー封筒 `HttpError`——code/reason/message/details、`AuthnError`/`StorageError` の組み込み変換；リクエストミドルウェアスタック recovery / request-id / logging / CORS / timeout と `HttpEdge` アセンブラ；`with_authn` / `with_authorization` で認証・認可契約を axum ルートに接続、`Authenticated` エクストラクター；feature ゲートの `/healthz`+`/readyz` と `/metrics` マウント）；`go-wind-plugins/transport/http/middleware` の対位、設計は [docs/http-edge.md](./docs/http-edge.md) |
| P3 | `rushwind-bootstrap`（serde による設定駆動アセンブリ） |

## レイアウト

| パス | 役割 |
|:---|:---|
| `crates/rushwind-core` | ライフサイクル編成：並行起動、カスケード停止、フェーズごとの期限、結果の観測 |
| `crates/rushwind-transport` | 契約層：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum アダプター：`Router` をライフサイクルの下で提供；シャットダウン対応はアーキテクチャ文書参照 |
| `crates/rushwind-transport-ws` | WS セッションルートビルダー：ゲートチェーン + アドミッション + セッションシャットダウンバス |
| `crates/rushwind-transport-quic` | QUIC アダプター：quinn 受け入れループをライフサイクルに接続、フルセッションチェーン；`stop()` は実釈放（Endpoint::close） |
| `crates/rushwind-http` | HTTP エッジ：エラー封筒 + リクエストミドルウェアスタック（recovery / request-id / logging / CORS / timeout）+ 認証・認可ブリッジ + ヘルス/メトリクスのマウント——[docs/http-edge.md](./docs/http-edge.md) 参照 |
| `crates/rushwind-authn` | 認証契約：`Authenticator` trait（抽出/検証の両半分）、`AuthClaims` クレームバッグ、エラー分類学、`AuthenticationGate` ゲート接続層——[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authn-apikey` | API キー・エンジン：静的キー集合 / キーごとのクレーム / 検証コールバック |
| `crates/rushwind-authn-basicauth` | Basic-Auth エンジン：RFC 7617 資格情報を静的ユーザーテーブルまたは検証コールバックに対して |
| `crates/rushwind-authn-hmac` | HMAC エンジン：keyID.timestamp.signature 検証、時計ずれ窓 |
| `crates/rushwind-authn-jwt` | JWT エンジン：HS/RS/PS/ES/EdDSA 族の発行と検証、golang-jwt v5 既定の検証プロファイルに整合 |
| `crates/rushwind-authn-noop` | noop エンジン：すべて受理、空の資格情報を鋳造 |
| `crates/rushwind-authn-presharedkey` | 事前共有鍵エンジン：集合所属検査、鋳造は無作為抽出 |
| `crates/rushwind-authn-session` | セッション・エンジン：不透明セッション ID と取り替え可能な SessionStore |
| `crates/rushwind-authz` | 認可契約：`Engine` trait（単一評決 + 3 つの一括フィルタ）、Subject/Action/Resource/Project モデル、JSON ポリシー相互運用——[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authz-acl` | ACL エンジン：順序付き allow/deny ルール + ワイルドカード照合、既定拒否・拒否優先 |
| `crates/rushwind-authz-rbac` | RBAC エンジン：役割→権限、ユーザー→役割の双表、循環検出付きの推移的継承 |
| `crates/rushwind-authz-noop` | noop エンジン：単一評決はすべて通過、一括フィルタはすべて空 |
| `crates/rushwind-config` | 設定ソース契約：`Source` trait（load + 既定の watch/watch_value 能力メソッド）、`SignalStream`/`ValueStream` ストリーム契約、`FallbackSource`——最初の回答が勝つ優先順位合成と、実効値への変更ストリーム統合、タスク境界なし |
| `crates/rushwind-config-env` | 環境変数エンジン：既定キー + 接頭辞解決、未設定変数は「不在」であってエラーではない |
| `crates/rushwind-config-file` | ファイルエンジン：ファイル全体の読み取り + 親ディレクトリ監視（エディタの原子リネームに強い）、イベントバースト統合、内容による陳腐値抑制、ストリームの drop で監視停止 |
| `crates/rushwind-metrics` | メトリクス契約：`Metrics` trait（counter 加算 / histogram 記録 / gauge 設定）、ラベル正規化ソート、記録が呼び出し元を失敗させない |
| `crates/rushwind-metrics-prometheus` | Prometheus エンジン：名前ごとの遅延登録 + 種類ごとのキャッシュ表、`encode()` でテキスト形式を描画し /metrics ルートに |
| `crates/rushwind-metrics-otel` | OTel エンジン：OTLP エクスポート（gRPC / HTTP バイナリ protobuf）、計器の遅延生成キャッシュ、gauge は up-down counter で代用 |
| `crates/rushwind-metrics-datadog` | Datadog エンジン：手書き DogStatsD ラインプロトコル over UDP、タグソート、サンプルレート接尾辞、任意バッチバッファ |
| `crates/rushwind-storage` | ストレージ契約：`Repository` trait、3 種のページング（Page/Offset/Token）、フィルターツリー、5 段階 Viewer テナンシー、FieldMask、監査フック |
| `crates/rushwind-storage-memory` | インメモリ参照エンジン：フィルター/ソート/カーソルの意味論的基準、依存ゼロ |
| `crates/rushwind-storage-seaorm` | SeaORM エンジン：SQLite/PostgreSQL/MySQL の 3 バックエンド同梱、方言ごとの SQL はスナップショットで固定、SQLite がスイート合格、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-cache` | Cache-Aside デコレーター：singleflight ミス統合、スコープ込みキャッシュキー、generation 保護付き無効化 |
| `crates/rushwind-storage-proto` | proto 契約のワイヤ形式：`proto/rushwind/storage/v1/query.proto` から生成（prost + pbjson）、29 操作子マッピング + AIP テキスト構文 |
| `crates/rushwind-storage-mongodb` | MongoDB エンジン：FilterExpr→BSON 翻訳はオフライン単体テスト済み、LIKE 族はエスケープ正規表現にコンパイル、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch エンジン：REST + 書き込み時 refresh、`.keyword` 完全一致、bulk 原子性とロールバック |
| `crates/rushwind-storage-opensearch` | OpenSearch エンジン：ES ワイヤ形式の薄い再利用（ワイヤ互換） |
| `crates/rushwind-storage-cassandra` | Cassandra エンジン：bucket 固定パーティション + 契約評価器フィルタ、LWT 原子バッチ |
| `crates/rushwind-storage-influxdb` | InfluxDB エンジン：measurement をテーブルとして、id は series タグ、InfluxQL 削除 |
| `crates/rushwind-storage-clickhouse` | ClickHouse エンジン：HTTP 経由の SQL、mutations_sync で読み取り一貫性、プローブ型競合検出 |
| `crates/rushwind-storage-soft-delete` | ソフト削除デコレーター：墓碑書き込み、全読み取り経路でフィルタ、restore/purge、エンジン非依存 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive マクロ：DTO↔Record マッピングをコンパイル時に生成（go-utils/mapper の対位） |
| `crates/rushwind-storage-tree` | 木構造クエリ：children/roots/ancestors/subtree を契約レベルの走査で＋循環検出、任意のエンジンで利用可 |
| `crates/rushwind-storage-observe` | 可観測性デコレーター：呼び出しごとに `tracing` スパン（table/op/outcome）、OTel 出力は subscriber の選択 |
| `crates/rushwind-storage-axum` | HTTP エッジ層：任意の Repository を CRUD ルートとして公開、一覧クエリは protojson `q` / AIP `filter` の二入口、viewer フックでテナンシーを収口 |
| `crates/rushwind-script` | スクリプトエンジン契約：能力分割 trait 族（loader / executor / global / function / module / watch の六能力を集約、sandbox / runtime-hook / sync / quota の四能力は独立）、probe メソッドが能力探出面、`ScriptValue` データブリッジ、名前キーのファクトリーレジストリ、`EnginePool` / `AutoGrowEnginePool`（キュー + 計数セマフォ、許可数とキュー長を一致させる `forget` セマンティクス、`Semaphore::close` で Go の `close(chan)` 覚醒に対応）、`Manager`；ソース契約 `ScriptSource` / `SignalStream` とそのローカル担体・合成（MemSource、mtime ポーリングの FileSource、StaticTree 上の FileSystemSource と接頭辞結合、MultiSource の fallback 順次走査と first-ok 同一 future 内競走、CachedSource の遅延失効ドレインと TTL、TransformSource の変換チェーン） |
| `crates/rushwind-testkit` | クロスアダプター適合性スイート——すべてのトランスポート/エンジンが全項目に合格する必要がある |
| `examples/multi-server` | 2 サーバーのライフサイクル・デモ（カスケード、フェーズ順序） |
| `examples/axum-admin` | axum アダプター・デモ：ヘルスルート + シグナル駆動のグレースフルシャットダウン |
| `examples/ws-gateway` | WS ゲートウェイ・デモ：ゲート拒否 + セッション上限 + ライフサイクル連動のセッション切断 |
| `examples/quic-gateway` | QUIC ゲートウェイ・デモ：ループバックゲート + セッション上限 + ハンドシェイク期限 + エンドポイント級の切断 |
| `examples/storage-basics` | 同じ Repository コードをインメモリと SQLite の 2 エンジンで実行、出力は行単位で一致 |

## アダプター作者へ

新しいトランスポート = `Server` を実装 + 適合性スイートに合格、の 2 ステップ：

```rust
// crates/rushwind-transport-<あなたのスタック>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

ストレージエンジンも同様 = `Repository` を実装 + ストレージスイートに合格：

```rust
// crates/rushwind-storage-<あなたのエンジン>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

`cargo test` が全緑であることが適合の定義です。CI がすべてのアダプター crate に対して強制します。契約の意味論とスイートのケース一覧は [docs/architecture.md](./docs/architecture.md) を参照。

## 開発ゲート

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI は Linux/Windows/macOS のマトリクス上で同一のゲートを実行します。ワークスペース全体が `#![forbid(unsafe_code)]` です。

## ライセンス

[MIT License](./LICENSE)
