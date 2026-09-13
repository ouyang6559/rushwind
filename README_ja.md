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
| P3 | `rushwind-bootstrap`（serde による設定駆動アセンブリ） |

## レイアウト

| パス | 役割 |
|:---|:---|
| `crates/rushwind-core` | ライフサイクル編成：並行起動、カスケード停止、フェーズごとの期限、結果の観測 |
| `crates/rushwind-transport` | 契約層：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum アダプター：`Router` をライフサイクルの下で提供；シャットダウン対応はアーキテクチャ文書参照 |
| `crates/rushwind-transport-ws` | WS セッションルートビルダー：ゲートチェーン + アドミッション + セッションシャットダウンバス |
| `crates/rushwind-transport-quic` | QUIC アダプター：quinn 受け入れループをライフサイクルに接続、フルセッションチェーン；`stop()` は実釈放（Endpoint::close） |
| `crates/rushwind-storage` | ストレージ契約：`Repository` trait、3 種のページング（Page/Offset/Token）、フィルターツリー、5 段階 Viewer テナンシー、FieldMask、監査フック |
| `crates/rushwind-storage-memory` | インメモリ参照エンジン：フィルター/ソート/カーソルの意味論的基準、依存ゼロ |
| `crates/rushwind-storage-seaorm` | SeaORM エンジン：SQLite/PostgreSQL/MySQL の 3 バックエンド同梱、方言ごとの SQL はスナップショットで固定、SQLite がスイート合格、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-cache` | Cache-Aside デコレーター：singleflight ミス統合、スコープ込みキャッシュキー、generation 保護付き無効化 |
| `crates/rushwind-storage-proto` | proto 契約のワイヤ形式：`proto/rushwind/storage/v1/query.proto` から生成（prost + pbjson）、29 操作子マッピング + AIP テキスト構文 |
| `crates/rushwind-storage-mongodb` | MongoDB エンジン：FilterExpr→BSON 翻訳はオフライン単体テスト済み、LIKE 族はエスケープ正規表現にコンパイル、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-soft-delete` | ソフト削除デコレーター：墓碑書き込み、全読み取り経路でフィルタ、restore/purge、エンジン非依存 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive マクロ：DTO↔Record マッピングをコンパイル時に生成（go-utils/mapper の対位） |
| `crates/rushwind-storage-tree` | 木構造クエリ：children/roots/ancestors/subtree を契約レベルの走査で＋循環検出、任意のエンジンで利用可 |
| `crates/rushwind-storage-observe` | 可観測性デコレーター：呼び出しごとに `tracing` スパン（table/op/outcome）、OTel 出力は subscriber の選択 |
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
