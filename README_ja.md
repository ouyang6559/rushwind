<div align="center">

# RushWind

[English](./README_en.md) | [中文](./README.md) | **日本語**

</div>

---

## 設計哲学

> **オールインワンではなく、レゴブロックの箱。**

RushWind が行うのは一つだけ：**信頼性の高いマルチサーバー・ライフサイクル編成**。コアは契約のみを定義します——トランスポート trait、シャットダウンシグナル、インスタンスモデル——そして、個々のプロトコルスタックはすべて独立したアダプター crate として、利用者が必要に応じて組み合わせます。コアにはロギングも、レジストリも、設定センターも含まれません。それらはブロックであって、土板ではありません。

Go の前身 [go-wind](https://github.com/tx7do/go-wind)（同一哲学の Go による表現）と比較して、RushWind は移植ではなく、Rust の所有権・キャンセル・エラーモデルの下で再表現した実装です。意味論的な差異一覧は [docs/architecture.md](./docs/architecture.md)（中国語）を参照してください。

## 現状

**P0（スキャフォールド）**：ライフサイクル・コア、トランスポート契約、クロスアダプターの適合性テストスイートが揃い、全検証を通過しています。アダプターは以下のロードマップに沿って進行します：

| フェーズ | スコープ |
|:---|:---|
| P0 | コア・ライフサイクル、トランスポート契約、適合性スイート ← 現在 |
| P1 | `rushwind-transport-axum`（管理/API 面）、`rushwind-transport-ws`（セッションミドルウェアチェーン） |
| P2 | `rushwind-transport-quic`（QUIC/http3/webtransport）、`rushwind-transport-mqtt`（外部ブローカー消費ブリッジ）、登録のみのレジストリ薄片 |
| P3 | `rushwind-bootstrap`（serde による設定駆動アセンブリ） |

## レイアウト

| パス | 役割 |
|:---|:---|
| `crates/rushwind-core` | ライフサイクル編成：並行起動、カスケード停止、フェーズごとの期限、結果の観測 |
| `crates/rushwind-transport` | 契約層：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-testkit` | クロスアダプター適合性スイート——すべてのトランスポートが全項目に合格する必要がある |
| `examples/multi-server` | 2 サーバーのライフサイクル・デモ（カスケード、フェーズ順序） |

## 開発ゲート

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI は Linux/Windows/macOS のマトリクス上で同一のゲートを実行します。ワークスペース全体が `#![forbid(unsafe_code)]` です。

## ライセンス

[MIT License](./LICENSE)
