# confluent-kafka-poc — CLAUDE.md

Apache Kafka / Confluent Platform のユースケース PoC 基盤。
新しいユースケースの feasibility 検証をすぐに始められるように設計されている。

## プロジェクト概要

- **目的**: Kafka を使ったシステムアーキテクチャのパターンを実装し、実際に動かして検証する
- **対象読者**: エンジニア / アーキテクト
- **スタック**: Python 3.11+ / confluent-kafka / Docker Compose / Confluent Platform 7.5

## 必須前提条件

- Docker Desktop（`docker` + `docker compose` コマンド）
- Python 3.11+
- `pip install -r requirements.txt`

## 環境起動

```bash
./scripts/start.sh          # Kafka 起動 + ヘルスチェック
./scripts/stop.sh           # シャットダウン
./scripts/reset.sh          # データ全削除（再実験用）
```

UI:
- kafka-ui: http://localhost:8080
- Control Center: http://localhost:9021

## ディレクトリ構成

```
confluent-kafka-poc/
├── docker/              # Docker Compose 設定（フル / 最小）
├── core/                # 共通インフラ（BaseProducer / BaseConsumer / AdminClient / Config）
├── use_cases/           # ユースケースデモ（01〜07）
├── benchmarks/          # スループット・レイテンシ計測
├── tests/               # pytest 統合テスト
└── scripts/             # 起動・停止・リセット・全デモ実行
```

## 各ユースケース

| # | 名前 | 検証できること |
|---|------|--------------|
| 01 | basic_pubsub | 基本的な pub/sub、コンシューマーグループ |
| 02 | event_sourcing | イベント履歴・状態再構築・リプレイ |
| 03 | stream_processing | ウィンドウ集計・フィルタ変換・ストリーム Join |
| 04 | cdc | DB 変更イベントの Kafka 経由レプリケーション |
| 05 | dead_letter_queue | 失敗メッセージのルーティングとリカバリー |
| 06 | exactly_once | トランザクション・冪等性・EOS |
| 07 | real_time_analytics | リアルタイム KPI 集計とコンソール表示 |

## 開発ルール

- **`find` コマンドは使わない** — ディレクトリ調査は `ls` のみ
- **環境変数はコードにハードコードしない** — 必ず `.env` 経由で `core/config.py` から読む
- **新しいユースケース** → `use_cases/0N_name/` に追加し、README.md・producer.py・consumer.py を揃える
- **トピック命名規則**: `{usecase}.{entity}.{version}` (例: `pubsub.orders.v1`)
- **テスト** → Kafka 起動状態で `pytest tests/ -v`

## よくあるエラーと対処

```bash
# Kafka に接続できない
# → ./scripts/start.sh でヘルスチェックが通るか確認
# → docker ps で全コンテナが Up か確認

# ModuleNotFoundError: confluent_kafka
pip install -r requirements.txt

# トピックが既に存在する
# → core/admin.py の ensure_topic_exists() は idempotent（存在していればスキップ）

# reset でもデータが残る
./scripts/reset.sh && ./scripts/start.sh
```

## 初学者向け補足

### このリポジトリの学習順序

1. まず `docs/kafka-concepts.md` で基本概念（トピック・パーティション・オフセット）を理解する
2. `docs/getting-started.md` で環境を構築する
3. `use_cases/01_basic_pubsub/` でメッセージの流れを体感する
4. `docs/use-cases-guide.md` を参考に 02〜07 を順番に試す
5. `benchmarks/` でスループット・レイテンシを計測する

### コードの読み方

`core/` の基底クラスを先に読む（BaseProducer → BaseConsumer → AdminClient）。
各ユースケースは core を継承しているだけなので、差分が小さく理解しやすい。

### ドキュメント一覧

| ドキュメント | 対象読者 | 内容 |
|-----------|---------|------|
| `docs/kafka-concepts.md` | 初学者 | Kafka の基本概念（ASCII 図解付き） |
| `docs/getting-started.md` | 初学者 | 環境構築の詳細手順 |
| `docs/use-cases-guide.md` | 初学者〜中級 | 全7ユースケースの詳細解説 |
| `docs/troubleshooting.md` | 全員 | よくあるエラーと解決法 |

---

## Confluent Cloud への切り替え

`.env` に以下を設定するだけ。コード変更不要:
```
KAFKA_ENV=confluent
KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<API_KEY>
KAFKA_SASL_PASSWORD=<API_SECRET>
```
