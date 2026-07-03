---
name: kafka-poc
description: |
  Confluent/Kafka の PoC 基盤。新しいユースケースを追加する・既存デモを実行する・
  ベンチマークを測る・Docker 環境を起動/停止する・コードを修正するときに使う。
  「Kafka でXXXを検証して」「新しいユースケースを追加して」「スループットを測って」
  「デモを動かして」「Kafka を起動して」のような依頼で使う。
---

# Confluent/Kafka PoC 基盤

## 概要

Apache Kafka / Confluent Platform のユースケースを素早く検証するための PoC リポジトリ。
7 種のユースケースデモ + ベンチマーク基盤 + Docker 環境が含まれる。

## 環境起動

```bash
# フル構成（Kafka + ZooKeeper + Schema Registry + Control Center + kafka-ui）
./scripts/start.sh

# 最小構成（Kafka + ZooKeeper + kafka-ui のみ）
cd docker && docker-compose -f docker-compose.minimal.yml up -d

# 停止
./scripts/stop.sh

# トピック・データ全削除（リセット）
./scripts/reset.sh
```

UI アクセス:
- kafka-ui: http://localhost:8080
- Control Center: http://localhost:9021（フル構成のみ）

## Python セットアップ

```bash
pip install -r requirements.txt
cp .env.tpl .env
# .env をローカル設定に合わせて編集（デフォルトのままでも動く）
```

## ユースケース一覧と実行コマンド

### 01 — Basic Pub/Sub
```bash
# ターミナル1（コンシューマー）
python use_cases/01_basic_pubsub/consumer.py

# ターミナル2（プロデューサー）
python use_cases/01_basic_pubsub/producer.py
```

### 02 — Event Sourcing
```bash
python use_cases/02_event_sourcing/event_store.py   # イベント送信 + 再構築
python use_cases/02_event_sourcing/replay.py         # リプレイデモ
```

### 03 — Stream Processing
```bash
python use_cases/03_stream_processing/aggregator.py        # 30秒ウィンドウ集計
python use_cases/03_stream_processing/filter_transform.py  # フィルタ・変換
python use_cases/03_stream_processing/stream_join.py       # 2ストリーム Join
```

### 04 — CDC（Change Data Capture）
```bash
# ターミナル1（CDCコンシューマー）
python use_cases/04_cdc/cdc_consumer.py

# ターミナル2（DBシミュレーター）
python use_cases/04_cdc/simulator.py
```

### 05 — Dead Letter Queue
```bash
python use_cases/05_dead_letter_queue/consumer.py    # DLQ ルーター起動
python use_cases/05_dead_letter_queue/producer.py    # 失敗メッセージ混入
python use_cases/05_dead_letter_queue/dlq_processor.py  # DLQ からリカバリー
```

### 06 — Exactly-Once Semantics
```bash
python use_cases/06_exactly_once/transactional_producer.py
python use_cases/06_exactly_once/idempotent_consumer.py
```

### 07 — Real-Time Analytics（コンソールダッシュボード）
```bash
# ターミナル1（ダッシュボード）
python use_cases/07_real_time_analytics/aggregator.py

# ターミナル2（イベント生成）
python use_cases/07_real_time_analytics/event_generator.py

# ターミナル3（Rich UI）
python use_cases/07_real_time_analytics/dashboard.py
```

### 全デモ順番に実行
```bash
./scripts/run_all_demos.sh
```

## ベンチマーク

```bash
# スループット測定（10万メッセージ）
python benchmarks/throughput_producer.py
python benchmarks/throughput_consumer.py

# E2E レイテンシ測定（P50/P95/P99）
python benchmarks/latency_test.py

# レポート生成（markdown）
python benchmarks/report.py
```

## テスト

```bash
pytest tests/ -v
# 注意: Kafka が起動している必要がある（./scripts/start.sh 後）
```

## 新しいユースケースを追加する方法

1. `use_cases/08_<name>/` ディレクトリを作成
2. `README.md`（目的・トピック設計・実行方法）を書く
3. `producer.py` と `consumer.py` を `core/producer.py` / `core/consumer.py` を継承して実装
4. `core/admin.py` の `ensure_topic_exists()` でトピックを作成
5. `tests/test_use_cases.py` にスモークテストを追加

## Confluent Cloud への切り替え

`.env` を以下のように変更:
```
KAFKA_ENV=confluent
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<CLUSTER_API_KEY>
KAFKA_SASL_PASSWORD=<CLUSTER_API_SECRET>
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-east-2.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<SR_KEY>
SCHEMA_REGISTRY_API_SECRET=<SR_SECRET>
```

コードは変更不要。`core/config.py` が自動的に SASL 設定を適用する。

## トピック命名規則

```
{usecase}.{entity}.{version}
例: pubsub.orders.v1, cdc.users.v1, analytics.events.v1
```

## 完了チェックリスト

- [ ] `./scripts/start.sh` で Kafka が起動した
- [ ] kafka-ui (http://localhost:8080) でトピックが見える
- [ ] 対象ユースケースを実行してメッセージの流れを確認した
- [ ] ベンチマーク結果を記録した（必要な場合）
- [ ] 新ユースケース追加時はテストを追加した
