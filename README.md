# confluent-kafka-poc

Apache Kafka / Confluent Platform の **PoC（実証実験）基盤**。

「Kafka でこのパターンは実現できるか？」という問いに対して、すぐ動くコードで答えます。

> **初めて Kafka を触る方へ**: まず [`docs/kafka-concepts.md`](docs/kafka-concepts.md) を読んで
> 基本概念を把握してから、[`docs/getting-started.md`](docs/getting-started.md) で環境を構築してください。

---

## Kafka って何？（1分で理解）

```
【Kafka がない世界】

サービスA ──→ サービスB
サービスA ──→ サービスC   ← Aが壊れたら B/C も困る
サービスA ──→ サービスD

【Kafka を挟んだ世界】

サービスA ──→ [Kafka] ──→ サービスB
                    ├──→ サービスC   ← AとB/C/Dが独立
                    └──→ サービスD
```

Kafka は「メッセージの巨大な郵便局」です。送る側（Producer）はポストに手紙を投函するだけ。
受け取る側（Consumer）は自分のペースで取りに行けます。お互いが相手のことを知る必要はありません。

---

## このリポジトリでできること

| # | ユースケース | 「こんなときに使う」 | 難易度 |
|---|------------|------------------|------|
| [01](use_cases/01_basic_pubsub/) | Basic Pub/Sub | サービス間でイベントを非同期に送りたい | ⭐ |
| [02](use_cases/02_event_sourcing/) | Event Sourcing | 「なぜ今この状態か」を全履歴で説明したい | ⭐⭐ |
| [03](use_cases/03_stream_processing/) | Stream Processing | リアルタイムに集計・変換・結合したい | ⭐⭐⭐ |
| [04](use_cases/04_cdc/) | CDC | DBの変更を他システムへ即座に伝えたい | ⭐⭐ |
| [05](use_cases/05_dead_letter_queue/) | Dead Letter Queue | 壊れたメッセージでシステム全体を止めたくない | ⭐⭐ |
| [06](use_cases/06_exactly_once/) | Exactly-Once | 二重処理を絶対に防ぎたい（金融・課金） | ⭐⭐⭐ |
| [07](use_cases/07_real_time_analytics/) | Real-Time Analytics | 「今この瞬間」のKPIをダッシュボードで見たい | ⭐⭐ |

**推奨学習順序**: 01 → 02 → 04 → 05 → 07 → 03 → 06

---

## 必要なもの

| ソフトウェア | 確認コマンド | 最低バージョン |
|-----------|------------|-------------|
| Docker Desktop | `docker --version` | 24.0+ |
| Python | `python --version` | 3.11+ |
| Git | `git --version` | 2.x |

Docker Desktop のインストール: https://www.docker.com/products/docker-desktop/

---

## 5分で動かす（クイックスタート）

```bash
# 1. リポジトリを取得
git clone https://github.com/<YOUR_ORG>/confluent-kafka-poc.git
cd confluent-kafka-poc

# 2. Python パッケージをインストール
pip install -r requirements.txt

# 3. 設定ファイルをコピー（デフォルトのままで動きます）
cp .env.tpl .env

# 4. Kafka を起動（初回は Docker イメージのダウンロードに数分かかります）
./scripts/start.sh
```

起動が成功すると以下のメッセージが表示されます:

```
✅ Kafka is ready!
   kafka-ui : http://localhost:8080
   Control Center : http://localhost:9021
```

```bash
# 5. ブラウザで http://localhost:8080 を開いてトピック一覧を確認

# 6. 最初のデモを動かす（ターミナルを 2 つ開いてください）

# ターミナル A: メッセージを受け取る側
python use_cases/01_basic_pubsub/consumer.py

# ターミナル B: メッセージを送る側
python use_cases/01_basic_pubsub/producer.py
```

ターミナル A に注文メッセージが流れてきたら成功です。

```bash
# 7. 終了するとき
./scripts/stop.sh
```

---

## ドキュメント

| ドキュメント | 内容 |
|-----------|------|
| [docs/kafka-concepts.md](docs/kafka-concepts.md) | Kafka の基本概念（トピック・パーティション・オフセットなど） |
| [docs/getting-started.md](docs/getting-started.md) | 環境構築の詳細手順 |
| [docs/use-cases-guide.md](docs/use-cases-guide.md) | 全7ユースケースの詳細解説 |
| [docs/troubleshooting.md](docs/troubleshooting.md) | よくあるエラーと解決法 |

---

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Compose                       │
│  ┌──────────┐  ┌───────┐  ┌─────────────────────────┐  │
│  │ZooKeeper │  │ Kafka │  │    Schema Registry       │  │
│  │  :2181   │  │ :9092 │  │        :8081             │  │
│  └──────────┘  └───────┘  └─────────────────────────┘  │
│                            ┌─────────────────────────┐  │
│                            │   Kafka Connect :8083    │  │
│                            └─────────────────────────┘  │
│  ┌─────────────────────┐  ┌─────────────────────────┐  │
│  │  kafka-ui :8080     │  │  Control Center :9021   │  │
│  └─────────────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
          ▲
          │ confluent-kafka (Python)
          ▼
┌─────────────────────────────────────────────────────────┐
│                      core/                               │
│  BaseProducer  BaseConsumer  AdminClient  Config         │
└─────────────────────────────────────────────────────────┘
          ▲
          │ 継承
          ▼
┌─────────────────────────────────────────────────────────┐
│                    use_cases/                            │
│  01_basic_pubsub  02_event_sourcing  03_stream_proc...   │
└─────────────────────────────────────────────────────────┘
```

---

## よくある質問

**Q: Kafka を使ったことがなくても大丈夫？**
A: 大丈夫です。[docs/kafka-concepts.md](docs/kafka-concepts.md) から始めてください。

**Q: どのユースケースから始めればいい？**
A: `01_basic_pubsub` が最もシンプルです。まずこれで「メッセージが流れる感覚」を掴みましょう。

**Q: Confluent Cloud（本番 SaaS）に切り替えるには？**
A: `.env` を書き換えるだけです。コードの変更は不要です。

```ini
# .env
KAFKA_ENV=confluent
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<CLUSTER_API_KEY>
KAFKA_SASL_PASSWORD=<CLUSTER_API_SECRET>
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<SR_KEY>
SCHEMA_REGISTRY_API_SECRET=<SR_SECRET>
```

**Q: 新しいユースケースを追加したい**

```bash
mkdir use_cases/08_my_use_case
# README.md / producer.py / consumer.py を追加
# core/BaseProducer, BaseConsumer を継承
# tests/test_use_cases.py にスモークテスト追加
```

**Q: エラーが出た**
A: [docs/troubleshooting.md](docs/troubleshooting.md) を確認してください。

---

## テスト・ベンチマーク

```bash
# テスト（Kafka 起動後に実行）
pytest tests/ -v

# スループット測定（10万メッセージ）
python benchmarks/throughput_producer.py
python benchmarks/throughput_consumer.py

# レイテンシ測定 P50/P95/P99
python benchmarks/latency_test.py

# マークダウンレポート生成
python benchmarks/report.py
```
