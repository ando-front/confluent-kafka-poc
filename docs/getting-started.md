# 環境構築ガイド

> Kafka を初めて動かすための詳細な手順書です。
> 詰まったときは [docs/troubleshooting.md](troubleshooting.md) を参照してください。

---

## 前提条件

### 1. Docker Desktop のインストール

Docker は「アプリを仮想コンテナで動かす」ツールです。
Kafka のような複雑なミドルウェアを手元で簡単に起動できます。

**インストール**:
- Mac: https://docs.docker.com/desktop/install/mac-install/
- Windows: https://docs.docker.com/desktop/install/windows-install/
- Linux: https://docs.docker.com/desktop/install/linux-install/

**確認**:
```bash
docker --version
# → Docker version 24.x.x 以上ならOK

docker compose version
# → Docker Compose version v2.x.x 以上ならOK
```

> ⚠️ Docker Desktop が起動していないと Kafka が立ち上がりません。
> タスクバー（Mac なら画面上部のメニューバー）に Docker のクジラアイコンがあれば起動中です。

---

### 2. Python 3.11+ のセットアップ

**確認**:
```bash
python --version
# → Python 3.11.x 以上ならOK
# Python 3.x と表示されない場合は python3 --version を試す
```

**インストール（まだ入っていない場合）**:
- 公式: https://www.python.org/downloads/
- Mac（pyenv 経由）:
  ```bash
  brew install pyenv
  pyenv install 3.11.9
  pyenv global 3.11.9
  ```

---

### 3. Git のセットアップ

```bash
git --version
# → git version 2.x.x ならOK
```

---

## セットアップ手順

### Step 1: リポジトリを取得

```bash
git clone https://github.com/<YOUR_ORG>/confluent-kafka-poc.git
cd confluent-kafka-poc
```

### Step 2: Python パッケージをインストール

```bash
pip install -r requirements.txt
```

主要なパッケージ:
- `confluent-kafka`: Kafka クライアントライブラリ本体
- `pydantic-settings`: `.env` ファイルから設定を読み込む
- `rich`: コンソールの見やすい表示（07_real_time_analytics で使用）

### Step 3: 設定ファイルをコピー

```bash
cp .env.tpl .env
```

`.env` ファイルは Kafka の接続先などを設定するファイルです。
ローカル開発ではデフォルト値のまま動きます（変更不要）。

```ini
# .env の主要項目（デフォルト値）
KAFKA_ENV=local                          # ローカル Docker を使う
KAFKA_BOOTSTRAP_SERVERS=localhost:9092   # Kafka の接続先
SCHEMA_REGISTRY_URL=http://localhost:8081
```

---

## Kafka の起動

```bash
./scripts/start.sh
```

このコマンドは内部で以下を実行します:

```
1. docker compose up -d  — Docker コンテナを起動
   └── ZooKeeper  (:2181) ... Kafka クラスターの管理情報を保持
   └── Kafka      (:9092) ... メッセージブローカー本体
   └── Schema Registry (:8081) ... メッセージのスキーマ管理
   └── Kafka Connect   (:8083) ... 外部システムとの連携
   └── Control Center  (:9021) ... Confluent の管理 UI
   └── kafka-ui        (:8080) ... シンプルな管理 UI

2. ヘルスチェック（最大60秒待機）
   └── Kafka が実際に応答できるまで確認

3. 成功メッセージと URL を表示
```

**起動成功の確認**:

```
✅ Kafka is ready!
   kafka-ui       : http://localhost:8080
   Control Center : http://localhost:9021
```

**ブラウザで確認**:

`http://localhost:8080` を開くと kafka-ui が表示されます。
左メニューの「Topics」をクリックして、トピック一覧が見えれば準備完了です。

---

## 最初のデモを動かす

2 つのターミナルウィンドウを開いてください。

**ターミナル A（メッセージを受け取る側）**:
```bash
cd confluent-kafka-poc
python use_cases/01_basic_pubsub/consumer.py
```

待機状態になります（メッセージが来るまで表示は止まります）:
```
[2026-xx-xx] Consumer started. Waiting for messages...
```

**ターミナル B（メッセージを送る側）**:
```bash
cd confluent-kafka-poc
python use_cases/01_basic_pubsub/producer.py
```

送信開始:
```
[2026-xx-xx] Produced: order_id=abc123, item=laptop, price=98000
[2026-xx-xx] Produced: order_id=def456, item=mouse, price=3200
...
```

**ターミナル A** に受信メッセージが流れてきたら成功です:
```
[2026-xx-xx] Received: order_id=abc123, item=laptop, price=98000
[2026-xx-xx] Received: order_id=def456, item=mouse, price=3200
...
```

`Ctrl + C` でどちらも停止できます。

---

## kafka-ui の使い方

`http://localhost:8080` にアクセスして Kafka の中身を視覚的に確認できます。

```
左メニュー:
  Topics     ─── 「pubsub.orders.v1」などのトピック一覧
  Consumers  ─── コンシューマーグループとオフセットの状態
  Brokers    ─── Kafka サーバーの状態
```

**メッセージを確認する方法**:
1. 「Topics」をクリック
2. `pubsub.orders.v1` をクリック
3. 「Messages」タブをクリック
4. メッセージの内容・オフセット・タイムスタンプを確認できます

---

## 停止・クリーンアップ

```bash
# Kafka を停止（データは保持）
./scripts/stop.sh

# Kafka を停止してデータも全削除（やり直したいとき）
./scripts/reset.sh
```

---

## Confluent Cloud（本番 SaaS）への切り替え

ローカルの Docker ではなく、Confluent Cloud（マネージドサービス）を使う場合は
`.env` を以下のように書き換えます。コードの変更は不要です。

```ini
# .env
KAFKA_ENV=confluent
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<CLUSTER_API_KEY>      ← Confluent Cloud の API キー
KAFKA_SASL_PASSWORD=<CLUSTER_API_SECRET>   ← Confluent Cloud の API シークレット
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<SR_KEY>
SCHEMA_REGISTRY_API_SECRET=<SR_SECRET>
```

---

## 次のステップ

- [docs/use-cases-guide.md](use-cases-guide.md) — 各ユースケースの詳細解説
- [docs/kafka-concepts.md](kafka-concepts.md) — Kafka の基本概念をより深く理解する
- [docs/troubleshooting.md](troubleshooting.md) — エラーが出たときの対処法
