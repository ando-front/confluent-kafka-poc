# 01 — Basic Pub/Sub

> **難易度**: ⭐（初級）
> **所要時間**: 約 10 分
> **前提**: [docs/getting-started.md](../../docs/getting-started.md) の環境構築が完了していること

---

## このユースケースで学べること

- Kafka の「メッセージを送る（produce）・受け取る（consume）」の基本
- **トピック**（メッセージの入れ物）の概念
- **コンシューマーグループ**（複数の受け取り手でメッセージを分担する仕組み）
- **パーティション**（並列処理のための分割）の挙動

---

## ビジネス上の意義

「注文が入ったら、在庫サービス・通知サービス・分析サービスの 3 つに伝えたい」

Kafka を使わない場合、注文サービスが全サービスに直接 API コールしなければなりません。
サービスが増えるたびに注文サービスを変更する必要があり、1 つのサービスが落ちると連鎖障害が起きます。

```
【Kafka なし】                    【Kafka あり】
注文サービス → 在庫API             注文サービス
注文サービス → 通知API     →→→         ↓ produce
注文サービス → 分析API             [Topic: orders]
                                        ↓
                                 ├→ 在庫サービス (subscribe)
                                 ├→ 通知サービス (subscribe)
                                 └→ 分析サービス (subscribe)
```

**Kafka を挟むことで**: 注文サービスは Kafka にメッセージを投げるだけ。
受け取る側は好きなタイミングで好きなだけ読めます。

---

## 仕組み（図解）

```
┌──────────────────┐   produce    ┌────────────────────────────────────┐
│   producer.py    │ ──────────→  │   Topic: pubsub.orders.v1          │
│ (注文を送る側)    │              │                                    │
└──────────────────┘              │  Partition 0: [msg0][msg3][msg6].. │
                                  │  Partition 1: [msg1][msg4][msg7].. │
                                  │  Partition 2: [msg2][msg5][msg8].. │
                                  └────────────────────────────────────┘
                                         ↓ consume
                                  ┌──────────────────┐
                                  │   consumer.py    │
                                  │ (注文を受け取る側) │
                                  └──────────────────┘
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `pubsub.orders.v1` | 注文イベント |

---

## 実行手順

### ステップ 1: コンシューマーを起動（まず受け取る側から）

ターミナルを 2 つ開いてください。

**ターミナル A**:
```bash
python use_cases/01_basic_pubsub/consumer.py
```

このような出力が出て、メッセージを待機します:
```
[INFO] Consumer started. Group: pubsub-group. Waiting for messages...
```

### ステップ 2: プロデューサーを起動（メッセージを送る）

**ターミナル B**:
```bash
# 1秒ごとにメッセージを送り続ける（Ctrl-C で停止）
python use_cases/01_basic_pubsub/producer.py

# 10件だけ送って終了したい場合
python use_cases/01_basic_pubsub/producer.py --count 10

# 送信間隔を変える（0.5秒ごと）
python use_cases/01_basic_pubsub/producer.py --interval 0.5
```

プロデューサーの出力:
```
[INFO] Produced: {"order_id": "abc123", "item": "laptop", "price": 98000, "timestamp": "..."}
[INFO] Produced: {"order_id": "def456", "item": "mouse", "price": 3200, "timestamp": "..."}
...
```

**ターミナル A** に受信メッセージが表示されます:
```
[INFO] Received [partition=0, offset=0]: {"order_id": "abc123", "item": "laptop", ...}
[INFO] Received [partition=1, offset=0]: {"order_id": "def456", "item": "mouse", ...}
...
```

---

## Kafka の中身を確認する（kafka-ui）

ブラウザで http://localhost:8080 を開きます。

1. 左メニュー「Topics」をクリック
2. `pubsub.orders.v1` をクリック
3. 「Messages」タブで、送信されたメッセージの内容・オフセット・パーティションを確認

---

## コンシューマーグループの挙動を試す

### 同じグループ内でスケールアウト（メッセージを分担）

**ターミナル A**:
```bash
python use_cases/01_basic_pubsub/consumer.py --group my-group
```

**ターミナル C**（新しいターミナル）:
```bash
python use_cases/01_basic_pubsub/consumer.py --group my-group
```

**結果**: 2 つのコンシューマーがパーティションを分担し、
1 つのメッセージはどちらか一方のみが受け取ります（負荷分散）。

### 異なるグループで全メッセージを受信（fan-out）

```bash
python use_cases/01_basic_pubsub/consumer.py --group group-inventory
python use_cases/01_basic_pubsub/consumer.py --group group-notification
```

**結果**: 両方のグループが全メッセージを受け取ります。
在庫サービスと通知サービスが同じ注文イベントをそれぞれ処理するイメージです。

---

## Kafka の重要な特性を確認する

```bash
# 1. コンシューマーを停止する（Ctrl-C）
# 2. プロデューサーを動かし続ける（メッセージが溜まる）
# 3. コンシューマーを再起動する
python use_cases/01_basic_pubsub/consumer.py --group my-group

# → 溜まっていたメッセージが一気に処理される！
# → Kafka はメッセージを保持するので、Consumer がダウンしていても消えない
```

---

## よくある疑問

**Q: コンシューマーを起動したら何も表示されない**
A: プロデューサーがまだ起動していません。別ターミナルで `producer.py` を実行してください。

**Q: パーティション数はどこで決まる？**
A: `core/admin.py` の `DEFAULT_TOPIC_PARTITIONS=3`（`.env` で変更可）。

**Q: コンシューマーが止まったときメッセージは消える？**
A: 消えません。Kafka に保持されており、再起動後に続きから読めます（デフォルト 7 日間保持）。

---

## 次のユースケース

基本を理解したら → [02_event_sourcing](../02_event_sourcing/)（イベントの履歴管理）
