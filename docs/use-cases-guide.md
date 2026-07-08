# ユースケース詳細ガイド

> 全 7 ユースケースをビジネスの文脈から解説します。
> 初めての方は [01_basic_pubsub](#01--basic-pubsub-) から順番に進めることをお勧めします。

---

## 01 — Basic Pub/Sub ⭐

### どんなときに使うか

「注文が入ったら在庫サービス・通知サービス・分析サービスに伝えたい」

Kafka を使わない場合、注文サービスは在庫・通知・分析それぞれに直接 API コールが必要です。
サービスが増えるたびに注文サービスの変更が必要になり、連鎖障害のリスクも高まります。

**Kafka を挟むことで**:
- 注文サービスは Kafka にメッセージを投げるだけ
- 受け取る側（Subscriber）は注文サービスを知らなくて済む
- 受け取る側が増えても注文サービスを変更しなくて良い

### 仕組み

```
[注文サービス]
    │ produce
    ▼
[Topic: pubsub.orders.v1]
    │
    ├─→ consume → [Consumer Group: inventory-service]  ← 在庫サービス
    ├─→ consume → [Consumer Group: notification-service] ← 通知サービス
    └─→ consume → [Consumer Group: analytics-service]  ← 分析サービス
```

### 実行手順

```bash
# ターミナル 1: コンシューマーを起動（メッセージを待機）
python use_cases/01_basic_pubsub/consumer.py

# ターミナル 2: プロデューサーを起動（1秒ごとに送信、Ctrl-C で停止）
python use_cases/01_basic_pubsub/producer.py

# 10件だけ送って終了したい場合
python use_cases/01_basic_pubsub/producer.py --count 10

# 別のコンシューマーグループで全メッセージを受信（fan-out の体験）
python use_cases/01_basic_pubsub/consumer.py --group another-group
```

### 確認ポイント

- 同じ `--group` のコンシューマーを 2 つ起動すると、パーティションが分担されます（スケールアウト）
- 異なる `--group` のコンシューマーはそれぞれが全メッセージを受け取ります（fan-out）
- kafka-ui (http://localhost:8080) の「Consumers」タブでオフセットの進みを確認できます

### 発展課題

```bash
# 1. コンシューマーを先に止めて、プロデューサーを動かし続ける
#    → コンシューマーを再起動すると、溜まったメッセージが一気に流れてくる（Kafka の重要な特性）

# 2. 同じグループ名で 3 つのコンシューマーを起動してみる
#    → パーティション数（3）と同数なので 1 対 1 対応になる
#    → 4 つ目のコンシューマーを起動すると「アイドル」になる（パーティション数が上限）
python use_cases/01_basic_pubsub/consumer.py --group test-scale &
python use_cases/01_basic_pubsub/consumer.py --group test-scale &
python use_cases/01_basic_pubsub/consumer.py --group test-scale &
```

---

## 02 — Event Sourcing ⭐⭐

### どんなときに使うか

「注文が今なぜキャンセル状態なのか、履歴を追って説明できるようにしたい」

通常のデータベースは「現在の状態」しか記録しません。
Event Sourcing では「状態を変化させたイベントの履歴」を保存し、現在の状態はそこから再構築します。

**活用場面**:
- 金融（送金・取引の全履歴）
- EC（注文ライフサイクルの追跡）
- 在庫管理（誰がいつ変更したかの監査）

### 仕組み

```
                    [Topic: eventsourcing.orders.v1]（append-only）
                    ┌─────────────────────────────────────────────┐
  注文#123 の履歴 → │[ORDER_CREATED] [PAYMENT_RECEIVED] [SHIPPED] │
                    └─────────────────────────────────────────────┘
                              ↓ fold（畳み込み）
                         注文#123 の現在状態: "shipped"
```

イベントは追記のみ（変更・削除なし）。何度再構築しても同じ結果になります。

### 実行手順

```bash
# サンプルの注文イベント履歴を生成し、現在状態を再構築して表示
python use_cases/02_event_sourcing/event_store.py

# 特定注文のイベント履歴をリプレイ（過去の状態を再現）
python use_cases/02_event_sourcing/replay.py
```

### 確認ポイント

- 同じ注文 ID のイベントは必ず同じパーティションに入ります（キー = 注文ID）
- `replay.py` で任意の時点まで巻き戻した状態を確認できます

### 試してみよう

- イベントを途中で止めて再開すると、続きから処理される様子を確認する
- `--since` オプションで「3時間前の注文状態」を再現してみる

---

## 03 — Stream Processing ⭐⭐⭐

### どんなときに使うか

「売上を1分ごとに集計してアラートを出したい」「特定条件のメッセージだけ別ルートに流したい」「注文と支払いのストリームを突き合わせたい」

### 3 種類のストリーム処理

**① タンブリングウィンドウ集計（30秒ごとの売上集計）**

```
  Stream: transactions
  ─────────────────────────────────────────────────────
  時刻:   0s    10s   20s   30s   40s   50s   60s
  金額:  [100] [200] [150]       [300] [120]  [80]
         └────── 30秒ウィンドウ1 ──────┘└─── 30秒ウィンドウ2 ───
         集計: 450円                    集計: 500円
```

**② フィルタ・変換**

```
  全取引ストリーム → [10,000円以上をフィルタ] → 高額取引ストリーム
                  → [税込み価格に変換]        → 変換済みストリーム
```

**③ ストリーム結合（注文 × 支払いの突き合わせ）**

```
  orders ──→  5分間の    → [支払い済み注文]
              ウィンドウ
  payments ─→ で結合
```

### 実行手順

```bash
# 集計（30秒タンブリングウィンドウ）
python use_cases/03_stream_processing/filter_transform.py --produce 300  # 取引データを生成
python use_cases/03_stream_processing/aggregator.py                        # 30秒ごとに集計

# フィルタ・変換
python use_cases/03_stream_processing/filter_transform.py

# ストリーム結合（2つのターミナルで）
python use_cases/03_stream_processing/stream_join.py --produce-orders
python use_cases/03_stream_processing/stream_join.py --join
```

---

## 04 — CDC（Change Data Capture）⭐⭐

### どんなときに使うか

「DBを変更したとき、Elasticsearchの検索インデックスも自動で更新したい」

CDC（Change Data Capture）とは、データベースの変更（INSERT/UPDATE/DELETE）を
リアルタイムでイベントとして捕捉する仕組みです。

**DB を直接 poll する問題**: 差分を取るには「最終更新時刻」などで比較が必要で、
削除されたレコードは検知できません。

**CDC の解決策**: 変更が起きた瞬間にイベントが発行されます。

### 仕組み（Debezium 形式）

```
  [source.db] に変更発生
         │
         ▼
  Kafka Topic: cdc.orders.v1
  ┌──────────────────────────────────────────────────────────┐
  │ {                                                         │
  │   "op": "u",        ← u=update, c=create, d=delete       │
  │   "before": { "status": "pending" },   ← 変更前          │
  │   "after":  { "status": "shipped" }    ← 変更後          │
  │ }                                                         │
  └──────────────────────────────────────────────────────────┘
         │
         ▼
  [CDCコンシューマー] → replica.db に反映
```

### 実行手順

```bash
# ターミナル 1: レプリケーター（変更を replica.db に反映）
python use_cases/04_cdc/cdc_consumer.py

# ターミナル 2: DB 変更シミュレーター
python use_cases/04_cdc/simulator.py
```

### 確認ポイント

- `op: "c"` (create) → `op: "u"` (update) → `op: "d"` (delete) の順番を確認
- `source.db` の変更が `replica.db` にリアルタイムで反映されることを確認

---

## 05 — Dead Letter Queue（DLQ）⭐⭐

### どんなときに使うか

「壊れたメッセージが1件あっても、残りの処理を止めたくない」

外部から受け取るデータは必ずしも正しい形式とは限りません。
不正なメッセージで Consumer がクラッシュし、処理全体が止まるリスクを避けます。

### 仕組み

```
  [Topic: orders]
  ┌───────────────────────────────────────────────────────────┐
  │ [正常] [正常] [❌壊れた] [正常] [❌壊れた] ...             │
  └───────────────────────────────────────────────────────────┘
         │
         ▼
  [Main Consumer]
    正常メッセージ → 通常処理
    不正メッセージ → [Topic: orders.dlq.v1] へ退避
                         │
                         ▼
                   [DLQ Processor]
                     修復可能 → orders トピックに再投入
                     修復不能 → poison ログに記録
```

### 実行手順

```bash
# ターミナル 1: メインコンシューマー（失敗を DLQ へルーティング）
python use_cases/05_dead_letter_queue/consumer.py

# ターミナル 2: DLQ プロセッサー（復旧・再投入）
python use_cases/05_dead_letter_queue/dlq_processor.py

# ターミナル 3: 20% の不正メッセージを含む注文を送信
python use_cases/05_dead_letter_queue/producer.py --count 50
```

### 確認ポイント

- メインコンシューマーが止まらずに処理を続けることを確認
- kafka-ui で `orders.dlq.v1` トピックに不正メッセージが入っていることを確認
- DLQ プロセッサーが一部を復旧して orders トピックに再投入することを確認

---

## 06 — Exactly-Once Semantics（EOS）⭐⭐⭐

### どんなときに使うか

「送金メッセージを絶対に2回処理してはいけない」

ネットワーク障害やリトライによって同じメッセージが複数回届くことがあります。
金融・課金・在庫では「二重処理」は深刻な問題です。

### 仕組み

**Producer 側（トランザクション）**:
```
begin_transaction()
  produce(msg1)
  produce(msg2)
  produce(msg3)
commit_transaction()   ← 全部コミットされるか、全部ロールバック（原子性）
```

**Consumer 側（冪等処理）**:
```
メッセージを受信
  → 処理済み ID を SQLite で確認
  → 未処理なら処理して ID を記録
  → 処理済みなら読み飛ばす（重複を無視）
```

### 実行手順

```bash
# ターミナル 1: 冪等コンシューマー
python use_cases/06_exactly_once/idempotent_consumer.py

# ターミナル 2: 5件を1トランザクションでコミット
python use_cases/06_exactly_once/transactional_producer.py --batch 5

# 途中で失敗させて全件 abort（コンシューマーには1件も届かない）
python use_cases/06_exactly_once/transactional_producer.py --batch 5 --fail
```

### 確認ポイント

- `--fail` オプションで途中に失敗を起こしたとき、コンシューマーに何も届かないことを確認
- 同じメッセージを再送しても、コンシューマーが2回目を無視することを確認

---

## 07 — Real-Time Analytics ⭐⭐

### どんなときに使うか

「セール開始直後のリアルタイム CVR を見たい」「今この瞬間の GMV をダッシュボードで確認したい」

バッチ集計（翌日レポート）ではなく「今」のKPIをストリームで計算します。

### 仕組み

```
  [EC イベント生成]
  page_view / add_to_cart / purchase
         │ produce
         ▼
  [Topic: analytics.events.v1]
         │ consume
         ▼
  [Aggregator: 1分ウィンドウで集計]
         │ produce
         ▼
  [Topic: analytics.kpi_1m.v1]
         │ consume
         ▼
  [Dashboard: Rich コンソール表示]

  ┌──────────────────────────────────────┐
  │     📊 Real-Time Analytics           │
  │  Page views  : 1,423                 │
  │  Purchases   : 89                    │
  │  GMV         : ¥1,234,500            │
  │  CVR         : 6.3%                  │
  │  Events/sec  : 48.2                  │
  └──────────────────────────────────────┘
```

### 実行手順

3 つのターミナルを開いてください:

```bash
# ターミナル 1: ダッシュボード（KPIを表示）
python use_cases/07_real_time_analytics/dashboard.py

# ターミナル 2: 集計エンジン（1分ウィンドウで集計）
python use_cases/07_real_time_analytics/aggregator.py

# ターミナル 3: イベント生成（EC サイトのアクセスをシミュレート）
python use_cases/07_real_time_analytics/event_generator.py
```

### 確認ポイント

- ダッシュボードが1秒ごとに更新されることを確認
- `event_generator.py` の送信速度を上げると `Events/sec` が変化することを確認
- kafka-ui で `analytics.events.v1` トピックへのメッセージ流量を確認

---

## 推奨学習パス

```
初心者
  ↓
  01 Basic Pub/Sub   ── Kafka の基本（トピック・コンシューマーグループ）
  ↓
  02 Event Sourcing  ── イベント設計の考え方
  ↓
  04 CDC             ── DB と Kafka の連携
  ↓
  05 DLQ             ── 本番で必須のエラー処理パターン
  ↓
  07 Analytics       ── ストリーム集計の体感
  ↓
  03 Stream Proc.    ── より高度な変換・結合
  ↓
  06 Exactly-Once    ── トランザクション・金融グレードの保証
  ↓
上級者
```
