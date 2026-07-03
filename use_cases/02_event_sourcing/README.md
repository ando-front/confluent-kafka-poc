# 02 — Event Sourcing

> **難易度**: ⭐⭐（初中級）
> **所要時間**: 約 15 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) を完了していること

---

## このユースケースで学べること

- **イベントソーシング**（状態ではなく「変化の記録」を保存する設計パターン）
- Kafka がどのようにイベントの**追記専用ログ**として機能するか
- **キー**（Key）によるパーティション割り当てと順序保証
- 過去のイベントを畳み込んで**現在の状態を再構築**する方法

---

## ビジネス上の意義

通常のデータベースは「今の状態」しか記録しません。

```
通常の DB:
  orders テーブル
  ┌──────┬──────────┬────────┐
  │ id   │ status   │ amount │
  │ #123 │ shipped  │ 98000  │
  └──────┴──────────┴────────┘
  ← なぜ shipped になったか、いつ支払いがあったか、わからない
```

Event Sourcing では「状態を変化させたイベントの連なり」を保存します:

```
Kafka Topic (append-only ログ):
  order_id=123: [ORDER_CREATED] → [PAYMENT_RECEIVED] → [SHIPPED]
                     ↑                   ↑                  ↑
               2026/7/1 10:00      2026/7/1 11:30    2026/7/2 09:00

  現在の状態は「全イベントを最初から読んで畳み込む」ことで再構築
```

**メリット**:
- **完全な監査証跡**: 「なぜ今この状態か」を全イベントで説明できる（金融・EC・在庫管理で重要）
- **時間旅行**: 昨日の 14:00 時点の状態を再現できる
- **What-if 分析**: 「もし支払いが届かなかったら？」をシミュレーションできる（実データは変更なし）

---

## 仕組み（図解）

```
┌──────────────────┐
│   event_store.py │
│  (注文イベントを   │
│   Kafka に追記)   │
└──────────────────┘
         │
         │ produce（キー=注文ID、同じ注文は同じパーティションへ）
         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Topic: eventsourcing.orders.v1（append-only）                    │
│                                                                    │
│  Partition 0（order_id=123 のイベントが順番に入る）                 │
│  offset=0: {id:123, type:ORDER_CREATED,    ts:10:00, amount:98000}│
│  offset=1: {id:123, type:PAYMENT_RECEIVED, ts:11:30}              │
│  offset=2: {id:123, type:SHIPPED,          ts:09:00+1d}           │
│                                                                    │
│  Partition 1（order_id=456 のイベント）                             │
│  offset=0: {id:456, type:ORDER_CREATED,    ts:10:05, amount:3200} │
│  offset=1: {id:456, type:ORDER_CANCELLED,  ts:10:30}              │
└──────────────────────────────────────────────────────────────────┘
         │
         │ consume + fold（畳み込み）
         ▼
┌──────────────────┐
│   replay.py      │
│  現在状態を再構築  │
│  order#123: shipped│
│  order#456: cancelled│
└──────────────────┘
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `eventsourcing.orders.v1` | 注文ライフサイクルイベント（追記のみ） |

イベントは `aggregate_id`（注文ID）でキー付けされるため、同一注文のイベントは
同じパーティションに入り、**順序が保証**されます。

---

## 実行手順

### ステップ 1: イベントを生成して状態を再構築

```bash
python use_cases/02_event_sourcing/event_store.py
```

期待される出力:
```
[INFO] Appended: ORDER_CREATED    order_id=abc123 amount=98000
[INFO] Appended: PAYMENT_RECEIVED order_id=abc123
[INFO] Appended: SHIPPED          order_id=abc123

=== Current State ===
order_id : abc123
status   : shipped
amount   : 98000
events   : 3
```

### ステップ 2: 特定注文の現在状態を再構築

```bash
python use_cases/02_event_sourcing/event_store.py --aggregate <order_id>
# <order_id> は ステップ1 で表示されたものを使用
```

### ステップ 3: 全履歴をリプレイ

```bash
python use_cases/02_event_sourcing/replay.py
```

期待される出力（全注文の最終状態一覧）:
```
=== Replay Result ===
order_id=abc123  status=shipped   events=3
order_id=def456  status=cancelled events=2
...
```

### ステップ 4: 時間旅行（特定時刻以前のイベントだけで再構築）

```bash
# 例: 今日の 10:00 以前のイベントだけを読む
python use_cases/02_event_sourcing/replay.py --since 2026-07-03T10:00:00+00:00
```

### ステップ 5: What-if 分析

```bash
# 「もし支払いイベントがなかったら？」をシミュレーション
python use_cases/02_event_sourcing/replay.py --whatif drop-payments
```

---

## kafka-ui で確認

1. http://localhost:8080 → 「Topics」→ `eventsourcing.orders.v1`
2. 「Messages」タブで各イベントの内容・キー・パーティション・オフセットを確認
3. 同じ `order_id` のメッセージが同じパーティションに入っていることを確認

---

## よくある疑問

**Q: なぜ状態を更新するのではなく、イベントを追記するのか？**
A: 更新すると「なぜそうなったか」の履歴が消えます。Kafka のトピックは追記専用なので、
イベントソーシングと非常に相性が良いです。

**Q: データが増え続けるのでは？**
A: はい、増えます。実運用では「スナップショット」（定期的にその時点の状態を保存）と
組み合わせて、古いイベントを削除するか、スナップショット以降だけリプレイします。

**Q: CQRS との関係は？**
A: Event Sourcing（書き込み）と読み取り用ビュー（CQRS の Query 側）を組み合わせることで、
読み書きを完全に分離できます。このリポジトリでは Event Sourcing 部分だけを実装しています。

---

## 次のユースケース

- [03_stream_processing](../03_stream_processing/)（リアルタイム集計・変換）
- [04_cdc](../04_cdc/)（DB 変更を Kafka に流す）
