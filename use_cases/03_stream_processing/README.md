# 03 — Stream Processing

> **難易度**: ⭐⭐⭐（中級）
> **所要時間**: 約 20 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) を完了していること

---

## このユースケースで学べること

- **ウィンドウ集計**（時間の窓ごとにデータを集計する方法）
- **フィルタ・変換**（条件に合うメッセージだけを別トピックに流す）
- **ストリーム結合**（2 つのトピックのメッセージを突き合わせる）
- Kafka Streams / ksqlDB が本番で担う処理の概念を理解する

---

## ビジネス上の意義

リアルタイムのデータパイプラインでは、メッセージをそのまま流すだけでなく
「変換・集計・結合」が必要になることがほとんどです。

| 処理 | 例 |
|-----|---|
| ウィンドウ集計 | 30秒ごとの売上合計 → アラート・ダッシュボード |
| フィルタ・変換 | 10,000円以上の取引だけ → 不正検知パイプライン |
| ストリーム結合 | 注文 × 支払い → 「支払い済み注文」ストリーム |

---

## 仕組み

### ① タンブリングウィンドウ集計

```
取引ストリーム（金額）:
───────────────────────────────────────────────────────
  t=0s   t=10s  t=20s  t=30s  t=40s  t=50s  t=60s
  [100]  [200]  [150]         [300]  [120]   [80]

  └────────── 30秒ウィンドウ ──────┘└──── 30秒ウィンドウ ──
     集計: 450円（window 1）             集計: 500円（window 2）
```

### ② フィルタ・変換

```
全取引ストリーム
  → [金額 > 10,000 をフィルタ] → 高額取引専用ストリーム
  → [税込み価格に変換]          → 変換済みストリーム
```

### ③ ストリーム結合（5分ウィンドウ）

```
orders ストリーム  ─→ │5分間の       │ → 支払い済み注文
payments ストリーム ─→ │ウィンドウで結合│
                      └─────────────┘
  ※ 5分以内に対応する支払いが届いた注文だけが結合される
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `stream.transactions.v1` | 入力: 取引ストリーム |
| `stream.sales_30s.v1` | 出力: 30秒窓の売上集計 |
| `stream.high_value.v1` | 出力: 10,000円以上の高額取引 |
| `stream.order_events.v1` / `stream.payment_events.v1` | 結合の入力 |
| `stream.order_payment_joined.v1` | 出力: 結合結果 |

---

## 実行手順

### パターン 1: ウィンドウ集計（30秒ごとに売上合計を計算）

```bash
# ターミナル A: 集計を開始（30秒ごとに合計を出力）
python use_cases/03_stream_processing/aggregator.py --window 30

# ターミナル B: サンプル取引データを生成
python use_cases/03_stream_processing/filter_transform.py --produce 300
```

期待される出力（ターミナル A）:
```
[INFO] Window closed: start=10:00:00, end=10:00:30, total=45300, count=12
[INFO] Window closed: start=10:00:30, end=10:01:00, total=38900, count=9
...
```

### パターン 2: フィルタ・変換（10,000円以上だけ転送）

```bash
# ターミナル A: フィルタ & 変換を起動
python use_cases/03_stream_processing/filter_transform.py

# ターミナル B: 取引データを生成
python use_cases/03_stream_processing/filter_transform.py --produce 200
```

期待される出力（ターミナル A）:
```
[INFO] HIGH VALUE: order_id=xxx price=15000 → forwarded to stream.high_value.v1
[INFO] SKIP: order_id=yyy price=3200 (below threshold)
...
```

### パターン 3: ストリーム結合（注文 × 支払いを突き合わせ）

```bash
# ターミナル A: 結合ロジックを起動
python use_cases/03_stream_processing/stream_join.py

# ターミナル B: 注文と支払いのデータを生成
python use_cases/03_stream_processing/stream_join.py --produce 50
```

期待される出力（ターミナル A）:
```
[INFO] JOINED: order_id=abc123 amount=98000 payment_status=confirmed
[INFO] JOINED: order_id=def456 amount=3200  payment_status=confirmed
[INFO] TIMEOUT: order_id=ghi789 no payment within 5min
...
```

---

## よくある疑問

**Q: ウィンドウ集計の結果はどこに行く？**
A: `stream.sales_30s.v1` トピックに書き出されます。kafka-ui で確認できます。

**Q: 結合のタイムアウトはなぜ必要？**
A: 支払いが永遠に届かないかもしれません。5分以内に支払いが来なかった注文は
「未払い」として別途処理します。

**Q: 本番でも同じコードを使える？**
A: この PoC では状態をメモリで保持しています。本番では **Kafka Streams** や
**ksqlDB** を使うと、状態の永続化・障害復旧・スケールアウトが組み込みで提供されます。

---

## 次のユースケース

- [04_cdc](../04_cdc/)（DB 変更を Kafka に流す）
- [07_real_time_analytics](../07_real_time_analytics/)（リアルタイムダッシュボード）
