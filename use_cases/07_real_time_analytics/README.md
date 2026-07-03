# 07 — Real-Time Analytics

> **難易度**: ⭐⭐（初中級）
> **所要時間**: 約 15 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) を完了していること

---

## このユースケースで学べること

- **リアルタイムストリーム集計** — 流れてくるイベントをウィンドウで集計する方法
- **マルチプロデューサー / シングルコンシューマー** の構成
- Kafka + ストリーム処理の典型的なユースケース（リアルタイムダッシュボード）
- Rich ライブラリを使ったコンソール UI の構築

---

## ビジネス上の意義

「セール開始から5分で CVR がどう変わったかを今すぐ知りたい」

バッチ集計（翌日のレポート）では、問題が起きてから気づくまでに数時間かかります。
Kafka ストリームなら「今この瞬間」の状況を数秒遅延で確認できます。

**活用場面**:
- ECサイトのリアルタイム CVR 監視（セール・キャンペーン期間）
- ゲームのリアルタイムユーザー数・売上ダッシュボード
- 金融の取引量モニタリング
- 障害時の影響範囲リアルタイム把握

---

## 仕組み（図解）

```
  [EC サイトのユーザー行動]
  page_view / add_to_cart / purchase
          │
          │ event_generator.py が毎秒 N 件を generate
          ↓
  ┌────────────────────────────────────────┐
  │  Topic: analytics.events.v1           │
  │  [page_view] [add_to_cart] [purchase] │
  │  [page_view] [page_view]  [purchase]  │
  └────────────────────────────────────────┘
          │ consume（60秒ローリングウィンドウ）
          ↓
  ┌─────────────────────────────────────────────┐
  │  dashboard.py（バックグラウンドスレッドで集計）  │
  │  直近60秒のイベントを集計して KPI を計算        │
  └─────────────────────────────────────────────┘
          │ 1秒ごとに画面を更新
          ↓
  ┌──────────────────────────────────────────────┐
  │          📊 Real-Time Analytics              │
  │  ──────────────────────────────────────────  │
  │  Page views  : 1,423    Events/sec: 48.2     │
  │  Purchases   :    89    CVR       :  6.3%    │
  │  GMV         : ¥1,234,500                    │
  │  ──────────────────────────────────────────  │
  └──────────────────────────────────────────────┘
```

オプションで `aggregator.py` を追加すると、1分ウィンドウの集計結果を
別トピック（`analytics.kpi_1m.v1`）に書き出し、下流のシステムで使えます。

---

## KPI の定義

| 指標 | 定義 | 活用例 |
|-----|------|-------|
| Page views | 直近 60 秒の `page_view` 数 | サイトへの流入確認 |
| Purchases | 直近 60 秒の `purchase` 数 | 売れ行き監視 |
| GMV | 直近 60 秒の購入金額合計 | 売上監視 |
| CVR | purchases / page_views | セール施策の効果測定 |
| Events/sec | スループット | システム負荷確認 |

---

## トピック

| トピック | 用途 |
|---------|------|
| `analytics.events.v1` | 入力: EC イベント（page_view / add_to_cart / purchase） |
| `analytics.kpi_1m.v1` | 出力: 1分窓の集計 KPI（aggregator 使用時） |

---

## 実行手順

2 つのターミナルを開いてください（aggregator はオプション）。

### ステップ 1: ダッシュボードを起動

**ターミナル A**:
```bash
python use_cases/07_real_time_analytics/dashboard.py
```

最初はデータがないので空欄です。次のステップでイベントを生成します。

### ステップ 2: EC イベントを生成

**ターミナル B**:
```bash
# 毎秒 20 件のイベントを生成
python use_cases/07_real_time_analytics/event_generator.py --rate 20
```

出力:
```
[INFO] Generated: page_view   user=u001 ts=10:00:01
[INFO] Generated: add_to_cart user=u002 item=laptop ts=10:00:01
[INFO] Generated: purchase    user=u003 amount=98000 ts=10:00:02
...
```

**ターミナル A** のダッシュボードがリアルタイムに更新されます:
```
📊 Real-Time Analytics  (更新: 10:00:05)
────────────────────────────────────────
Page views  :   142    Events/sec:  20.4
Purchases   :     9    CVR       :   6.3%
GMV         : ¥882,000
────────────────────────────────────────
```

### ステップ 3（オプション）: 1分ウィンドウ集計を追加

**ターミナル C**:
```bash
python use_cases/07_real_time_analytics/aggregator.py
```

1分ごとに確定した KPI が `analytics.kpi_1m.v1` トピックに書き出されます。

---

## 試してみよう

```bash
# イベント生成レートを上げてみる（毎秒100件）
python use_cases/07_real_time_analytics/event_generator.py --rate 100
# → Events/sec が増え、ダッシュボードの KPI が急変する

# レートを下げてみる（毎秒1件）
python use_cases/07_real_time_analytics/event_generator.py --rate 1
# → Events/sec が下がり、CVR の計算精度が下がる（母数が少ない）
```

---

## kafka-ui で確認

1. http://localhost:8080 → 「Topics」→ `analytics.events.v1`
2. 「Messages」タブでイベントが流れていることをリアルタイム確認
3. 「Topics」→ `analytics.kpi_1m.v1`（aggregator 使用時）で確定 KPI を確認

---

## よくある疑問

**Q: 60秒ローリングウィンドウとは何か？**
A: 「直近60秒以内に届いたイベントだけを集計する」という意味です。
古いイベント（60秒以上前）は集計から除外されます。

**Q: aggregator.py なしでも dashboard.py が動くのはなぜ？**
A: `dashboard.py` は `analytics.events.v1` を直接読んで自前でウィンドウ集計しています。
`aggregator.py` は別のシステム向けに「確定KPIを Kafka に書き出す」追加機能です。

**Q: 本番ではどのようなアーキテクチャにする？**
A: **Kafka Streams** や **ksqlDB** を使うと、ウィンドウ集計・状態管理・障害復旧が
組み込みで提供されます。このリポジトリは概念理解のための PoC です。

---

## 次のステップ

- [docs/use-cases-guide.md](../../docs/use-cases-guide.md) で全ユースケースを振り返る
- `benchmarks/` でスループット・レイテンシを計測する
- Confluent Cloud に切り替えて本番環境に近い構成を試す
