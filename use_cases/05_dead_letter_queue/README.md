# 05 — Dead Letter Queue (DLQ)

> **難易度**: ⭐⭐（初中級）
> **所要時間**: 約 15 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) を完了していること

---

## このユースケースで学べること

- **DLQ（Dead Letter Queue）** パターン — 失敗したメッセージを退避する仕組み
- 不正なメッセージでシステム全体が止まることを防ぐ方法
- **エラーの分類と復旧**（修復可能 vs 修復不能）
- Kafka メッセージの **ヘッダー**（メタ情報）の使い方

---

## ビジネス上の意義

外部から受け取るデータは、必ずしも正しい形式とは限りません。

**DLQ なしの場合**:
```
[正常] [正常] [❌壊れた] ← コンシューマーがここで例外を投げてクラッシュ
                            → 再起動 → また同じメッセージで失敗 → 無限ループ
                            → 後続の正常メッセージも処理されない
```

**DLQ ありの場合**:
```
[正常] → 処理OK
[正常] → 処理OK
[❌壊れた] → DLQ トピックへ退避（後で調査・再処理）
[正常] → 処理OK（止まらない！）
```

**メリット**:
- **可用性**: 1件の不正データで全体が止まることを防ぐ
- **データ損失防止**: 捨てずに DLQ に保管して後から分析・再処理できる
- **運用の分離**: 通常処理と障害処理（復旧・アラート）を分けて管理できる

---

## 仕組み（図解）

```
┌──────────────────┐
│   producer.py    │  ← 20% の不正メッセージ（必須フィールド欠落・壊れた JSON）を混入
└──────────────────┘
         │ produce
         ▼
┌────────────────────────────────────────────────────────┐
│  Topic: dlq.orders.v1                                   │
│  [正常msg] [正常msg] [❌不正msg] [正常msg] [❌不正msg]   │
└────────────────────────────────────────────────────────┘
         │ consume
         ▼
┌─────────────────────────────────────────────────┐
│  consumer.py（メインコンシューマー）               │
│                                                  │
│  正常メッセージ → 通常処理（ログ出力）               │
│  不正メッセージ → エラーヘッダーを付けて DLQ へ転送  │
└─────────────────────────────────────────────────┘
                        │ produce
                        ▼
         ┌────────────────────────────────────────┐
         │  Topic: dlq.orders.dlq.v1（DLQ）       │
         │  ヘッダー: error="missing fields"        │
         │           source_topic="dlq.orders.v1"  │
         │           source_offset="42"            │
         └────────────────────────────────────────┘
                        │ consume
                        ▼
         ┌────────────────────────────────────────┐
         │  dlq_processor.py（復旧処理）            │
         │                                        │
         │  修復可能 → 既定値を補完して再投入         │
         │           → dlq.orders.v1 に戻す        │
         │                                        │
         │  修復不能 → poison ログに記録             │
         │           （本番ではアラート通知）         │
         └────────────────────────────────────────┘
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `dlq.orders.v1` | メイン: 注文メッセージ（20% 不正） |
| `dlq.orders.dlq.v1` | DLQ: 失敗したメッセージ（エラー情報をヘッダーに付与） |

---

## 実行手順

3 つのターミナルを開いてください。

### ステップ 1: メインコンシューマーを起動

**ターミナル A**:
```bash
python use_cases/05_dead_letter_queue/consumer.py
```

待機中の出力:
```
[INFO] Main consumer started. Routing failures to DLQ...
```

### ステップ 2: DLQ プロセッサーを起動

**ターミナル B**:
```bash
python use_cases/05_dead_letter_queue/dlq_processor.py
```

待機中の出力:
```
[INFO] DLQ processor started. Listening on dlq.orders.dlq.v1...
```

### ステップ 3: 不正メッセージを含む注文を送信

**ターミナル C**:
```bash
python use_cases/05_dead_letter_queue/producer.py --count 50
```

送信内容（20% が不正）:
```
[INFO] Produced VALID:   order_id=001 item=laptop   amount=98000
[INFO] Produced VALID:   order_id=002 item=mouse    amount=3200
[INFO] Produced INVALID: order_id=003 (missing 'amount' field)  ← 不正
[INFO] Produced VALID:   order_id=004 item=keyboard amount=8800
[INFO] Produced INVALID: order_id=005 (corrupted JSON)          ← 不正
...
```

---

## 期待される動作

**ターミナル A**（メインコンシューマー）:
```
[INFO] Processed OK: order_id=001 item=laptop
[INFO] Processed OK: order_id=002 item=mouse
[WARN] Failed: order_id=003 error="missing fields" → routing to DLQ
[INFO] Processed OK: order_id=004 item=keyboard
[WARN] Failed: order_id=005 error="invalid JSON"   → routing to DLQ
```

**ターミナル B**（DLQ プロセッサー）:
```
[INFO] DLQ received: order_id=003 error="missing fields"
[INFO]   → RECOVERABLE: filled default amount=0, re-queued
[INFO] DLQ received: order_id=005 error="invalid JSON"
[INFO]   → UNRECOVERABLE: logged as poison message
```

---

## 復旧ロジック

| エラー種別 | 判定 | 処理 |
|----------|------|------|
| `missing_fields` | 修復可能 | デフォルト値を補完してメイントピックへ再投入 |
| `invalid_json` | 修復不能 | poison ログに記録（本番は永続ストレージ＋アラート） |

---

## kafka-ui で確認

1. http://localhost:8080 → 「Topics」→ `dlq.orders.dlq.v1`
2. 「Messages」タブで不正メッセージが DLQ に入っていることを確認
3. メッセージのヘッダーを展開して `error` / `source_topic` / `source_offset` を確認

---

## よくある疑問

**Q: なぜ不正メッセージを最初から弾かない（バリデーションで落とす）のか？**
A: プロデューサー側でのバリデーションは重要ですが、DB の制約変更・スキーマ変更・
外部連携システムの変更などで「想定外のメッセージ」は本番でも発生します。
DLQ はそのような「想定外」への保険です。

**Q: DLQ のメッセージを再処理する頻度は？**
A: 障害の種類によります。元データのバグなら人手で修正後に一括再処理、
下流サービスの障害なら復旧後に自動再処理が一般的です。

---

## 次のユースケース

- [06_exactly_once](../06_exactly_once/)（重複処理ゼロを保証する）
- [07_real_time_analytics](../07_real_time_analytics/)（リアルタイムダッシュボード）
