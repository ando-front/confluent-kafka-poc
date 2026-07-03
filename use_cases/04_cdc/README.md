# 04 — Change Data Capture (CDC)

> **難易度**: ⭐⭐（初中級）
> **所要時間**: 約 15 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) を完了していること

---

## このユースケースで学べること

- **CDC（Change Data Capture）** とは何か — DB の変更を Kafka に流す仕組み
- **Debezium 形式**（`op` / `before` / `after` フィールド）のイベント構造
- DB を変更せずに複数のシステムへリアルタイム同期する方法
- Kafka キーと主キーの対応関係（同一行の変更順序保証）

---

## ビジネス上の意義

「商品の在庫テーブルが更新されたとき、検索インデックスとキャッシュも自動で最新化したい」

DB を直接ポーリング（定期チェック）する場合の問題:
- ミリ秒単位の変更を捕捉できない
- 削除されたレコードを検知できない
- DB に余分な負荷がかかる

**CDC を使うと**: DB に変更が入った瞬間にイベントが Kafka に流れ、
接続された全システムがリアルタイムに同期されます。

```
【CDC なし】                        【CDC あり】
                                     変更発生
DB ← 注文サービス（書き込み）        source.db
DB → 在庫サービス（ポーリング）          ↓ CDC
DB → 検索（ポーリング）           [Kafka Topic]
DB → 通知（ポーリング）                  ↓
     ↑ 全部がDBを直接叩く          ├→ replica.db（レプリケーション）
                                   ├→ 検索インデックス
                                   └→ 通知サービス
```

---

## 仕組み（図解）

```
┌───────────────┐
│  simulator.py │
│  source.db に │
│  INSERT/UPDATE│
│  /DELETE する  │
└───────────────┘
       │
       │ 変更を Debezium 形式でイベント化して produce
       ▼
┌──────────────────────────────────────────────────────────────────┐
│  Topic: cdc.orders.v1                                            │
│                                                                  │
│  INSERT のイベント例:                                              │
│  {                                                               │
│    "op": "c",                  ← c=create, u=update, d=delete   │
│    "before": null,             ← 変更前（INSERT なら null）        │
│    "after": {                  ← 変更後                          │
│      "id": 123,                                                  │
│      "item": "laptop",                                           │
│      "status": "pending"                                         │
│    }                                                             │
│  }                                                               │
│                                                                  │
│  UPDATE のイベント例:                                              │
│  {                                                               │
│    "op": "u",                                                    │
│    "before": { "status": "pending" },                            │
│    "after":  { "status": "shipped" }                             │
│  }                                                               │
└──────────────────────────────────────────────────────────────────┘
       │
       │ consume（op に応じて upsert / delete）
       ▼
┌───────────────┐
│ cdc_consumer  │
│ replica.db に │
│ リアルタイム同期│
└───────────────┘
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `cdc.orders.v1` | `orders` テーブルの変更イベント（Debezium 形式） |

キーは主キー（`id`）。同一行の変更は必ず同じパーティションに入るため、
「INSERT → UPDATE → DELETE」の順序が保証されます。

---

## 実行手順

### ステップ 1: レプリケーターを起動

**ターミナル A**:
```bash
python use_cases/04_cdc/cdc_consumer.py
```

期待される出力（待機中）:
```
[INFO] CDC Consumer started. Listening for changes on cdc.orders.v1...
```

### ステップ 2: DB 変更を発生させる

**ターミナル B**:
```bash
python use_cases/04_cdc/simulator.py --changes 30
```

期待される出力:
```
[INFO] INSERT: order_id=1  item=laptop   status=pending
[INFO] UPDATE: order_id=1  status: pending → shipped
[INFO] DELETE: order_id=2
...
```

**ターミナル A** にレプリケーション結果が表示されます:
```
[INFO] Applied CREATE: order_id=1 item=laptop status=pending
[INFO] Applied UPDATE: order_id=1 status=shipped
[INFO] Applied DELETE: order_id=2
```

### ステップ 3: レプリケーション結果を確認

```bash
sqlite3 use_cases/04_cdc/replica.db "SELECT * FROM orders;"
```

`source.db` と同じ内容が `replica.db` にリアルタイムで反映されているはずです。

---

## kafka-ui で確認

1. http://localhost:8080 → 「Topics」→ `cdc.orders.v1`
2. 「Messages」タブで各変更イベントの内容を確認
3. `op: "c"` → `op: "u"` → `op: "d"` の順にイベントが流れていることを確認
4. 同じ `id` のメッセージが同じパーティションに入っていることを確認

---

## よくある疑問

**Q: 本番では Debezium をどう使う？**
A: 本番では **Debezium + Kafka Connect** を使います。Debezium が MySQL/PostgreSQL の
バイナリログ（binlog/WAL）を直接読み取り、Kafka に流します。このリポジトリは
その「イベント形式」と「下流処理」を Python でシミュレートしています。

**Q: `op: "u"` のとき before と after の両方が必要？**
A: 必須ではありませんが、あると便利です。例えば「status が pending から shipped に
変わったときだけ通知する」という条件フィルタに使えます。

**Q: レプリケーターを止めていた間の変更はどうなる？**
A: Kafka に保持されているため、再起動後に溜まった変更が順番に処理されます。
これが Kafka + CDC の強みです（ポーリングでは止めていた間の変更を取りこぼす可能性がある）。

---

## 次のユースケース

- [05_dead_letter_queue](../05_dead_letter_queue/)（不正データへの対処）
- [07_real_time_analytics](../07_real_time_analytics/)（リアルタイム集計）
