# 06 — Exactly-Once Semantics (EOS)

> **難易度**: ⭐⭐⭐（中上級）
> **所要時間**: 約 20 分
> **前提**: [01_basic_pubsub](../01_basic_pubsub/) と [05_dead_letter_queue](../05_dead_letter_queue/) を完了していること

---

## このユースケースで学べること

- **Exactly-Once** とは何か — At-Least-Once との違い
- **Kafka トランザクション** — 複数メッセージを「全部コミット or 全部ロールバック」する方法
- **冪等コンシューマー** — 同じメッセージが2回来ても1回だけ処理する設計
- `transactional.id` / `read_committed` / `isolation_level` の役割

---

## ビジネス上の意義

「送金処理を絶対に 2 回実行してはいけない」

Kafka のデフォルトは **At-Least-Once**（少なくとも1回配信）です。
ネットワーク障害や再試行によって同じメッセージが複数回届くことがあります。

```
【At-Least-Once の問題】
  Producer → Kafka → Consumer
                     ↓ 処理中にネットワーク断
                     再接続 → 同じメッセージを再受信
                     ↓ 二重送金！
```

**金融・課金・在庫では絶対に許容できない**二重処理を、
Kafka の **トランザクション + 冪等処理** で防ぎます。

---

## 配信保証レベルの比較

| レベル | 消失 | 重複 | 用途 |
|-------|------|------|------|
| At-Most-Once | あり | なし | ログ（多少の欠損を許容） |
| At-Least-Once | なし | あり | 通知・分析（冪等処理で対応） |
| **Exactly-Once** | **なし** | **なし** | **金融・課金・在庫** |

---

## 仕組み（図解）

### Producer 側: トランザクション

```
Producer
  ↓ begin_transaction()
  ↓ produce(送金#1: A→B 10,000円)
  ↓ produce(送金#2: C→D  5,000円)
  ↓ produce(送金#3: E→F  3,000円)
  ↓ commit_transaction()
     → 3件が Kafka に原子的に書き込まれる
     → abort の場合は 3件ともなかったことになる（Consumer には見えない）
```

### Consumer 側: 冪等処理

```
Consumer
  ↓ メッセージを受信
  ↓ SQLite の「処理済みID」テーブルを確認
     処理済み → スキップ（重複を無視）
     未処理   → 処理して ID を記録
```

### `read_committed` による未コミットメッセージの除外

```
Topic に書かれているが未コミットのメッセージ:
  Consumer: isolation_level=read_committed を設定
  → abort されたトランザクションのメッセージは見えない
  → commit されたものだけを読む
```

---

## トピック

| トピック | 用途 |
|---------|------|
| `eos.transfers.v1` | 送金イベント（トランザクション単位でコミット） |

---

## 実行手順

### ステップ 1: 冪等コンシューマーを起動

**ターミナル A**:
```bash
python use_cases/06_exactly_once/idempotent_consumer.py
```

出力:
```
[INFO] Idempotent consumer started (isolation_level=read_committed)
[INFO] Waiting for committed messages...
```

### ステップ 2: トランザクション送信（成功パターン）

**ターミナル B**:
```bash
python use_cases/06_exactly_once/transactional_producer.py --batch 5
```

出力（プロデューサー）:
```
[INFO] Begin transaction
[INFO] Producing transfer: tx_id=001 from=A to=B amount=10000
[INFO] Producing transfer: tx_id=002 from=C to=D amount=5000
...
[INFO] Commit transaction (5 messages)
[INFO] Transaction committed successfully
```

出力（コンシューマー）:
```
[INFO] Received: tx_id=001 from=A to=B amount=10000 → Processed (new)
[INFO] Received: tx_id=002 from=C to=D amount=5000  → Processed (new)
...
```

### ステップ 3: トランザクション中断（失敗パターン）

```bash
python use_cases/06_exactly_once/transactional_producer.py --batch 5 --fail
```

出力（プロデューサー）:
```
[INFO] Begin transaction
[INFO] Producing transfer: tx_id=006 ...
[INFO] Producing transfer: tx_id=007 ...
[ERROR] Simulated failure at message 3
[INFO] Abort transaction
```

出力（コンシューマー）:
```
← 何も表示されない（abort されたので Consumer には届かない）
```

### ステップ 4: 重複送信でも1回だけ処理されることを確認

```bash
# 同じ batch を再送信
python use_cases/06_exactly_once/transactional_producer.py --batch 5
```

出力（コンシューマー）:
```
[INFO] Received: tx_id=001 from=A to=B → SKIP (already processed)
[INFO] Received: tx_id=002 from=C to=D → SKIP (already processed)
...
← 同じIDは処理されない（冪等性）
```

---

## kafka-ui で確認

1. http://localhost:8080 → 「Topics」→ `eos.transfers.v1`
2. 「Messages」タブで `abort` されたメッセージは表示されないことを確認
3. コミット済みメッセージのみが Consumer から見えることを確認

---

## よくある疑問

**Q: `transactional.id` とは何？**
A: プロデューサーを一意に識別する ID です。同じ `transactional.id` で再接続すると、
前回のプロデューサーは「フェンシング」（無効化）されます。
これにより、古いプロデューサーゾンビが重複書き込みをするのを防ぎます。

**Q: 処理済み台帳（SQLite）がなくなったら？**
A: 重複が発生する可能性があります。本番では処理済み台帳も冗長化（PostgreSQL など）します。

**Q: Exactly-Once はパフォーマンスに影響する？**
A: はい。トランザクションのオーバーヘッドがあるため、スループットは低下します。
本番では At-Least-Once + 冪等処理の組み合わせで済む場合が多く、
純粋な EOS は本当に重複が許されないシステムだけに限定します。

---

## 次のユースケース

- [07_real_time_analytics](../07_real_time_analytics/)（リアルタイムダッシュボード）
- ベンチマーク: `benchmarks/` で At-Least-Once と Exactly-Once のスループット差を計測
