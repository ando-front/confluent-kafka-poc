# 図解（draw.io）

自己学習用のリッチな図解。**`.drawio` が正本、`.svg` は生成物**。

- 編集は [app.diagrams.net](https://app.diagrams.net/) か draw.io デスクトップ版で `.drawio` を開く
- 編集したら SVG を作り直す（draw.io アプリは不要）:

```bash
python3 scripts/drawio_to_svg.py              # docs/diagrams/*.drawio を全変換
python3 scripts/drawio_to_svg.py docs/diagrams/01-kafka-overview.drawio
```

`scripts/drawio_to_svg.py` は、ここで使っている mxGraph スタイルのサブセット
（rounded / ellipse / rhombus / swimlane / text、および exit・entry 指定つきの
orthogonal エッジ）だけを解釈する軽量レンダラー。新しい図形を使いたくなったら
先にレンダラー側を足す。

## 一覧

| ファイル | 内容 | 貼ってある場所 |
|---------|------|--------------|
| `01-kafka-overview` | Kafka 全体像（Producer → Broker → Consumer Group） | site 01 / kafka-concepts |
| `02-topic-partition-offset` | Topic・Partition・Offset・committed offset・lag | site 02 / kafka-concepts |
| `03-consumer-group` | 分担 / fan-out / idle コンシューマー | site 08 / kafka-concepts / use-cases 01 |
| `04-docker-stack` | ローカル環境（Docker Compose 6 サービスと core 階層） | site 06 / getting-started |
| `05-event-sourcing` | イベント追記・畳み込み・リプレイ | site 09 / use-cases 02 |
| `06-stream-processing` | ウィンドウ集計 / フィルタ変換 / ストリーム結合 | site 13 / use-cases 03 |
| `07-cdc` | ポーリングとの比較と Debezium 形式（op/before/after） | site 10 / use-cases 04 |
| `08-dead-letter-queue` | リトライ → DLQ 退避 → 再投入 | site 11 / use-cases 05 |
| `09-exactly-once` | 送信側トランザクション × 受信側冪等台帳 | site 14 / use-cases 06 |
| `10-realtime-analytics` | 生イベント → 集計 → KPI → ダッシュボード | site 12 / use-cases 07 |
| `11-confluent-cloud` | .env だけでローカル / Confluent Cloud を切り替え | site 18 / getting-started |
| `12-delivery-semantics` | 配信保証の 3 レベル比較 | site 02 / kafka-concepts |
| `13-troubleshooting-flow` | 障害切り分けフローチャート | site 17 / troubleshooting |
| `14-learning-path` | 19 ステップの学習ロードマップ | site 冒頭 / kafka-concepts |
| `15-schema-registry` | スキーマの版管理と互換性チェック | site 06 / kafka-concepts |

## 色の取り決め

| 用途 | 塗り | 線 |
|-----|------|----|
| 強調・Kafka 側 | `#D6EDEF` | `#0B7A84` |
| 中立 | `#EEF3F4` | `#9FB3B7` |
| 成功・望ましい状態 | `#DCF0E7` | `#1F7A5C` |
| 注意・保留 | `#F7E9D4` | `#B07A28` |
| 失敗・アンチパターン | `#F8E0E0` | `#B44A4A` |

SVG は白背景で固定。学習サイトはダークテーマにも対応しているため、
図は白いパネルとして浮かぶ見え方になる（意図どおり）。

> ブログ等に公開するときは Mermaid 化して差し替える想定。ここでは可読性を優先している。
