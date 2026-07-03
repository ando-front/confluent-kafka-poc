# トラブルシューティング

> エラーが出たときの対処法をまとめています。
> 解決しない場合は、エラーメッセージ全文を添えて確認してください。

---

## 起動関連

### Docker が動いていない

**症状**:
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

**原因**: Docker Desktop が起動していない

**解決**:
- Mac: Dock または Applications フォルダから Docker アプリを起動
- Windows: スタートメニューから Docker Desktop を起動
- 起動後、メニューバーのクジラアイコンが「Docker Desktop is running」になるまで待つ

---

### Kafka に接続できない

**症状**:
```
KafkaException: Failed to resolve 'localhost:9092'
Connection refused
```

**原因 1**: `./scripts/start.sh` をまだ実行していない

**解決**:
```bash
./scripts/start.sh
# ✅ Kafka is ready! が表示されるまで待つ
```

**原因 2**: ヘルスチェックが失敗している（コンテナが起動中）

**解決**:
```bash
docker ps
# すべてのコンテナが "Up" になっているか確認
# まだ "starting" なら1〜2分待ってから再試行
```

---

### ポートが使用中

**症状**:
```
Error response from daemon: Bind for 0.0.0.0:9092 failed: port is already allocated
```

**原因**: すでに Kafka（または別のプロセス）がポートを使っている

**解決**:
```bash
# 一度完全に停止してから再起動
./scripts/stop.sh
./scripts/start.sh
```

```bash
# それでも解決しない場合: ポートを使っているプロセスを確認（Mac/Linux）
lsof -i :9092
kill -9 <PID>
```

---

## Python 関連

### モジュールが見つからない

**症状**:
```
ModuleNotFoundError: No module named 'confluent_kafka'
ModuleNotFoundError: No module named 'pydantic_settings'
```

**解決**:
```bash
pip install -r requirements.txt
```

```bash
# pip が Python 3.11+ を使っているか確認
pip --version
# pip 23.x from /path/to/python3.11/... と表示されるはず
```

---

### Python バージョンが古い

**症状**:
```
SyntaxError: ...
# または type hints の TypeError
```

**解決**:
```bash
python --version
# Python 3.11 未満の場合はアップグレード

# Mac（pyenv）
brew install pyenv
pyenv install 3.11.9
pyenv local 3.11.9
```

---

## メッセージ関連

### コンシューマーが止まったまま何も表示されない

**症状**: `python use_cases/01_basic_pubsub/consumer.py` を実行しても何も表示されない

**原因**: メッセージが来るまで待機している（正常動作）

**解決**: 別のターミナルでプロデューサーを起動してください:
```bash
python use_cases/01_basic_pubsub/producer.py
```

---

### コンシューマーが終了しない（Ctrl+C で止める方法）

**症状**: `python use_cases/.../consumer.py` を実行したが、止め方がわからない

**理由**: コンシューマーはメッセージを待ち続けるループで動いています（正常動作）。
これは「郵便配達員が配達物が来るまで待機している」状態です。

**解決**: ターミナルで `Ctrl + C` を押してください。

```bash
# → プログラムが実行中のターミナルで Ctrl + C
# 以下のようなメッセージが出て終了します:
# ^C
# [INFO] Consumer stopped by user
```

> ターミナルを閉じてしまった場合は、別のターミナルで `./scripts/stop.sh` を実行するか
> `kill $(lsof -ti :9092)` で Kafka を止めてください（コンシューマーも自動終了します）。

---

### コンシューマーを再起動したら最初から読み直した

**症状**: 同じメッセージが再度表示される

**原因**: `KAFKA_AUTO_OFFSET_RESET=earliest` が設定されており、新しいグループは最初から読む

**解決（同じグループで続きから読む）**:
```bash
# 同じ --group 名を使うと、前回のオフセットから再開される
python use_cases/01_basic_pubsub/consumer.py --group my-group
```

---

### トピックが「already exists」エラー

**症状**:
```
TopicAlreadyExistsException: Topic 'xxx' already exists
```

**原因**: トピックが残っている（通常は問題なし）

**解決**: `core/admin.py` の `ensure_topic_exists()` は「存在していればスキップ」するように
設計されているため、このエラーは無視できます。

完全にリセットしたい場合:
```bash
./scripts/reset.sh
./scripts/start.sh
```

---

### メッセージが kafka-ui に表示されない

**症状**: プロデューサーを動かしてもトピックにメッセージが見えない

**解決チェックリスト**:
1. kafka-ui を **リロード** する（F5）
2. 「Topics」→「Refresh」ボタンをクリック
3. トピック名が正しいか確認（`pubsub.orders.v1` など）
4. 画面右上のクラスター名が `local` になっているか確認

---

## ユースケース固有

### 06_exactly_once: `transactional.id` エラー

**症状**:
```
KafkaException: Invalid replication factor
# または transactional_id 関連のエラー
```

**解決**:
```bash
# リセットして再試行（トランザクション状態が残っている可能性）
./scripts/reset.sh
./scripts/start.sh
python use_cases/06_exactly_once/idempotent_consumer.py
python use_cases/06_exactly_once/transactional_producer.py
```

---

### 07_real_time_analytics: Rich ライブラリのエラー

**症状**:
```
ModuleNotFoundError: No module named 'rich'
```

**解決**:
```bash
pip install rich
```

---

## データリセット

```bash
# トピックデータを全削除してやり直す
./scripts/reset.sh
./scripts/start.sh
```

---

## ログ確認

```bash
# Kafka ブローカーのログを確認
docker logs confluent-kafka-poc-broker-1 --tail 50

# すべてのコンテナの状態を確認
docker ps

# コンテナが停止していた場合
docker compose -f docker/docker-compose.yml up -d
```

---

## それでも解決しない場合

以下の情報を集めてください:

```bash
# 環境情報
python --version
docker --version
docker compose version

# コンテナの状態
docker ps -a

# Kafka のログ（直近 100 行）
docker logs confluent-kafka-poc-broker-1 --tail 100
```
