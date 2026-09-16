#!/usr/bin/env bash
# =============================================================================
# serve_site.sh — ハンズオン学習サイト (site/index.html) を LAN に公開する。
#
# Usage:
#   ./scripts/serve_site.sh              # ポート 8090 で起動（フォアグラウンド）
#   ./scripts/serve_site.sh 9000         # ポートを指定
#   PORT=9000 ./scripts/serve_site.sh    # 環境変数でも指定可
#
# 公開されるのは site/.dist/ の中身だけ。リポジトリの他のファイル（.env など）は
# 一切配信されない。停止は Ctrl + C。
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SRC="$ROOT_DIR/site/index.html"
DIST="$ROOT_DIR/site/.dist"
PORT="${1:-${PORT:-8090}}"

if [[ ! -f "$SRC" ]]; then
  echo "ERROR: $SRC が見つかりません。" >&2
  exit 1
fi

PY="$(command -v python3 || command -v python || true)"
if [[ -z "$PY" ]]; then
  echo "ERROR: python3 が見つかりません。" >&2
  exit 1
fi

# --- 配信用の完全な HTML ドキュメントを生成 ----------------------------------
# site/index.html は Artifact 公開用にヘッダを持たない断片なので、
# ここで doctype / viewport を補って標準モードの単独ページに組み立てる。
mkdir -p "$DIST"
"$PY" - "$SRC" "$DIST/index.html" <<'PYEOF'
import re
import sys

src, dst = sys.argv[1], sys.argv[2]
html = open(src, encoding="utf-8").read()

m = re.search(r"<title>(.*?)</title>", html, re.S)
title = m.group(1).strip() if m else "Kafka ハンズオン"
body = re.sub(r"<title>.*?</title>\s*", "", html, count=1, flags=re.S)
body = re.sub(r'<meta charset="utf-8">\s*', "", body, count=1)

doc = (
    "<!doctype html>\n"
    '<html lang="ja">\n'
    "<head>\n"
    '<meta charset="utf-8">\n'
    '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
    f"<title>{title}</title>\n"
    "<style>html{color-scheme:light dark}body{margin:0}"
    "img{max-width:100%}[hidden]{display:none!important}</style>\n"
    "</head>\n"
    "<body>\n" + body.strip() + "\n</body>\n</html>\n"
)
open(dst, "w", encoding="utf-8").write(doc)
print(f"built {dst} ({len(doc):,} bytes)")
PYEOF

# --- LAN の IP アドレスを調べる ----------------------------------------------
LAN_IP=""
for IFACE in en0 en1 en2 en5 eth0 wlan0; do
  if command -v ipconfig >/dev/null 2>&1; then
    IP="$(ipconfig getifaddr "$IFACE" 2>/dev/null || true)"
  else
    IP="$(ip -4 addr show "$IFACE" 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -1)"
  fi
  if [[ -n "$IP" ]]; then LAN_IP="$IP"; break; fi
done

echo
echo "================================================================"
echo "  Kafka ハンズオン学習サイトを公開しました"
echo "================================================================"
echo "  このマシンから : http://localhost:${PORT}/"
if [[ -n "$LAN_IP" ]]; then
  echo "  LAN の他端末から: http://${LAN_IP}:${PORT}/"
else
  echo "  LAN の他端末から: このマシンの IP アドレス:${PORT}"
fi
echo
echo "  公開しているのは site/.dist/ のみ（.env などは配信されません）"
echo "  停止: Ctrl + C"
echo "================================================================"
echo

exec "$PY" -m http.server "$PORT" --bind 0.0.0.0 --directory "$DIST"
