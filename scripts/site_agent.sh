#!/usr/bin/env bash
# =============================================================================
# site_agent.sh — ハンズオン学習サイトを launchd に常駐登録する（macOS）。
#
#   ./scripts/site_agent.sh install     # 登録して起動（ログインのたび自動起動）
#   ./scripts/site_agent.sh sync        # サイトを編集したあと配信物を更新
#   ./scripts/site_agent.sh status      # 状態確認
#   ./scripts/site_agent.sh restart     # サーバーだけ再起動
#   ./scripts/site_agent.sh uninstall   # 登録解除して停止
#   PORT=9000 ./scripts/site_agent.sh install
#
# ■ なぜ配信ディレクトリをリポジトリの外に置くのか
#   このリポジトリは ~/Documents 配下にある。macOS の TCC（プライバシー保護）は
#   launchd から起動されたプロセスの ~/Documents へのアクセスを既定で拒否するため、
#   リポジトリを直接 --directory に指定すると "Operation not permitted" で落ちる。
#   そこでビルド（~/Documents を読む）は端末から実行し、生成物だけを
#   ~/Library/Application Support 配下に複製して、launchd にはそこを配信させる。
#
#   → サイトを編集したら install か sync を実行すること。配信物は自動追従しない。
#
# 公開されるのは site/.dist/ の中身（HTML と図版）だけ。.env などは複製されない。
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DIST="$ROOT_DIR/site/.dist"
LABEL="com.ando-front.kafka-handson-site"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
PUBLISH_DIR="$HOME/Library/Application Support/kafka-handson-site/www"
LOG="$HOME/Library/Logs/kafka-handson-site.log"
PORT="${PORT:-8090}"
DOMAIN="gui/$(id -u)"

die() { echo "ERROR: $*" >&2; exit 1; }
xml_escape() { printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

# --- ビルドして配信ディレクトリへ複製 ----------------------------------------
sync_site() {
  BUILD_ONLY=1 "$SCRIPT_DIR/serve_site.sh" "$PORT" > /dev/null || die "ビルドに失敗しました"
  [[ -f "$DIST/index.html" ]] || die "$DIST/index.html が生成されていません"
  mkdir -p "$PUBLISH_DIR"
  rm -rf "${PUBLISH_DIR:?}"/*
  cp -R "$DIST/." "$PUBLISH_DIR/"
  echo "配信物を更新しました: $PUBLISH_DIR"
}

write_plist() {
  local py
  py="$(command -v python3 || command -v python)" || die "python3 が見つかりません"
  case "$py" in
    "$HOME"/Documents/*|"$HOME"/Desktop/*|"$HOME"/Downloads/*)
      die "python3 が TCC 保護領域にあります（$py）。別の python3 を使ってください。" ;;
  esac

  mkdir -p "$(dirname "$PLIST")" "$(dirname "$LOG")"
  cat > "$PLIST" <<PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <!-- 配信するのは生成物の複製のみ。リポジトリ（~/Documents 配下）には触らない -->
  <key>ProgramArguments</key>
  <array>
    <string>$(xml_escape "$py")</string>
    <string>-m</string>
    <string>http.server</string>
    <string>${PORT}</string>
    <string>--bind</string>
    <string>0.0.0.0</string>
    <string>--directory</string>
    <string>$(xml_escape "$PUBLISH_DIR")</string>
  </array>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>

  <!-- ログイン時に起動し、落ちたら起動し直す -->
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>

  <key>StandardOutPath</key>
  <string>$(xml_escape "$LOG")</string>
  <key>StandardErrorPath</key>
  <string>$(xml_escape "$LOG")</string>
</dict>
</plist>
PLISTEOF
  plutil -lint "$PLIST" > /dev/null || die "生成した plist が不正です: $PLIST"
}

stop_agent() {
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || launchctl unload "$PLIST" 2>/dev/null || true
}

lan_ip() {
  local iface ip
  for iface in en0 en1 en2 en5 eth0 wlan0; do
    ip="$(ipconfig getifaddr "$iface" 2>/dev/null || true)"
    [[ -n "$ip" ]] && { printf '%s' "$ip"; return; }
  done
}

case "${1:-status}" in
  install)
    # 手動で起動しっぱなしのサーバーがいるとポートを奪い合うので先に片付ける
    pkill -f "http.server ${PORT}" 2>/dev/null || true
    stop_agent
    sync_site
    write_plist
    launchctl bootstrap "$DOMAIN" "$PLIST" 2>/dev/null || launchctl load -w "$PLIST" 2>/dev/null \
      || die "launchctl への登録に失敗しました"
    sleep 1
    echo
    echo "登録しました: $PLIST"
    echo "  このマシンから  : http://localhost:${PORT}/"
    ip="$(lan_ip)"; [[ -n "$ip" ]] && echo "  LAN の他端末から: http://${ip}:${PORT}/"
    echo "  ログ            : ${LOG}"
    echo "  サイトを編集したら: ./scripts/site_agent.sh sync"
    ;;
  sync)
    sync_site
    ;;
  restart)
    [[ -f "$PLIST" ]] || die "未登録です。先に install を実行してください。"
    launchctl kickstart -k "$DOMAIN/$LABEL" || die "再起動に失敗しました"
    echo "再起動しました"
    ;;
  uninstall)
    stop_agent
    rm -f "$PLIST"
    rm -rf "$(dirname "$PUBLISH_DIR")"
    echo "登録を解除し、plist と配信物を削除しました"
    ;;
  status)
    if [[ -f "$PLIST" ]]; then
      echo "plist   : $PLIST"
      echo "配信元  : $PUBLISH_DIR"
      launchctl print "$DOMAIN/$LABEL" 2>/dev/null | awk '/^\tstate =|pid =|last exit code/' \
        || echo "  （launchd に未ロード）"
    else
      echo "未登録です（$PLIST がありません）"
    fi
    ;;
  *)
    die "使い方: $0 {install|sync|restart|uninstall|status}"
    ;;
esac
