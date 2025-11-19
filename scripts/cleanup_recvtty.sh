#!/usr/bin/env bash
# Best-effort cleanup for recvtty helpers and runc containers
set -euo pipefail
OUT=/runc/dirty-track/logs/cleanup_recvtty.out
mkdir -p $(dirname "$OUT")
echo "---- cleanup $(date --rfc-3339=seconds) ----" > "$OUT"
for f in /tmp/recvtty_*.pid; do
  [ -f "$f" ] || continue
  pid=$(cat "$f" 2>/dev/null || true)
  echo "Found $f -> $pid" >> "$OUT"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "Killing $pid" >> "$OUT"
    kill "$pid" 2>>"$OUT" || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      echo "$pid still alive, kill -9" >> "$OUT"
      kill -9 "$pid" 2>>"$OUT" || true
      sleep 1
    fi
  fi
  rm -f "$f" 2>>"$OUT" || true
done

echo "Checking runc containers" >> "$OUT"
if runc list -q 2>/dev/null | grep -q .; then
  for id in $(runc list -q 2>/dev/null); do
    echo "Stopping container $id" >> "$OUT"
    runc kill "$id" 2>>"$OUT" || true
    runc delete "$id" 2>>"$OUT" || true
  done
else
  echo "No runc containers found" >> "$OUT"
fi

echo "done" >> "$OUT"
cat "$OUT"
