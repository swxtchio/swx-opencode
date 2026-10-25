#!/usr/bin/env bash
set -euo pipefail

root=/home/byates/vm14-builds/opencode-release-832df553
script=${1:?script name is required}
name=${2:?run name is required}
log=$root/evidence/$name.log
result=$root/evidence/$name.exit

mkdir -p "$root/evidence"
bash "$root/$script" > "$log" 2>&1 &
child=$!
printf '%s\n' "$child" > "$root/evidence/$name.pid"
while kill -0 "$child" 2>/dev/null; do
  touch /home/byates/vm14-leases/ACTIVE/heartbeat
  sleep 30
done
if wait "$child"; then
  status=0
else
  status=$?
fi
printf '%s\n' "$status" > "$result"
cat "$log"
exit "$status"
