#!/bin/bash
# Run the arm64 build, then the jail rig, under the rig scratch root; heartbeat the lease while either lives and record
# each exit code. The rig runs only on a passing build.
root=/home/byates/vm14-builds/opencode-release-d7f78881
(
  "$root/arm-build.sh" > "$root/arm-execution.log" 2>&1
  echo $? > "$root/result"
  test "$(cat "$root/result")" = 0 || exit 1
  "$root/rig.sh" > "$root/rig-execution.log" 2>&1
  echo $? > "$root/rig-exit"
) &
child=$!
echo "$child" > "$root/child.pid"
while kill -0 "$child" 2>/dev/null; do
  touch /home/byates/vm14-leases/ACTIVE/heartbeat
  sleep 30
done
wait "$child"
echo $? > "$root/supervise-exit"
