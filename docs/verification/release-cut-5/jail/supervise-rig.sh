#!/bin/bash
# Lap 2: rerun only the jail rig on the lap-1 candidate archive; heartbeat the lease while it lives.
root=/home/byates/vm14-builds/opencode-release-d7f78881
"$root/rig.sh" > "$root/rig-execution.log" 2>&1 &
child=$!
echo "$child" > "$root/child.pid"
while kill -0 "$child" 2>/dev/null; do
  touch /home/byates/vm14-leases/ACTIVE/heartbeat
  sleep 30
done
wait "$child"
echo $? > "$root/rig-exit"
