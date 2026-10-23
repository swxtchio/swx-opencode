#!/bin/bash
# Run the ARM build under the granted root, heartbeat the lease while it lives, record the exit code.
root=/home/byates/vm14-builds/opencode-release-774d90c7
"$root/arm-build.sh" > "$root/arm-execution.log" 2>&1 &
child=$!
echo "$child" > "$root/child.pid"
while kill -0 "$child" 2>/dev/null; do
  touch /home/byates/vm14-leases/ACTIVE/heartbeat
  sleep 30
done
wait "$child"
echo $? > "$root/result"
