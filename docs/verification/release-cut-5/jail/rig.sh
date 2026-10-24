#!/bin/bash
# Abbe's jail V2 egress rig on the unpublished arm64 candidate, as jail-v2-egress-rig.md §1-§3 specify, with the
# release download replaced by the native-tested archive (Dockerfile.candidate.diff). All writes stay under the root.
set -euo pipefail
root=/home/byates/vm14-builds/opencode-release-d7f78881
V=1.18.32-swxtch.d7f78881
T=v$V
ev=$root/evidence/jail
image="opencode-variant-jail:v2-egress-$V-candidate"
mkdir -p "$ev" "$root/rig-tmp"
# --no-cache: a cached build skips the install step, so its digest and --version checks would not run (lap 2).
# The real HOME: the rig imports pytest from the host user site, which a redirected HOME hides (lap 1).
date -u +%FT%TZ
checkout=$root/rig/swx-abbe
variant=$checkout/components/orch/services/opencode-variant
test "$(cat "$root/rig/abbe-commit")" = 1c6b3582dab3ecef9081dca516102e4c9786f7f4
SHA=$(cut -d' ' -f1 "$root/assets/opencode-linux-arm64.tar.gz.sha256")
(cd "$root/assets" && sha256sum -c opencode-linux-arm64.tar.gz.sha256)
cp "$root/assets/opencode-linux-arm64.tar.gz" "$variant/jail/opencode-candidate.tar.gz"
cp "$root/rig/Dockerfile.jail.candidate" "$variant/jail/Dockerfile.jail.candidate"
(cd "$variant" && DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain -f jail/Dockerfile.jail.candidate \
    -t "$image" \
    --build-arg OPENCODE_RELEASE_TAG="$T" \
    --build-arg OPENCODE_ARCHIVE_SHA256="$SHA" \
    --build-arg OPENCODE_VERSION="$V" \
    --build-arg GIT_COMMIT=1c6b3582dab3ecef9081dca516102e4c9786f7f4 \
    .) > "$ev/image-build.log" 2>&1
grep -F "/tmp/opencode-linux-arm64.tar.gz: OK" "$ev/image-build.log"
docker run --rm --entrypoint sh "$image" -c 'sha256sum /usr/local/bin/opencode /usr/local/bin/libfff_c-*.so' > "$ev/image-bytes.sha256"
docker image inspect "$image" --format '{{.Id}} {{.Architecture}}' >> "$ev/image-bytes.sha256"
cat "$ev/image-bytes.sha256"
set +e
(cd "$checkout" && TMPDIR="$root/rig-tmp" PYTHONDONTWRITEBYTECODE=1 \
  OPENCODE_VARIANT_JAIL_IMAGE="$image" \
  OPENCODE_SERVER_PASSWORD=local-rig-password \
  python3 "$root/rig/jail_v2_egress_probe.py" v1-control v2 v2-named compact) > "$ev/rig-run.log" 2>&1
rc=$?
set -e
echo "exit=$rc" >> "$ev/rig-run.log"
date -u +%FT%TZ >> "$ev/rig-run.log"
cat "$ev/rig-run.log"
docker image rm "$image" > /dev/null
docker network ls --filter name=jail-v2-egress --format '{{.Name}}' | tee "$ev/leftover-networks.txt"
echo "$rc" > "$ev/rig-result"
test "$rc" -eq 0
echo RIG-PASS
