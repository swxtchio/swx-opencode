#!/bin/bash
# Native linux/arm64 candidate build of this PR's head for the jail compaction rig (swxtchio/swx-abbe#1953).
# Every write stays under the task worktree's ignored logs/compact-rig and its evidence directory.
set -euo pipefail
W=/home/byates/fm-worktrees/fm-llmrouter/opencode-v2-smallmodel-1953/swx-opencode
R=$W/logs/compact-rig
E=$W/docs/verification/v2-small-model-1953/arm64
S=$1
V=1.18.32-swxtch.${S:0:8}
date -u +%FT%TZ
test "$(uname -m)" = aarch64
uname -a | tee "$E/host.txt"
rm -rf "$R/source" "$R/dist" "$R/assets" "$R/extracted"
mkdir -p "$R/dist" "$R/assets" "$R/extracted" "$R/tmp"
export TMPDIR=$R/tmp
git clone -q "$W" "$R/source"
git -C "$R/source" -c advice.detachedHead=false checkout -q "$S"
test "$(git -C "$R/source" rev-parse HEAD)" = "$S"
git -C "$R/source" log -1 --format='%H %s' | tee "$E/source.txt"
test "$(bun --version)" = "$(node -e 'console.log(require(process.argv[1]).packageManager.replace("bun@",""))' "$R/source/package.json")"
bun install --cwd "$R/source" --frozen-lockfile --os=linux --cpu=arm64 > "$E/install.log" 2>&1
test -z "$(git -C "$R/source" status --porcelain)"
# The catalog snapshot release-cut-4's arm64 build consumed.
M=$W/docs/verification/release-cut-4/inputs/models.dev.arm64.json
sha256sum "$M" | tee "$E/models.dev.sha256"
(cd "$R/source" && OPENCODE_VERSION="$V" MODELS_DEV_API_JSON="$M" OPENCODE_BUILD_DIST="$R/dist" \
  bun run packages/opencode/script/build.ts --single --skip-install) > "$E/build.log" 2>&1
test -z "$(git -C "$R/source" status --porcelain)"
bin=$R/dist/opencode-linux-arm64/bin
shopt -s nullglob
sidecars=("$bin"/libfff_c-*.so)
test "${#sidecars[@]}" -eq 1
sidecar=$(basename "${sidecars[0]}")
tar -czf "$R/assets/opencode-linux-arm64.tar.gz" -C "$bin" opencode "$sidecar"
tar -tzf "$R/assets/opencode-linux-arm64.tar.gz" | tee "$E/archive-entries.txt"
tar -xzf "$R/assets/opencode-linux-arm64.tar.gz" -C "$R/extracted"
sha256sum "$R/assets/opencode-linux-arm64.tar.gz" "$R/extracted/opencode" "$R/extracted/$sidecar" | tee "$E/selected-bytes.sha256"
test "$("$R/extracted/opencode" --version)" = "$V"
"$R/extracted/opencode" --version | tee "$E/version.txt"
date -u +%FT%TZ
echo ARM-BUILD-PASS
