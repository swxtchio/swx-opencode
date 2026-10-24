#!/bin/bash
# Native linux/arm64 candidate build and packaged probe for release-cut-5 on vm14-001.
# Every write stays under the rig scratch root the release-cut-5 brief authorizes.
set -euo pipefail
root=/home/byates/vm14-builds/opencode-release-d7f78881
S=d7f78881560aa83e9390f0231562fa79639eacd9
V=1.18.32-swxtch.d7f78881
T=v$V
ev=$root/evidence
mkdir -p "$ev" "$root"/{home,tmp,cache/bun,cache/npm,xdg/config,xdg/data,xdg/cache,xdg/state,dist,assets,extracted,proof}
export HOME=$root/home TMPDIR=$root/tmp BUN_INSTALL_CACHE_DIR=$root/cache/bun npm_config_cache=$root/cache/npm
export XDG_CONFIG_HOME=$root/xdg/config XDG_DATA_HOME=$root/xdg/data XDG_CACHE_HOME=$root/xdg/cache XDG_STATE_HOME=$root/xdg/state
unset OPENCODE_RELEASE OPENCODE_CHANNEL OPENCODE_BUMP GH_REPO
date -u +%FT%TZ
test "$(uname -m)" = aarch64
uname -a | tee "$ev/host.txt"
bun -e 'if (process.platform !== "linux" || process.arch !== "arm64") throw new Error("not native linux-arm64"); console.log(`Bun ${Bun.version} ${process.platform}-${process.arch}`)' | tee "$ev/bun.txt"
git clone -q https://github.com/swxtchio/swx-opencode "$root/source"
git -C "$root/source" -c advice.detachedHead=false checkout -q "$S"
test "$(git -C "$root/source" rev-parse HEAD)" = "$S"
test "$(bun --version)" = "$(node -e 'console.log(require(process.argv[1]).packageManager.replace("bun@",""))' "$root/source/package.json")"
for c in d7f78881560aa83e9390f0231562fa79639eacd9 cd3f0ce2d5e55944df41fcf43a167f77e40c8483 774d90c7fb02b9f0962620971630a4f09c1a886b bce37c276f4070e3c9d343baa61ce5f35ca38dc9 9340c4d4a88b3c9f1f5274e69e72bd8c631a5053 05a4d4194ca4306ae9f11758f66e56d8fb1cf295 cbfcad62a6fe04475dae23545c6296976f53fa5d 4b87478ddf; do
  git -C "$root/source" merge-base --is-ancestor "$c" "$S"
  echo "ancestry PASS $c $S"
done | tee "$ev/ancestry.txt"
printf '{"inputs":{"release_tag":"%s","source_commit":"%s","version":"%s","candidate":true}}\n' "$T" "$S" "$V" > "$ev/dispatch-event.json"
: > "$ev/target.env"
(cd "$root/source" && RELEASE_SOURCE_DIR="$root/source" GITHUB_EVENT_PATH="$ev/dispatch-event.json" GITHUB_ENV="$ev/target.env" \
  RELEASE_PROBE_DEFAULT_TAG="$T" GITHUB_EVENT_NAME=workflow_dispatch bun run packages/opencode/script/release-probe-target.ts --candidate) | tee "$ev/target.json"
cat "$ev/target.env"
grep -qx "OPENCODE_VERSION=$V" "$ev/target.env"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$ev/source-before.txt"
sha256sum "$root/source/bun.lock" | tee "$ev/bun.lock.sha256"
bun install --cwd "$root/source" --frozen-lockfile --os=linux --cpu=arm64 2>&1 | tee "$ev/install.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$ev/source-after-install.txt"
curl --fail --location --retry 3 --silent --show-error --max-time 180 https://models.dev/api.json --output "$ev/models.dev.json"
sha256sum "$ev/models.dev.json" | tee "$ev/models.dev.sha256"
(cd "$root/source" && OPENCODE_VERSION="$V" MODELS_DEV_API_JSON="$ev/models.dev.json" OPENCODE_BUILD_DIST="$root/dist" \
  bun run packages/opencode/script/build.ts --single --skip-install) 2>&1 | tee "$ev/build.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$ev/source-after-build.txt"
bin=$root/dist/opencode-linux-arm64/bin
test ! -e "$root/dist/opencode-linux-x64/bin/opencode"
shopt -s nullglob
sidecars=("$bin"/libfff_c-*.so)
test "${#sidecars[@]}" -eq 1
sidecar=$(basename "${sidecars[0]}")
file "$bin/opencode" "${sidecars[0]}" | tee "$ev/files.txt"
tar -czf "$root/assets/opencode-linux-arm64.tar.gz" -C "$bin" opencode "$sidecar"
(cd "$root/assets" && sha256sum opencode-linux-arm64.tar.gz > opencode-linux-arm64.tar.gz.sha256 && sha256sum -c opencode-linux-arm64.tar.gz.sha256)
test "$(tar -tzf "$root/assets/opencode-linux-arm64.tar.gz" | sort)" = "$(printf '%s\n' opencode "$sidecar" | sort)"
tar -tzf "$root/assets/opencode-linux-arm64.tar.gz" | tee "$ev/archive-entries.txt"
stat -c %s "$root/assets/opencode-linux-arm64.tar.gz" | tee "$ev/archive-bytes.txt"
tar -xzf "$root/assets/opencode-linux-arm64.tar.gz" -C "$root/extracted"
sha256sum "$root/assets/opencode-linux-arm64.tar.gz" "$root/extracted/opencode" "$root/extracted/$sidecar" | tee "$ev/selected-bytes.sha256"
test "$("$root/extracted/opencode" --version)" = "$V"
"$root/extracted/opencode" --version | tee "$ev/version.txt"
(cd "$root/source" && OPENCODE_VERSION="$V" bun run packages/opencode/script/release-probe.ts arm64 "$root/extracted/opencode" "$root/proof") 2>&1 | tee "$ev/probe.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$ev/source-after-probe.txt"
date -u +%FT%TZ
echo ARM-BUILD-PASS
