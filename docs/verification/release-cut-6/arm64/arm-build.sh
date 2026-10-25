#!/usr/bin/env bash
set -euo pipefail

root=/home/byates/vm14-builds/opencode-release-832df553
source_commit=832df553518d8bb581952433c72495a472da8c5d
version=1.18.32-swxtch.832df553
tag=v$version
evidence=$root/evidence/arm64

mkdir -p "$evidence" "$root"/{home,tmp,cache/bun,cache/npm,xdg/config,xdg/data,xdg/cache,xdg/state,dist,assets,extracted,proof}
export HOME=$root/home TMPDIR=$root/tmp BUN_INSTALL_CACHE_DIR=$root/cache/bun npm_config_cache=$root/cache/npm
export XDG_CONFIG_HOME=$root/xdg/config XDG_DATA_HOME=$root/xdg/data XDG_CACHE_HOME=$root/xdg/cache XDG_STATE_HOME=$root/xdg/state
unset OPENCODE_RELEASE OPENCODE_CHANNEL OPENCODE_BUMP GH_REPO

date -u +%FT%TZ | tee "$evidence/started-at.txt"
hostname -f | tee "$evidence/host.txt"
uname -a | tee "$evidence/uname.txt"
test "$(uname -m)" = aarch64
bun -e 'if (process.platform !== "linux" || process.arch !== "arm64") throw new Error("not native linux-arm64"); console.log(`Bun ${Bun.version} ${process.platform}-${process.arch}`)' | tee "$evidence/bun.txt"
node --version | tee "$evidence/node.txt"

git clone -q https://github.com/swxtchio/swx-opencode "$root/source"
git -C "$root/source" -c advice.detachedHead=false checkout -q "$source_commit"
test "$(git -C "$root/source" rev-parse HEAD)" = "$source_commit"
expected_bun=$(node -p 'require(process.argv[1]).packageManager.match(/^bun@([^+]+)/)?.[1] || ""' "$root/source/package.json")
test -n "$expected_bun"
test "$(bun --version)" = "$expected_bun"
printf 'expected=%s\nactual=%s\n' "$expected_bun" "$(bun --version)" > "$evidence/bun-pin.txt"

for commit in 4973723074ed4ba2e778e287eacd3d518cc315d7 832df553518d8bb581952433c72495a472da8c5d 4b87478ddf; do
  git -C "$root/source" merge-base --is-ancestor "$commit" "$source_commit"
  printf 'ancestry PASS %s %s\n' "$commit" "$source_commit"
done | tee "$evidence/ancestry.txt"
if git -C "$root/source" merge-base --is-ancestor 4973723074ed4ba2e778e287eacd3d518cc315d7 d7f78881560aa83e9390f0231562fa79639eacd9; then
  echo 'cut-5 source unexpectedly contains #358' >&2
  exit 1
fi
echo 'cut-5 source predates #358 and #361' | tee "$evidence/older-pin.txt"

printf '{"inputs":{"release_tag":"%s","source_commit":"%s","version":"%s","candidate":true}}\n' "$tag" "$source_commit" "$version" > "$evidence/dispatch-event.json"
: > "$evidence/target.env"
(cd "$root/source" && RELEASE_SOURCE_DIR="$root/source" GITHUB_EVENT_PATH="$evidence/dispatch-event.json" GITHUB_ENV="$evidence/target.env" \
  RELEASE_PROBE_DEFAULT_TAG="$tag" GITHUB_EVENT_NAME=workflow_dispatch bun run packages/opencode/script/release-probe-target.ts --candidate) \
  | tee "$evidence/target.json"
cat "$evidence/target.env" | tee "$evidence/target.env.readback.txt"
grep -qx "RELEASE_TAG=$tag" "$evidence/target.env"
grep -qx "SOURCE_COMMIT=$source_commit" "$evidence/target.env"
grep -qx "OPENCODE_VERSION=$version" "$evidence/target.env"
grep -qx "BUN_VERSION=$expected_bun" "$evidence/target.env"

test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$evidence/source-before.txt"
sha256sum "$root/source/bun.lock" | tee "$evidence/bun.lock.sha256"
bun install --cwd "$root/source" --frozen-lockfile --os=linux --cpu=arm64 2>&1 | tee "$evidence/install.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$evidence/source-after-install.txt"

curl --fail --location --retry 3 --silent --show-error --max-time 180 https://models.dev/api.json --output "$evidence/models.dev.json"
sha256sum "$evidence/models.dev.json" | tee "$evidence/models.dev.sha256"
(cd "$root/source" && env -u OPENCODE_RELEASE -u OPENCODE_CHANNEL -u OPENCODE_BUMP -u GH_REPO \
  OPENCODE_VERSION="$version" MODELS_DEV_API_JSON="$evidence/models.dev.json" OPENCODE_BUILD_DIST="$root/dist" \
  bun run packages/opencode/script/build.ts --single --skip-install) 2>&1 | tee "$evidence/build.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$evidence/source-after-build.txt"

bin=$root/dist/opencode-linux-arm64/bin
test ! -e "$root/dist/opencode-linux-x64/bin/opencode"
shopt -s nullglob
sidecars=("$bin"/libfff_c-*.so)
test "${#sidecars[@]}" -eq 1
sidecar=$(basename "${sidecars[0]}")
file "$bin/opencode" "${sidecars[0]}" | tee "$evidence/files.txt"
tar -czf "$root/assets/opencode-linux-arm64.tar.gz" -C "$bin" opencode "$sidecar"
(cd "$root/assets" && sha256sum opencode-linux-arm64.tar.gz > opencode-linux-arm64.tar.gz.sha256 && sha256sum -c opencode-linux-arm64.tar.gz.sha256)
test "$(tar -tzf "$root/assets/opencode-linux-arm64.tar.gz" | sort)" = "$(printf '%s\n' opencode "$sidecar" | sort)"
tar -tzf "$root/assets/opencode-linux-arm64.tar.gz" | tee "$evidence/archive-entries.txt"
stat -c %s "$root/assets/opencode-linux-arm64.tar.gz" | tee "$evidence/archive-bytes.txt"
tar -xzf "$root/assets/opencode-linux-arm64.tar.gz" -C "$root/extracted"
sha256sum "$root/assets/opencode-linux-arm64.tar.gz" "$root/extracted/opencode" "$root/extracted/$sidecar" | tee "$evidence/selected-bytes.sha256"
test "$("$root/extracted/opencode" --version)" = "$version"
"$root/extracted/opencode" --version | tee "$evidence/version.txt"
(cd "$root/source" && OPENCODE_VERSION="$version" bun run packages/opencode/script/release-probe.ts arm64 "$root/extracted/opencode" "$root/proof") \
  2>&1 | tee "$evidence/probe.log"
test -z "$(git -C "$root/source" status --porcelain)"
git -C "$root/source" status --porcelain=v1 | tee "$evidence/source-after-probe.txt"

RELEASE_TAG="$tag" SOURCE_COMMIT="$source_commit" RELEASE_VERSION="$version" EXPECTED_BUN="$expected_bun" ACTUAL_BUN="$(bun --version)" \
  SIDECAR="$sidecar" python3 - "$root" <<'PY'
import hashlib, json, os, pathlib, subprocess, sys

root = pathlib.Path(sys.argv[1])
evidence = root / "evidence/arm64"
archive = root / "assets/opencode-linux-arm64.tar.gz"
extracted = root / "extracted"
sidecar = os.environ["SIDECAR"]
sha256 = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
metadata = {
    "release_tag": os.environ["RELEASE_TAG"],
    "release_source_commit": os.environ["SOURCE_COMMIT"],
    "version": os.environ["RELEASE_VERSION"],
    "expected_bun_version": os.environ["EXPECTED_BUN"],
    "actual_bun_version": os.environ["ACTUAL_BUN"],
    "proof_harness_head": os.environ["SOURCE_COMMIT"],
    "build_classification": "native linux-arm64 candidate from clean selected source; no source overlays",
    "runner_host": (evidence / "host.txt").read_text().strip(),
    "runner_uname": (evidence / "uname.txt").read_text().strip(),
    "runtime": (evidence / "bun.txt").read_text().strip(),
    "node_version": (evidence / "node.txt").read_text().strip(),
    "bun_lock_sha256": (evidence / "bun.lock.sha256").read_text().split()[0],
    "models_dev_sha256": (evidence / "models.dev.sha256").read_text().split()[0],
    "build_inputs": {
        "OPENCODE_VERSION": os.environ["RELEASE_VERSION"],
        "MODELS_DEV_API_JSON": str(evidence / "models.dev.json"),
        "models_dev_sha256": (evidence / "models.dev.sha256").read_text().split()[0],
        "build_command": "bun run packages/opencode/script/build.ts --single --skip-install",
    },
    "clean_source": {
        "before_build": (evidence / "source-before.txt").read_text(),
        "after_install": (evidence / "source-after-install.txt").read_text(),
        "after_build": (evidence / "source-after-build.txt").read_text(),
        "after_probe": (evidence / "source-after-probe.txt").read_text(),
    },
    "archive": {
        "name": archive.name,
        "bytes": archive.stat().st_size,
        "sha256": sha256(archive),
        "companion": (root / "assets/opencode-linux-arm64.tar.gz.sha256").read_text().strip(),
        "entries": (evidence / "archive-entries.txt").read_text().splitlines(),
        "extracted_binary_sha256": sha256(extracted / "opencode"),
        "extracted_sidecar": sidecar,
        "extracted_sidecar_sha256": sha256(extracted / sidecar),
        "version_output": (evidence / "version.txt").read_text().strip(),
    },
    "native_probe": json.loads((root / "proof/evidence.json").read_text()),
}
(evidence / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY

date -u +%FT%TZ | tee "$evidence/completed-at.txt"
echo ARM-BUILD-PASS | tee "$evidence/result.txt"
