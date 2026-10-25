#!/usr/bin/env bash
set -euo pipefail

root=/home/byates/vm14-builds/opencode-release-832df553
source_commit=832df553518d8bb581952433c72495a472da8c5d
version=1.18.32-swxtch.832df553
tag=v$version
consumer_commit=8ac1489e7d3ecad9f9a09431d3df54b6b69ad8e6
evidence=$root/evidence/jail
checkout=$root/consumer/swx-abbe
variant=$checkout/components/orch/services/opencode-variant
image=opencode-variant-jail:v2-egress-$version-candidate

mkdir -p "$evidence/tool" "$root/rig-tmp"
if docker image inspect "$image" >/dev/null 2>&1; then
  echo "candidate image tag already exists: $image" >&2
  exit 1
fi
cleanup() {
  docker image rm "$image" > "$evidence/image-removal.txt" 2>&1 || true
}
trap cleanup EXIT

date -u +%FT%TZ | tee "$evidence/started-at.txt"
hostname -f | tee "$evidence/host.txt"
uname -a | tee "$evidence/uname.txt"
docker version | tee "$evidence/docker-version.txt"
docker network ls --format '{{.ID}} {{.Name}}' | tee "$evidence/networks-before.txt"
docker ps -a --no-trunc --format '{{.ID}} {{.Image}} {{.Names}}' | tee "$evidence/containers-before.txt"

archive=$root/assets/opencode-linux-arm64.tar.gz
companion=$archive.sha256
expected_archive_sha=$(cut -d' ' -f1 "$companion")
test "$(sha256sum "$archive" | cut -d' ' -f1)" = "$expected_archive_sha"
cp "$archive" "$variant/jail/opencode-candidate.tar.gz"
sha256sum "$archive" "$variant/jail/opencode-candidate.tar.gz" | tee "$evidence/candidate-archive.sha256"

sha256sum "$variant/jail/Dockerfile.jail" | tee "$evidence/Dockerfile.jail.sha256"
python3 "$root/rig/prepare-candidate-dockerfile.py" "$variant/jail/Dockerfile.jail" "$variant/jail/Dockerfile.jail.candidate"
sha256sum "$variant/jail/Dockerfile.jail.candidate" | tee "$evidence/Dockerfile.jail.candidate.sha256"
if diff -u "$variant/jail/Dockerfile.jail" "$variant/jail/Dockerfile.jail.candidate" > "$evidence/Dockerfile.candidate.diff"; then
  echo 'candidate Dockerfile unexpectedly matches the published-download Dockerfile' >&2
  exit 1
fi
grep -F 'COPY jail/opencode-candidate.tar.gz /tmp/opencode-linux-arm64.tar.gz' "$variant/jail/Dockerfile.jail.candidate"

if (cd "$variant" && DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain --platform linux/arm64 \
  -f jail/Dockerfile.jail.candidate -t "$image" \
  --build-arg OPENCODE_RELEASE_TAG="$tag" \
  --build-arg OPENCODE_RELEASE_SOURCE_REVISION="$source_commit" \
  --build-arg OPENCODE_ARCHIVE_SHA256="$expected_archive_sha" \
  --build-arg OPENCODE_VERSION="$version" \
  --build-arg OPENCODE_RELEASE_PLATFORM=linux/arm64 \
  --build-arg GIT_COMMIT="$consumer_commit" .) > "$evidence/image-build.log" 2>&1; then
  echo 'image build passed' | tee "$evidence/image-build-result.txt"
else
  status=$?
  cat "$evidence/image-build.log"
  echo "image build failed with exit $status" | tee "$evidence/image-build-result.txt"
  exit "$status"
fi
grep -F "/tmp/opencode-linux-arm64.tar.gz: OK" "$evidence/image-build.log" | tee "$evidence/dockerfile-digest-check.txt"
image_id=$(docker image inspect "$image" --format '{{.Id}}')
printf '%s\n' "$image_id" | tee "$evidence/image-id.txt"
docker image inspect "$image" --format '{{.Id}} {{.Architecture}} {{.Os}}' | tee "$evidence/image-platform.txt"
docker run --rm --network none --entrypoint sh "$image" -c \
  'sha256sum /usr/local/bin/opencode /usr/local/bin/libfff_c-*.so; /usr/local/bin/opencode --version' \
  | tee "$evidence/image-bytes-and-version.txt"

candidate_binary_sha=$(sha256sum "$root/extracted/opencode" | cut -d' ' -f1)
candidate_sidecar_sha=$(sha256sum "$root/extracted"/libfff_c-*.so | cut -d' ' -f1)
grep -F "$candidate_binary_sha" "$evidence/image-bytes-and-version.txt"
grep -F "$candidate_sidecar_sha" "$evidence/image-bytes-and-version.txt"
grep -Fx "$version" "$evidence/image-bytes-and-version.txt"

set +e
(cd "$checkout" && TMPDIR="$root/rig-tmp" PYTHONDONTWRITEBYTECODE=1 PYTEST_ADDOPTS='-p no:cacheprovider' \
  OPENCODE_VARIANT_JAIL_IMAGE="$image" OPENCODE_SERVER_PASSWORD=local-rig-password \
  RELEASE_CUT_6_SOURCE_COMMIT="$source_commit" RELEASE_CUT_6_CONSUMER_COMMIT="$consumer_commit" \
  RELEASE_CUT_6_ARCHIVE_SHA256="$expected_archive_sha" RELEASE_CUT_6_IMAGE_ID="$image_id" \
  RELEASE_CUT_6_EVIDENCE="$evidence/tool" \
  python3 -m pytest -q components/orch/services/opencode-variant/tests/test_release_cut_6_candidate.py) \
  > "$evidence/tool-pytest.log" 2>&1
pytest_status=$?
printf 'exit=%s\n' "$pytest_status" | tee "$evidence/tool-pytest.result"

(cd "$checkout" && TMPDIR="$root/rig-tmp" PYTHONDONTWRITEBYTECODE=1 \
  OPENCODE_VARIANT_JAIL_IMAGE="$image" OPENCODE_SERVER_PASSWORD=local-rig-password \
  python3 "$root/rig/jail_v2_egress_probe.py" v1-control compact) \
  > "$evidence/control-compact.log" 2>&1
probe_status=$?
printf 'exit=%s\n' "$probe_status" | tee "$evidence/control-compact.result"
RELEASE_SOURCE_COMMIT="$source_commit" CONSUMER_SOURCE_COMMIT="$consumer_commit" ARCHIVE_SHA256="$expected_archive_sha" \
  IMAGE_ID="$image_id" IMAGE="$image" PYTEST_STATUS="$pytest_status" PROBE_STATUS="$probe_status" \
  python3 - "$evidence/control-compact-identity.json" <<'PY'
import json, os, sys

identity = {
    "release_source_commit": os.environ["RELEASE_SOURCE_COMMIT"],
    "consumer_source_commit": os.environ["CONSUMER_SOURCE_COMMIT"],
    "candidate_archive_sha256": os.environ["ARCHIVE_SHA256"],
    "candidate_image": os.environ["IMAGE"],
    "candidate_image_id": os.environ["IMAGE_ID"],
    "tool_acceptance_exit": int(os.environ["PYTEST_STATUS"]),
    "v1_control_and_compaction_exit": int(os.environ["PROBE_STATUS"]),
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(identity, output, indent=2)
    output.write("\n")
PY
set -e
cat "$evidence/tool-pytest.log"
cat "$evidence/control-compact.log"

date -u +%FT%TZ | tee "$evidence/completed-at.txt"
docker ps -a --no-trunc --format '{{.ID}} {{.Image}} {{.Names}}' | tee "$evidence/containers-after-tests.txt"
docker network ls --format '{{.ID}} {{.Name}}' | tee "$evidence/networks-after-tests.txt"
cleanup
trap - EXIT
docker ps -a --no-trunc --format '{{.ID}} {{.Image}} {{.Names}}' | tee "$evidence/containers-after-cleanup.txt"
docker network ls --format '{{.ID}} {{.Name}}' | tee "$evidence/networks-after-cleanup.txt"
docker image ls --no-trunc --format '{{.ID}} {{.Repository}}:{{.Tag}}' | grep -F "$image" > "$evidence/image-leftovers.txt" || true
docker ps -a --no-trunc --format '{{.ID}} {{.Image}} {{.Names}}' | grep -F "$image" > "$evidence/container-leftovers.txt" || true
if test -s "$evidence/image-leftovers.txt" || test -s "$evidence/container-leftovers.txt"; then
  echo 'task Docker resources remain after the jail lap' >&2
  exit 1
fi
test "$pytest_status" -eq 0
test "$probe_status" -eq 0
echo JAIL-RIG-PASS | tee "$evidence/result.txt"
