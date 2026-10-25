"""Prepare a scratch-only jail Dockerfile that installs the exact local candidate archive."""

import sys
from pathlib import Path


source = Path(sys.argv[1])
destination = Path(sys.argv[2])
lines = source.read_text().splitlines(keepends=True)
curl_lines = [index for index, line in enumerate(lines) if "curl -fsSL --proto '=https'" in line]
if len(curl_lines) != 1:
    raise SystemExit(f"expected one release-archive download, found {len(curl_lines)}")

curl_index = curl_lines[0]
url_indexes = [
    index
    for index in range(curl_index, min(curl_index + 4, len(lines)))
    if "https://github.com/swxtchio/swx-opencode/releases/download/" in lines[index]
]
if len(url_indexes) != 1:
    raise SystemExit(f"expected one OpenCode asset URL in the download command, found {len(url_indexes)}")
url_index = url_indexes[0]
if not lines[url_index].rstrip().endswith("; \\"):
    raise SystemExit("release download URL is not the expected Dockerfile continuation")

run_indexes = [
    index
    for index, line in enumerate(lines)
    if line == "RUN set -eu; \\\n" and index + 1 < len(lines) and "OPENCODE_RELEASE_PLATFORM" in lines[index + 1]
]
if len(run_indexes) != 1:
    raise SystemExit(f"expected one platform-gated archive install block, found {len(run_indexes)}")

lines[run_indexes[0]:run_indexes[0]] = ["COPY jail/opencode-candidate.tar.gz /tmp/opencode-linux-arm64.tar.gz\n"]
curl_index = next(index for index, line in enumerate(lines) if "curl -fsSL --proto '=https'" in line)
url_index = next(
    index
    for index in range(curl_index, len(lines))
    if "https://github.com/swxtchio/swx-opencode/releases/download/" in lines[index]
)
del lines[curl_index:url_index + 1]
destination.write_text("".join(lines))
