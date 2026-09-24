#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  printf 'Usage: %s\nBuild and install this checkout as the local swxtch opencode binary.\n' "$0"
  exit 0
fi
if [[ $# -ne 0 ]]; then
  printf 'Unknown argument: %s\n' "$1" >&2
  exit 2
fi

root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$root"
if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  printf 'Commit or remove working-tree changes before creating a versioned local release.\n' >&2
  exit 1
fi

version=$(node -p 'require("./packages/opencode/package.json").version')
bun_version=$(node -p 'require("./package.json").packageManager.split("@")[1]')
release="$version-swxtch.$(git rev-parse --short=8 HEAD)"
platform=$(node -p 'process.platform + "-" + process.arch')
built="$root/packages/opencode/dist/opencode-$platform/bin/opencode"
target="${OPENCODE_LOCAL_BIN:-$HOME/.local/opencode-swxtch/bin/opencode}"
database="${OPENCODE_DB_PATH:-$HOME/.local/share/opencode/opencode.db}"
if [[ "$target" != /* || "$database" != /* ]]; then
  printf 'OPENCODE_LOCAL_BIN and OPENCODE_DB_PATH must be absolute paths.\n' >&2
  exit 1
fi

if command -v opencode >/dev/null 2>&1; then
  resolved=$(node -p 'require("fs").realpathSync(process.argv[1])' "$(command -v opencode)")
  installed="$target"
  if [[ -e "$target" ]]; then
    installed=$(node -p 'require("fs").realpathSync(process.argv[1])' "$target")
  fi
  if [[ "$resolved" != "$installed" ]]; then
    printf 'PATH resolves opencode to %s, not %s. Set OPENCODE_LOCAL_BIN to that binary or adjust PATH.\n' "$resolved" "$target" >&2
    exit 1
  fi
fi
if [[ -f "$target" && "$("$target" db path)" != "$database" ]]; then
  printf 'Installed opencode uses a different database; set OPENCODE_DB_PATH to its path before replacing it.\n' >&2
  exit 1
fi

printf 'Building %s with Bun %s\n' "$release" "$bun_version"
npx --yes "bun@$bun_version" install --frozen-lockfile
OPENCODE_VERSION="$release" npx --yes "bun@$bun_version" run packages/opencode/script/build.ts --single --skip-install

if [[ "$("$built" --version)" != "$release" || "$("$built" db path)" != "$database" ]]; then
  printf 'Built binary version or database path did not match; install aborted.\n' >&2
  exit 1
fi

mkdir -p "$(dirname "$target")"
backup=""
if [[ -f "$target" ]]; then
  old=$("$target" --version)
  backup="$target.$old"
  cp -p "$target" "$backup"
  printf 'Previous binary backed up at %s\n' "$backup"
fi

temporary="$target.new.$$"
trap 'rm -f "$temporary"' EXIT
install -m 755 "$built" "$temporary"
mv -f "$temporary" "$target"

if [[ "$("$target" --version)" != "$release" || "$("$target" db path)" != "$database" ]]; then
  if [[ -n "$backup" ]]; then
    cp -p "$backup" "$temporary"
    mv -f "$temporary" "$target"
    printf 'Installed binary verification failed; restored %s.\n' "$backup" >&2
  else
    printf 'Installed binary verification failed; no previous binary to restore.\n' >&2
  fi
  exit 1
fi
printf 'Installed %s at %s (database: %s)\n' "$release" "$target" "$database"
if command -v opencode >/dev/null 2>&1; then
  printf 'PATH opencode: %s\n' "$(opencode --version)"
else
  printf 'Add %s to PATH or symlink %s into a directory on PATH.\n' "$(dirname "$target")" "$target"
fi
