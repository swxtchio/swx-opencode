#!/usr/bin/env bash
# Runs the repository's unit suite: the per-package `test` scripts dispatched by
# turbo, exactly as the "Run unit tests" step of .github/workflows/test.yml runs
# them. The root `npm test` / `bun test` refuse on purpose, so this is the
# entrypoint for running everything from the root.
#
# Extra arguments go to `turbo test`, e.g. `tests/run.sh --filter=@opencode-ai/core`.
# A missing Bun or an uninstalled workspace fails instead of passing vacuously,
# and the suite's own exit status is returned unchanged.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! command -v bun >/dev/null 2>&1; then
  echo "tests/run.sh: bun is not on PATH; the unit suite needs the Bun named in package.json packageManager" >&2
  exit 127
fi
if [ ! -x node_modules/.bin/turbo ]; then
  echo "tests/run.sh: dependencies are not installed (no node_modules/.bin/turbo); run bun install first" >&2
  exit 1
fi

# GITHUB_ACTIONS=false keeps turbo's output plain, as in CI.
GITHUB_ACTIONS=false exec bun turbo test "$@"
