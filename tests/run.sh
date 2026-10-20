#!/usr/bin/env bash
# Runs the repository's unit suite: the per-package `test` scripts dispatched by
# turbo, exactly as the "Run unit tests" step of .github/workflows/test.yml runs
# them. The root `npm test` / `bun test` refuse on purpose, so this is the
# entrypoint for running everything from the root.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  if [ "$#" -ne 1 ]; then
    printf '%s\n' 'tests/run.sh: help does not accept additional arguments' >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=help reason=unexpected-arguments exit_code=2'
    exit 2
  fi
  printf '%s\n' \
    'Usage: tests/run.sh [turbo arguments...]' \
    '       tests/run.sh --fast' \
    '       tests/run.sh --help' \
    '' \
    'With no selector, runs the complete per-package Turbo test suite and forwards all arguments to Turbo.' \
    '--fast runs exactly the entries in tests/fast-suite.txt through their owning package test tasks.' \
    'The manifest accepts blank lines and whole-line comments beginning with #; every other line must be one' \
    'committed, regular project-relative path matching packages/<package-path>/test/<path>.test.ts.' \
    'Manifest paths use / separators and cannot contain whitespace, backslashes, glob characters, inline comments,' \
    'absolute paths, or . / .. path components.' \
    'The target and its owning package must exist in the checkout, and the owning package must define a test script.' \
    'Fast mode uses a registered package Turbo task and its build dependencies, or the package test script' \
    'when no task is registered.' \
    'Both dispatch paths preserve package scripts and bunfig preloads.' \
    'Fast mode refuses if a selected file reports no active test cases.' \
    'Fast mode refuses a checkout with tracked or untracked changes; full mode preserves the original Turbo behavior.' \
    'A terminal verdict is printed on every exit.'
  printf '%s\n' 'tests/run.sh: verdict=completed mode=help exit_code=0'
  exit 0
fi

mode=full
if [ "${1:-}" = "--fast" ]; then
  mode=fast
  shift
  if [ "$#" -ne 0 ]; then
    printf '%s\n' 'tests/run.sh: --fast does not accept additional test arguments' >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=unexpected-arguments exit_code=2'
    exit 2
  fi
fi

if [ "$mode" = fast ]; then
  if ! git_status="$(git status --porcelain --untracked-files=all 2>/dev/null)"; then
    printf '%s\n' 'tests/run.sh: could not establish checkout status' >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=git-status-unavailable exit_code=1'
    exit 1
  fi
  if [ -n "$git_status" ]; then
    printf '%s\n' 'tests/run.sh: refusing to execute tests from a dirty checkout' >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=dirty-checkout exit_code=1'
    exit 1
  fi
fi

if ! command -v bun >/dev/null 2>&1; then
  printf '%s\n' 'tests/run.sh: bun is not on PATH; use the version specified by package.json packageManager' >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode='"$mode"' reason=bun-not-found exit_code=127'
  exit 127
fi
if [ ! -x node_modules/.bin/turbo ]; then
  printf '%s\n' 'tests/run.sh: dependencies are not installed (no node_modules/.bin/turbo); run bun install first' >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode='"$mode"' reason=dependencies-not-installed exit_code=1'
  exit 1
fi

if [ "$mode" = full ]; then
  status=0
  GITHUB_ACTIONS=false bun turbo test "$@" || status=$?
  printf 'tests/run.sh: verdict=completed mode=full exit_code=%d\n' "$status"
  exit "$status"
fi

manifest=tests/fast-suite.txt
if [ ! -f "$manifest" ] || [ -L "$manifest" ] || [ ! -r "$manifest" ] \
  || ! git ls-files --error-unmatch -- "$manifest" >/dev/null 2>&1; then
  printf 'tests/run.sh: fast manifest is missing, unreadable, or untracked: %s\n' "$manifest" >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=fast-manifest-unavailable exit_code=1'
  exit 1
fi

declare -a package_paths=()
declare -a package_modes=()
declare -a test_paths=()
seen=$'\n'
target_count=0
line_number=0
manifest_invalid=0
while IFS= read -r line || [ -n "$line" ]; do
  line_number=$((line_number + 1))
  line=${line%$'\r'}
  case "$line" in
    ''|\#*) continue ;;
    *[[:space:]]*)
      printf 'tests/run.sh: malformed fast manifest entry on line %d: %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
      ;;
  esac
  case "$line" in
    packages/*/test/*.test.ts) ;;
    *)
      printf 'tests/run.sh: malformed fast manifest entry on line %d: %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
      ;;
  esac
  case "/$line/" in
    *"//"*|*"/./"*|*"/../"*)
      printf 'tests/run.sh: fast manifest path is not normalized on line %d: %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
      ;;
  esac
  case "$line" in
    *\\*|*'*'*|*'?'*|*'['*|*']'*)
      printf 'tests/run.sh: reserved character in fast manifest entry (line %d): %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
      ;;
  esac
  case "$seen" in
    *$'\n'"$line"$'\n'*)
      printf 'tests/run.sh: duplicate fast manifest entry on line %d: %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
      ;;
  esac

  relative=${line#packages/}
  package=${relative%%/test/*}
  package_root="packages/$package"
  test_file=${relative#"$package"/test/}
  if [ ! -f "$root/$line" ] || [ -L "$root/$line" ] \
    || [ ! -f "$root/$package_root/package.json" ] || [ -L "$root/$package_root/package.json" ] \
    || ! git ls-files --error-unmatch -- "$package_root/package.json" >/dev/null 2>&1; then
    printf 'tests/run.sh: entry is not a regular test file in a package (line %d): %s\n' "$line_number" "$line" >&2
    manifest_invalid=1
    break
  fi
  if ! git ls-files --error-unmatch -- "$line" >/dev/null 2>&1; then
    printf 'tests/run.sh: fast manifest entry is not tracked on line %d: %s\n' "$line_number" "$line" >&2
    manifest_invalid=1
    break
  fi
  package_mode=
  if ! package_mode="$(
    FAST_SUITE_PACKAGE_JSON="$root/$package_root/package.json" \
      FAST_SUITE_TURBO_JSON="$root/turbo.json" \
      bun -e '
        const pkg = await Bun.file(process.env.FAST_SUITE_PACKAGE_JSON).json()
        if (typeof pkg.scripts?.test !== "string") process.exit(1)
        const config = await Bun.file(process.env.FAST_SUITE_TURBO_JSON).json()
        const task = String(pkg.name) + "#test"
        process.stdout.write(config.tasks?.test || config.tasks?.[task] ? "turbo" : "package")
      '
  )"; then
    printf 'tests/run.sh: could not resolve the owning package test command on line %d: %s\n' "$line_number" "$line" >&2
    manifest_invalid=1
    break
  fi

  seen+="$line"$'\n'
  package_paths+=("$package_root")
  package_modes+=("$package_mode")
  test_paths+=("test/$test_file")
  target_count=$((target_count + 1))
done < "$manifest"

if [ "$manifest_invalid" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=invalid-fast-manifest exit_code=1'
  exit 1
fi
if [ "$target_count" -eq 0 ]; then
  printf 'tests/run.sh: fast manifest contains no test targets: %s\n' "$manifest" >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=empty-fast-manifest exit_code=1'
  exit 1
fi

report_file="$root/.fast-suite-report.$$.xml"
# A tracked .test.ts file can contain no registered cases, so check Bun's per-file JUnit results.
trap 'rm -f -- "$report_file"' EXIT
fast_status=0
coverage_invalid=0
index=0
while [ "$index" -lt "$target_count" ]; do
  package_root=${package_paths[index]}
  package_mode=${package_modes[index]}
  selected=()
  report_args=(--reporter=junit --reporter-outfile="$report_file")
  while [ "$index" -lt "$target_count" ] && [ "${package_paths[index]}" = "$package_root" ]; do
    selected+=("${test_paths[index]}")
    index=$((index + 1))
  done
  target_list=$(printf '%s\n' "${selected[@]}")
  package_status=0
  # Bun treats bare positional paths as substring filters; ./ selects the exact target file.
  selected_paths=()
  for test_path in "${selected[@]}"; do
    selected_paths+=("./$test_path")
  done
  if [ "$package_mode" = turbo ]; then
    GITHUB_ACTIONS=false bun turbo test \
      --filter="./$package_root" --force -- "${report_args[@]}" "${selected_paths[@]}" || package_status=$?
  else
    (cd "$root/$package_root" && GITHUB_ACTIONS=false bun run test -- \
      "${report_args[@]}" "${selected_paths[@]}") || package_status=$?
  fi

  if [ "$package_status" -eq 0 ]; then
    report_status=0
    FAST_SUITE_REPORT="$report_file" FAST_SUITE_TARGETS="$target_list" bun -e '
      const report = await Bun.file(process.env.FAST_SUITE_REPORT).text()
      const targets = (process.env.FAST_SUITE_TARGETS ?? "").split("\n").filter(Boolean)
      const suites = Array.from(report.matchAll(/<testsuite\b([^>]*)>/g), ([, text]) => {
        const attributes = Object.fromEntries(
          Array.from(text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g), (match) => [match[1], match[2]]),
        )
        return { file: attributes.file, tests: Number(attributes.tests), skipped: Number(attributes.skipped ?? 0) }
      })
      const missing = targets.filter((target) =>
        !suites.some((suite) => suite.file === target && Number.isFinite(suite.tests) && suite.tests > suite.skipped),
      )
      if (missing.length) {
        console.error("tests/run.sh: Bun reported no active test cases for: " + missing.join(", "))
        process.exit(1)
      }
      ' || report_status=$?
    if [ "$report_status" -ne 0 ]; then
      coverage_invalid=1
      package_status=1
    fi
  elif [ -s "$report_file" ]; then
    printf 'tests/run.sh: JUnit report for %s:\n' "$package_root" >&2
    while IFS= read -r report_line || [ -n "$report_line" ]; do
      printf '%s\n' "$report_line" >&2
    done < "$report_file"
  fi
  rm -f -- "$report_file"
  if [ "$fast_status" -eq 0 ] && [ "$package_status" -ne 0 ]; then
    fast_status=$package_status
  fi
done

if [ "$coverage_invalid" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=no-test-cases exit_code=1'
  exit 1
fi
printf 'tests/run.sh: verdict=completed mode=fast target_count=%d exit_code=%d\n' "$target_count" "$fast_status"
exit "$fast_status"
