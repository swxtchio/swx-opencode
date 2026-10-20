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
    '--fast invokes each manifest entry with bun test directly from its owning package directory.' \
    'The manifest accepts blank lines and whole-line comments beginning with #; every other line must be one' \
    'committed, regular project-relative path matching packages/<package-path>/test/<path>.test.ts.' \
    'Manifest paths use / separators and cannot contain whitespace, backslashes, glob characters, inline comments,' \
    'absolute paths, or . / .. path components.' \
    'The target and its owning package must exist in the checkout, and the owning package must define a test script.' \
    'Fast mode bypasses package scripts.test and Turbo; Bun loads the owning package bunfig.toml preloads.' \
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
if [ "$mode" = full ] && [ ! -x node_modules/.bin/turbo ]; then
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
  if ! FAST_SUITE_PACKAGE_JSON="$root/$package_root/package.json" bun -e '
    const pkg = await Bun.file(process.env.FAST_SUITE_PACKAGE_JSON).json()
    if (typeof pkg.scripts?.test !== "string") process.exit(1)
  ' >/dev/null; then
    printf 'tests/run.sh: could not resolve the owning package test command on line %d: %s\n' "$line_number" "$line" >&2
    manifest_invalid=1
    break
  fi

  seen+="$line"$'\n'
  package_paths+=("$package_root")
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
execution_set_invalid=0
declare -a executed_paths=()
index=0
while [ "$index" -lt "$target_count" ]; do
  package_root=${package_paths[index]}
  target=${test_paths[index]}
  report_args=(--reporter=junit --reporter-outfile="$report_file")
  target_status=0
  selected_path="./$target"
  (cd "$root/$package_root" && GITHUB_ACTIONS=false bun test \
    "${report_args[@]}" "$selected_path") || target_status=$?

  if [ -s "$report_file" ]; then
    report_status=0
    executed_report=
    executed_report="$(FAST_SUITE_REPORT="$report_file" FAST_SUITE_TARGET="$target" bun -e '
      const report = await Bun.file(process.env.FAST_SUITE_REPORT).text()
      const target = process.env.FAST_SUITE_TARGET
      const suites = Array.from(report.matchAll(/<testsuite\b([^>]*)>/g), ([, text]) => {
        const attributes = Object.fromEntries(
          Array.from(text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g), (match) => [match[1], match[2]]),
        )
        return { file: attributes.file, tests: Number(attributes.tests), skipped: Number(attributes.skipped ?? 0) }
      })
      if (suites.some((suite) => typeof suite.file !== "string" || suite.file.length === 0)) {
        console.error("tests/run.sh: Bun JUnit report omitted file attribution for: " + target)
        process.exit(2)
      }
      const executed = Array.from(new Set(suites.map((suite) => suite.file)))
      const unexpected = executed.filter((file) => file !== target)
      if (unexpected.length) {
        console.error("tests/run.sh: Bun executed files outside manifest entry " + target + ": " + unexpected.join(", "))
        process.exit(2)
      }
      const active = suites.some(
        (suite) => suite.file === target && Number.isFinite(suite.tests) && suite.tests > suite.skipped,
      )
      if (!active) {
        console.error("tests/run.sh: Bun reported no active test cases for: " + target)
        process.exit(3)
      }
      if (executed.length !== 1) {
        console.error("tests/run.sh: Bun report did not resolve exactly one file for: " + target)
        process.exit(2)
      }
      process.stdout.write(executed[0])
      '
    )" || report_status=$?
    if [ "$report_status" -ne 0 ]; then
      if [ "$report_status" -eq 3 ]; then
        coverage_invalid=1
      else
        execution_set_invalid=1
      fi
      target_status=1
    else
      executed_paths+=("$executed_report")
    fi
  elif [ "$target_status" -eq 0 ]; then
    coverage_invalid=1
    target_status=1
  else
    execution_set_invalid=1
  fi
  if [ "$target_status" -ne 0 ] && [ -s "$report_file" ]; then
    printf 'tests/run.sh: JUnit report for %s:\n' "$target" >&2
    while IFS= read -r report_line || [ -n "$report_line" ]; do
      printf '%s\n' "$report_line" >&2
    done < "$report_file"
  fi
  rm -f -- "$report_file"
  if [ "$target_status" -ne 0 ]; then fast_status=1; fi
  index=$((index + 1))
done

if [ "$coverage_invalid" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=no-test-cases exit_code=1'
  exit 1
fi
if [ "$execution_set_invalid" -ne 0 ] || [ "${#executed_paths[@]}" -ne "$target_count" ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=execution-set-mismatch exit_code=1'
  exit 1
fi
index=0
while [ "$index" -lt "$target_count" ]; do
  if [ "${executed_paths[index]}" != "${test_paths[index]}" ]; then
    printf 'tests/run.sh: JUnit execution set differs from manifest at: %s\n' "${test_paths[index]}" >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=execution-set-mismatch exit_code=1'
    exit 1
  fi
  index=$((index + 1))
done
printf 'tests/run.sh: verdict=completed mode=fast target_count=%d exit_code=%d\n' "$target_count" "$fast_status"
exit "$fast_status"
