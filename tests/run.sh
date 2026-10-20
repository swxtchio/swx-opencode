#!/usr/bin/env bash
# Runs the repository's unit suite: the per-package `test` scripts dispatched by
# turbo, exactly as the "Run unit tests" step of .github/workflows/test.yml runs
# them. The root `npm test` / `bun test` refuse on purpose, so this is the
# entrypoint for running everything from the root.
# Direct `bun test` receives one already-validated path per manifest line. Forward
# every supported Bun test option and leading environment assignment; run each
# registered Turbo test task's declared `^build` prerequisites first. The selected
# tasks currently declare no explicit input globs. Always run `posttest` after a
# pretest/test attempt, including failures; JUnit-less nonzero exits remain attributed
# refusals. The unexpected-file guard catches extra attributed suites.
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
    'The target and owning package must exist; scripts.test must be a direct bun test command.' \
    'Fast mode bypasses scripts.test selectors but forwards supported Bun options and literal leading environment' \
    'assignments. Registered Turbo test tasks run their declared ^build preparation through Turbo first; unsupported' \
    'Turbo dependencies or explicit test inputs refuse instead of being silently skipped.' \
    'Optional pretest/posttest hooks run directly around each target; posttest runs even after earlier failures.' \
    'A nonzero target with no JUnit report is refused with its target attribution, not labeled an execution-set mismatch.' \
    'Bun is invoked directly with the owning package bunfig.toml and exactly one manifest path.' \
    'Fast mode refuses if a selected file reports no active test cases.' \
    'Only --fast refuses a checkout with tracked or untracked changes; full mode preserves the original Turbo behavior.' \
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
declare -a package_config_paths=()
declare -a configured_packages=()
declare -a configured_package_paths=()
seen=$'\n'
target_count=0
line_number=0
manifest_invalid=0
if ! config_dir="$(mktemp -d "${TMPDIR:-/tmp}/fast-suite-config.XXXXXX")"; then
  printf '%s\n' 'tests/run.sh: could not create temporary package-config directory' >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=temporary-directory-unavailable exit_code=1'
  exit 1
fi
report_file="$root/.fast-suite-report.$$.xml"
trap 'rm -f -- "$report_file" "$config_dir"/*; rmdir -- "$config_dir" 2>/dev/null || true' EXIT
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
  package_config_index=0
  while [ "$package_config_index" -lt "${#configured_packages[@]}" ] \
    && [ "${configured_packages[package_config_index]}" != "$package_root" ]; do
    package_config_index=$((package_config_index + 1))
  done
  if [ "$package_config_index" -eq "${#configured_packages[@]}" ]; then
    config_path="$config_dir/$package_config_index"
    if ! FAST_SUITE_PACKAGE_JSON="$root/$package_root/package.json" \
      FAST_SUITE_TURBO_JSON="$root/turbo.json" bun -e '
      const pkg = await Bun.file(process.env.FAST_SUITE_PACKAGE_JSON).json()
      const turbo = await Bun.file(process.env.FAST_SUITE_TURBO_JSON).json()
      const fail = (message) => {
        console.error("tests/run.sh: " + message)
        process.exit(1)
      }
      if (typeof pkg.scripts?.test !== "string") fail("scripts.test must be a string")
      for (const hook of ["pretest", "posttest"]) {
        if (pkg.scripts[hook] !== undefined && typeof pkg.scripts[hook] !== "string") {
          fail("package " + hook + " hook must be a string")
        }
      }
      const script = pkg.scripts.test
      const single = String.fromCharCode(39)
      const words = []
      let word = ""
      let quote = ""
      let escaped = false
      let started = false
      for (const character of script) {
        if (escaped) {
          // Match Bun shell semantics: double-quoted backslashes survive before non-special characters.
          if (quote === "\"" && !["$", "`", "\"", "\\", "\n"].includes(character)) word += "\\"
          if (quote !== "\"" || character !== "\n") word += character
          escaped = false
          started = true
          continue
        }
        if (quote === single) {
          if (character === single) quote = ""
          else word += character
          started = true
          continue
        }
        if (quote === "\"") {
          if (character === "\\") {
            escaped = true
            continue
          }
          if (character === "\"") quote = ""
          else word += character
          started = true
          continue
        }
        if (character === single || character === "\"") {
          quote = character
          started = true
          continue
        }
        if (character === "\\") {
          escaped = true
          continue
        }
        if (character === " " || character === "\t" || character === "\n" || character === "\r") {
          if (started) words.push(word)
          word = ""
          started = false
          continue
        }
        if (/[;&|<>]/.test(character)) fail("scripts.test must be a direct bun test command without shell operators")
        word += character
        started = true
      }
      if (quote || escaped) fail("scripts.test contains an unterminated quote or escape")
      if (started) words.push(word)
      const assignment = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/
      const environment = []
      let command = 0
      while (command < words.length && assignment.test(words[command])) {
        const value = words[command].slice(words[command].indexOf("=") + 1)
        if (/[\u0000\r\n$`*?\[\]]/.test(value) || value.startsWith("~")) {
          fail("scripts.test environment assignments must use literal, non-glob values")
        }
        environment.push(words[command])
        command += 1
      }
      if (words[command] !== "bun" || words[command + 1] !== "test") {
        fail("scripts.test must begin with optional literal assignments followed by bun test")
      }
      const booleanOptions = new Set([
        "--no-orphans",
        "-u",
        "--update-snapshots",
        "--todo",
        "--only",
        "--pass-with-no-tests",
        "--concurrent",
        "--randomize",
        "--coverage",
        "--dots",
        "--only-failures",
        "--isolate",
        "--no-isolate",
        "--test-worker",
        "--update-timings",
      ])
      const valueOptions = new Set([
        "--timeout",
        "--rerun-each",
        "--retry",
        "--seed",
        "--coverage-reporter",
        "--coverage-dir",
        "-t",
        "--test-name-pattern",
        "--max-concurrency",
        "--parallel-delay",
        "--shard",
        "--timings",
      ])
      const optionalValueOptions = new Set(["--bail", "--parallel"])
      const options = []
      for (let index = command + 2; index < words.length; index += 1) {
        const argument = words[index]
        if (argument === "--") fail("scripts.test must not forward arguments after --")
        if (!argument.startsWith("-")) continue
        const equals = argument.indexOf("=")
        const option = equals === -1 ? argument : argument.slice(0, equals)
        if (option === "--reporter-outfile" || option === "--dots") {
          fail("scripts.test reporter options conflict with fast mode JUnit verification")
        }
        if (option === "--reporter") {
          const reporter = equals === -1 ? words[index + 1] : argument.slice(equals + 1)
          if (reporter !== "junit") fail("scripts.test reporter must be junit in fast mode")
          options.push(argument)
          if (equals === -1) {
            options.push(reporter)
            index += 1
          }
          continue
        }
        if (booleanOptions.has(option)) {
          options.push(argument)
          continue
        }
        if (valueOptions.has(option)) {
          const value = equals === -1 ? words[index + 1] : argument.slice(equals + 1)
          if (value === undefined || value === "") fail("scripts.test option " + option + " requires a value")
          options.push(argument)
          if (equals === -1) {
            options.push(value)
            index += 1
          }
          continue
        }
        if (optionalValueOptions.has(option)) {
          if (equals !== -1) {
            options.push(argument)
            continue
          }
          options.push(argument)
          if (/^\d+$/.test(words[index + 1] ?? "")) {
            options.push(words[index + 1])
            index += 1
          }
          continue
        }
        fail("unsupported scripts.test Bun option: " + option)
      }
      const task = turbo.tasks?.[String(pkg.name) + "#test"] ?? turbo.tasks?.test
      let prepareBuild = "0"
      if (task !== undefined) {
        if (typeof task !== "object" || task === null || Array.isArray(task)) fail("Turbo test task must be an object")
        const inputs = task.inputs
        if (inputs !== undefined && (!Array.isArray(inputs) || inputs.length > 0)) {
          fail("fast mode cannot honor explicit Turbo test inputs")
        }
        const dependencies = task.dependsOn ?? []
        if (!Array.isArray(dependencies) || dependencies.some((dependency) => typeof dependency !== "string")) {
          fail("Turbo test task dependencies must be strings")
        }
        if (dependencies.some((dependency) => dependency !== "^build")) {
          fail("unsupported Turbo test task dependency: " + dependencies.find((dependency) => dependency !== "^build"))
        }
        if (dependencies.includes("^build")) prepareBuild = "1"
      }
      const fields = [
        pkg.scripts.pretest === undefined ? "0" : "1",
        pkg.scripts.posttest === undefined ? "0" : "1",
        String(environment.length),
        ...environment,
        String(options.length),
        ...options,
        prepareBuild,
      ]
      if (fields.some((field) => /[\u0000\r\n]/.test(field))) fail("scripts.test fields cannot contain newline characters")
      process.stdout.write(fields.join("\n") + "\n")
    ' > "$config_path"; then
      printf 'tests/run.sh: could not read the owning package test settings on line %d: %s\n' "$line_number" "$line" >&2
      manifest_invalid=1
      break
    fi
    configured_packages+=("$package_root")
    configured_package_paths+=("$config_path")
  fi

  seen+="$line"$'\n'
  package_paths+=("$package_root")
  test_paths+=("test/$test_file")
  package_config_paths+=("${configured_package_paths[package_config_index]}")
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

# A tracked .test.ts file can contain no registered cases, so check Bun's per-file JUnit results.
declare -a turbo_build_filters=()
seen_turbo_build_filters=$'\n'
index=0
while [ "$index" -lt "$target_count" ]; do
  package_config=()
  while IFS= read -r field; do package_config+=("$field"); done < "${package_config_paths[index]}"
  script_env_count=${package_config[2]}
  script_option_count_index=$((3 + script_env_count))
  script_option_count=${package_config[script_option_count_index]}
  turbo_prepare_index=$((script_option_count_index + 1 + script_option_count))
  if [ "${package_config[turbo_prepare_index]}" = 1 ]; then
    filter="--filter=./${package_paths[index]}^..."
    case "$seen_turbo_build_filters" in
      *$'\n'"$filter"$'\n'*) ;;
      *)
        seen_turbo_build_filters+="$filter"$'\n'
        turbo_build_filters+=("$filter")
        ;;
    esac
  fi
  index=$((index + 1))
done
if [ "${#turbo_build_filters[@]}" -gt 0 ]; then
  preparation_status=0
  GITHUB_ACTIONS=false bun turbo run build "${turbo_build_filters[@]}" || preparation_status=$?
  if [ "$preparation_status" -ne 0 ]; then
    printf '%s\n' 'tests/run.sh: Turbo-declared build preparation failed for fast-suite targets' >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=turbo-preparation-failed exit_code=1'
    exit 1
  fi
fi

fast_status=0
coverage_invalid=0
execution_set_invalid=0
attributed_refusal=0
attributed_failure_target=
attributed_failure_phase=
index=0
while [ "$index" -lt "$target_count" ]; do
  package_root=${package_paths[index]}
  target=${test_paths[index]}
  package_config=()
  while IFS= read -r field; do package_config+=("$field"); done < "${package_config_paths[index]}"
  has_pretest=${package_config[0]}
  has_posttest=${package_config[1]}
  script_env_count=${package_config[2]}
  script_env=("${package_config[@]:3:script_env_count}")
  script_option_count_index=$((3 + script_env_count))
  script_option_count=${package_config[script_option_count_index]}
  script_options_index=$((script_option_count_index + 1))
  script_options=("${package_config[@]:script_options_index:script_option_count}")
  target_status=0
  target_failure_phase=
  test_invoked=1
  if [ "$has_pretest" = 1 ]; then
    pretest_status=0
    GITHUB_ACTIONS=false bun --cwd="$root/$package_root" run pretest || pretest_status=$?
    if [ "$pretest_status" -ne 0 ]; then
      target_status=$pretest_status
      target_failure_phase="pretest"
      test_invoked=0
    fi
  fi
  report_args=(--reporter=junit --reporter-outfile="$report_file")
  selected_path="./$target"
  # Bun resolves bunfig.toml and relative preload paths from its own working directory.
  if [ "$test_invoked" -eq 1 ]; then
    test_status=0
    env GITHUB_ACTIONS=false "${script_env[@]}" bun --cwd="$root/$package_root" test \
      "${script_options[@]}" "${report_args[@]}" "$selected_path" || test_status=$?
    if [ "$test_status" -ne 0 ] && [ "$target_status" -eq 0 ]; then
      target_status=$test_status
      target_failure_phase="test"
    fi
  fi
  # Run posttest after the pretest/test attempt even when an earlier step failed.
  if [ "$has_posttest" = 1 ]; then
    posttest_status=0
    GITHUB_ACTIONS=false bun --cwd="$root/$package_root" run posttest || posttest_status=$?
    if [ "$posttest_status" -ne 0 ] && [ "$target_status" -eq 0 ]; then
      target_status=$posttest_status
      target_failure_phase="posttest"
    fi
  fi

  if [ "$test_invoked" -eq 1 ] && [ -s "$report_file" ]; then
    report_status=0
    FAST_SUITE_REPORT="$report_file" FAST_SUITE_TARGET="$target" bun -e '
      const report = await Bun.file(process.env.FAST_SUITE_REPORT).text()
      const target = process.env.FAST_SUITE_TARGET
      const suites = Array.from(report.matchAll(/<testsuite\b([^>]*)>/g), ([, text]) => {
        const attributes = Object.fromEntries(
          Array.from(text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g), (match) => [match[1], match[2]]),
        )
        return { file: attributes.file, tests: Number(attributes.tests), skipped: Number(attributes.skipped ?? 0) }
      })
      const unexpected = Array.from(new Set(suites.map((suite) => suite.file))).filter((file) => file !== target)
      if (unexpected.length) {
        console.error("tests/run.sh: Bun executed files outside manifest entry " + target + ": " + unexpected.map((file) => String(file ?? "<unattributed>")).join(", "))
        process.exit(2)
      }
      const active = suites.some(
        (suite) => suite.file === target && Number.isFinite(suite.tests) && suite.tests > suite.skipped,
      )
      if (!active) {
        console.error("tests/run.sh: Bun reported no active test cases for: " + target)
        process.exit(3)
      }
      ' || report_status=$?
    if [ "$report_status" -ne 0 ]; then
      if [ "$report_status" -eq 3 ]; then
        coverage_invalid=1
      else
        execution_set_invalid=1
      fi
      target_status=1
    fi
  elif [ "$test_invoked" -eq 1 ] && [ "$target_status" -eq 0 ]; then
    coverage_invalid=1
    target_status=1
  elif [ "$target_status" -ne 0 ]; then
    attributed_refusal=1
    if [ -z "$attributed_failure_target" ]; then
      attributed_failure_target=$target
      attributed_failure_phase=$target_failure_phase
    fi
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
if [ "$attributed_refusal" -ne 0 ]; then
  printf 'tests/run.sh: target failed during %s without JUnit attribution: %s\n' "$attributed_failure_phase" "$attributed_failure_target" >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=target-failed-without-junit exit_code=1'
  exit 1
fi
if [ "$execution_set_invalid" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=execution-set-mismatch exit_code=1'
  exit 1
fi
printf 'tests/run.sh: verdict=completed mode=fast target_count=%d exit_code=%d\n' "$target_count" "$fast_status"
exit "$fast_status"
