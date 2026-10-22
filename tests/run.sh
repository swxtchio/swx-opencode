#!/usr/bin/env bash
# Runs the repository's unit suite: the per-package `test` scripts dispatched by
# turbo, exactly as the "Run unit tests" step of .github/workflows/test.yml runs
# them. The root `npm test` / `bun test` refuse on purpose, so this is the
# entrypoint for running everything from the root.
# Fast mode asks Turbo for each selected package's task graph. It prepares the task's
# resolved prerequisites through Turbo, then runs that package's exact selected files
# together through its scoped test task; packages without a task use `bun run test`.
# Turbo/Bun own strict env, local config, script options, and one package lifecycle.
# JUnit-less nonzero exits remain attributed refusals; extra or missing suites are rejected.
# Fast mode needs Linux procfs/ptrace and Python 3 to observe child execs outside the Bun shim.
# Remaining accepted limits (swxtchio/swx-opencode#324): this is not a security boundary;
# unrecognized interpreter identities, in-process test frameworks, and deliberate observer/report
# tampering are outside its observation scope. The full suite remains the release gate.
# In particular, bare Node scripts using node:test without --test remain unobserved.
# The observer records the loaded interpreter and kernel-expanded argv at exec stops.
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
    '--fast groups manifest entries by package and runs the exact selected files through its owning Turbo test task' \
    'when registered, otherwise its package test script.' \
    'The manifest accepts blank lines and whole-line comments beginning with #; every other line must be one' \
    'committed, regular project-relative path matching packages/<package-path>/test/<path>.test.ts.' \
    'Manifest paths use / separators and cannot contain whitespace, backslashes, glob characters, inline comments,' \
    'absolute paths, or . / .. path components.' \
    'The target and owning package must exist; scripts.test must be a string test command with no static file selectors.' \
    'Fast mode passes only the exact manifest files to one package invocation; the owning runner retains all script' \
    'options, script environment assignments, strict Turbo env, package-local Turbo config, and declared preparation.' \
    'Only build tasks are admitted as preparation outside JUnit validation; other prerequisite tasks are refused.' \
    'A package-local turbo.json without a resolved owning test task is refused rather than bypassed by direct execution.' \
    'Bun pretest/test/posttest lifecycle runs once per package: pretest failure stops the test, and posttest runs only' \
    'after a successful test command; hook failures propagate through the task result.' \
    'All captured Bun JUnit reports, including preparation and lifecycle-hook launches, are collected; their observed' \
    'file set must equal the manifest exactly for PATH-resolved Bun test launches; see swxtchio/swx-opencode#324.' \
    'Fast mode requires Python 3 and working Linux procfs/ptrace; unobserved Bun tests and Node --test invalidate the run.' \
    'The loaded interpreter identity and kernel-expanded argv are captured while each process is stopped at exec,' \
    'before user code runs. Failed or incomplete observation is refused; later script/cwd cleanup cannot change these records.' \
    'bun run --bun delegation is refused. This is a merge-gate speedup, not a security boundary; full suite gates releases.' \
    'A nonzero target with no JUnit report is refused with target attribution, not labeled an execution-set mismatch.' \
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
real_bun="$(command -v bun)"
if ! command -v python3 >/dev/null 2>&1 \
  || ! python3 "$root/tests/observe-exec.py" "$config_dir/probe.execs" \
    "$real_bun" -p 'process.execPath' > "$config_dir/runtime-bun"; then
  printf '%s\n' 'tests/run.sh: fast mode requires Python 3 and a working Linux procfs/ptrace process-launch observer' >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=process-observer-unavailable exit_code=1'
  exit 1
fi
if ! FAST_SUITE_WRAPPER_PATH="$config_dir/bun" FAST_SUITE_CAPTURE_REAL_BUN="$real_bun" \
  FAST_SUITE_REPORT_DIR="$config_dir" FAST_SUITE_MAIN_REPORT="$report_file" bun -e '
  const wrapper = `#!/usr/bin/env bash
set -euo pipefail
real_bun=${JSON.stringify(process.env.FAST_SUITE_CAPTURE_REAL_BUN)}
report_dir=${JSON.stringify(process.env.FAST_SUITE_REPORT_DIR)}
main_report=${JSON.stringify(process.env.FAST_SUITE_MAIN_REPORT)}
args=("$@")
wrapper_name=\${BASH_SOURCE[0]##*/}
command_index=0
delegated=0
while [ "$command_index" -lt "\${#args[@]}" ]; do
  case "\${args[command_index]}" in --bun|-b) delegated=1 ;; esac
  case "\${args[command_index]}" in
    --cwd=*) command_index=$((command_index + 1)) ;;
    --cwd|--config|--env-file|--preload|--require|--import|--conditions|--define|--loader|-r|-p|-e|-d|-l) command_index=$((command_index + 2)) ;;
    --*=*) command_index=$((command_index + 1)) ;;
    -*) command_index=$((command_index + 1)) ;;
    *) break ;;
  esac
done
if [ "\${args[command_index]:-}" = run ]; then
  option_index=$((command_index + 1))
  while [ "$option_index" -lt "\${#args[@]}" ]; do
    case "\${args[option_index]}" in --bun|-b) delegated=1 ;; esac
    case "\${args[option_index]}" in
      --) break ;;
      --cwd|--config|--env-file|--preload|--require|--import|--conditions|--define|--loader|-r|-p|-e|-d|-l) option_index=$((option_index + 2)) ;;
      -*) option_index=$((option_index + 1)) ;;
      *) break ;;
    esac
  done
fi
if [ "$wrapper_name" != bunx ] && [ "\${args[command_index]:-}" != x ] && [ "$delegated" -eq 1 ]; then
  printf "%s\\n" "tests/run.sh: refusing unobservable Bun delegation" >&2
  touch "$report_dir/refused-delegation"
  exit 126
fi
for argument in "\${args[@]}"; do
  if [ "$argument" = -- ]; then break; fi
  if [ "$argument" = --test-worker ]; then exec "$real_bun" "\${args[@]}"; fi
done
command="\${args[command_index]:-}"
if [ "$wrapper_name" = bunx ]; then
  case "$command" in vitest|jest|playwright|mocha|ava|pytest|cypress) printf "%s\\n" "tests/run.sh: refusing an unobservable Bunx test runner: $command" >&2; exit 126 ;; esac
  exec "$real_bun" x "\${args[@]}"
fi
if [ "$command" != test ]; then exec "$real_bun" "\${args[@]}"; fi
report_outfile=
index=$((command_index + 1))
while [ "$index" -lt "\${#args[@]}" ]; do
  case "\${args[index]}" in
    --reporter-outfile=*) report_outfile=\${args[index]#*=} ;;
    --reporter-outfile) index=$((index + 1)); report_outfile=\${args[index]:-} ;;
  esac
  index=$((index + 1))
done
if [ "$report_outfile" != "$main_report" ]; then
  report_outfile=$(mktemp "$report_dir/report.XXXXXX")
  printf "%s\\n" "$PWD" > "$report_outfile.cwd"
fi
forwarded=()
index=0
while [ "$index" -lt "\${#args[@]}" ]; do
  argument=\${args[index]}
  if [ "$index" -gt "$command_index" ]; then
    case "$argument" in
      --reporter-outfile|--reporter) index=$((index + 2)); continue ;;
      --reporter-outfile=*|--reporter=*|--dots) index=$((index + 1)); continue ;;
    esac
  fi
  forwarded+=("$argument")
  index=$((index + 1))
done
exec_args=("\${forwarded[@]:0:$((command_index + 1))}" --reporter=junit "--reporter-outfile=$report_outfile" "\${forwarded[@]:$((command_index + 1))}")
status=0
"$real_bun" "\${exec_args[@]}" || status=$?
if [ ! -s "$report_outfile" ]; then
  printf "%s\\n" "tests/run.sh: Bun test invocation produced no JUnit report: $report_outfile" >&2
  if [ "$status" -eq 0 ]; then status=125; fi
fi
exit "$status"
`
  await Bun.write(process.env.FAST_SUITE_WRAPPER_PATH, wrapper)
'; then
  printf '%s\n' 'tests/run.sh: could not create the JUnit process observer' >&2
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=junit-observer-unavailable exit_code=1'
  exit 1
fi
chmod 700 "$config_dir/bun"
ln -s bun "$config_dir/bunx"
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
    if ! FAST_SUITE_ROOT="$root" FAST_SUITE_PACKAGE_ROOT="$package_root" \
      FAST_SUITE_PACKAGE_JSON="$root/$package_root/package.json" bun -e '
      const pkg = await Bun.file(process.env.FAST_SUITE_PACKAGE_JSON).json()
      const fail = (message) => {
        console.error("tests/run.sh: " + message)
        process.exit(1)
      }
      if (typeof pkg.scripts?.test !== "string") fail("scripts.test must be a string")
      if (typeof pkg.name !== "string") fail("package name must be a string")
      const rootTurbo = await Bun.file(process.env.FAST_SUITE_ROOT + "/turbo.json").json()
      if (typeof rootTurbo.tasks !== "object" || rootTurbo.tasks === null || Array.isArray(rootTurbo.tasks)) {
        fail("root turbo.json tasks must be an object")
      }
      const hasRootTurboTestTask = rootTurbo.tasks[pkg.name + "#test"] !== undefined || rootTurbo.tasks.test !== undefined
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
      const assignment = /^[A-Za-z_][A-Za-z0-9_]*=/
      let command = 0
      while (command < words.length && assignment.test(words[command])) {
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
      const selectors = []
      for (let index = command + 2; index < words.length; index += 1) {
        const argument = words[index]
        if (argument === "--") fail("scripts.test must not forward arguments after --")
        if (!argument.startsWith("-")) {
          selectors.push(argument)
          continue
        }
        const equals = argument.indexOf("=")
        const option = equals === -1 ? argument : argument.slice(0, equals)
        if (option === "--reporter-outfile" || option === "--dots") {
          fail("scripts.test reporter options conflict with fast mode JUnit verification")
        }
        if (option === "--reporter") {
          const reporter = equals === -1 ? words[index + 1] : argument.slice(equals + 1)
          if (reporter !== "junit") fail("scripts.test reporter must be junit in fast mode")
          if (equals === -1) {
            index += 1
          }
          continue
        }
        if (booleanOptions.has(option)) continue
        if (valueOptions.has(option)) {
          const value = equals === -1 ? words[index + 1] : argument.slice(equals + 1)
          if (value === undefined || value === "") fail("scripts.test option " + option + " requires a value")
          if (equals === -1) index += 1
          continue
        }
        if (optionalValueOptions.has(option)) {
          if (equals === -1 && /^\d+$/.test(words[index + 1] ?? "")) index += 1
          continue
        }
        fail("unsupported scripts.test Bun option: " + option)
      }
      if (selectors.length > 0) {
        fail("scripts.test contains file selectors; fast mode requires the manifest to define the exact file set")
      }
      const localTurboConfig =
        process.env.FAST_SUITE_ROOT + "/" + process.env.FAST_SUITE_PACKAGE_ROOT + "/turbo.json"
      const hasLocalTurboConfig = await Bun.file(localTurboConfig).exists()
      const dryRun = Bun.spawnSync(
        ["bun", "turbo", "run", "test", "--filter=./" + process.env.FAST_SUITE_PACKAGE_ROOT, "--dry=json"],
        { cwd: process.env.FAST_SUITE_ROOT, stdout: "pipe", stderr: "pipe" },
      )
      if (dryRun.exitCode !== 0) {
        fail(
          "could not resolve the owning Turbo test task" +
            (hasLocalTurboConfig ? " with package-local turbo.json" : "") +
            ": " +
            new TextDecoder().decode(dryRun.stderr),
        )
      }
      const plan = JSON.parse(new TextDecoder().decode(dryRun.stdout))
      if (!Array.isArray(plan.tasks)) fail("Turbo dry-run did not provide a task plan")
      const targetTask = plan.tasks.find(
        (candidate) =>
          candidate.directory === process.env.FAST_SUITE_PACKAGE_ROOT &&
          typeof candidate.taskId === "string" &&
          candidate.taskId.endsWith("#test"),
      )
      if (!targetTask && hasLocalTurboConfig) {
        fail("package-local turbo.json exists but Turbo did not resolve its test task; refusing direct fallback")
      }
      if (!targetTask && plan.tasks.length === 0 && hasRootTurboTestTask) {
        fail("Turbo returned an empty plan for a root-declared test task; refusing direct fallback")
      }
      if (!targetTask && plan.tasks.length > 0) {
        fail("Turbo dry-run did not resolve the owning test task for " + process.env.FAST_SUITE_PACKAGE_ROOT)
      }
      const preparation = []
      if (targetTask) {
        if (!Array.isArray(targetTask.dependencies) || targetTask.dependencies.some((dependency) => typeof dependency !== "string")) {
          fail("Turbo dry-run dependencies must be task ids")
        }
        const taskById = new Map(plan.tasks.map((candidate) => [candidate.taskId, candidate]))
        const pending = [...targetTask.dependencies].reverse().map((taskId) => ({ taskId, collect: true }))
        const visitedForCollection = new Set()
        const visitedForAudit = new Set()
        while (pending.length > 0) {
          const current = pending.pop()
          const visited = current.collect ? visitedForCollection : visitedForAudit
          if (visited.has(current.taskId)) continue
          visited.add(current.taskId)
          const dependencyID = current.taskId
          const dependency = taskById.get(dependencyID)
          if (!dependency) fail("Turbo dry-run omitted dependency task " + dependencyID)
          if (typeof dependency.taskId !== "string") fail("Turbo dependency has an invalid task id: " + dependencyID)
          const separator = dependency.taskId.lastIndexOf("#")
          if (separator < 1) fail("Turbo dependency has an invalid task id: " + dependencyID)
          const taskName = dependency.taskId.slice(separator + 1)
          if (typeof dependency.command !== "string") fail("Turbo dependency has no resolved command: " + dependencyID)
          const commandless = dependency.command === "<NONEXISTENT>"
          if (
            /test/i.test(taskName) ||
            /\b(?:test|vitest|jest|playwright|mocha|ava|pytest|cypress)\b/i.test(dependency.command) ||
            (!commandless && taskName !== "build")
          ) {
            fail("Turbo prerequisite may execute tests outside the JUnit guard or is not an admitted build task: " + dependencyID)
          }
          if (!Array.isArray(dependency.dependencies) || dependency.dependencies.some((child) => typeof child !== "string")) {
            fail("Turbo dependency has no valid child task list: " + dependencyID)
          }
          if (commandless) {
            pending.push(...dependency.dependencies.slice().reverse().map((taskId) => ({ taskId, collect: current.collect })))
            continue
          }
          if (
            typeof dependency.directory !== "string" ||
            dependency.directory.startsWith("/") ||
            dependency.directory.split("/").includes("..")
          ) {
            fail("Turbo dependency has an unsupported workspace directory: " + dependencyID)
          }
          if (taskName.includes("|") || dependency.directory.includes("|")) {
            fail("Turbo dependency contains a reserved field delimiter: " + dependencyID)
          }
          if (current.collect) preparation.push(taskName + "|" + dependency.directory)
          pending.push(...dependency.dependencies.slice().reverse().map((taskId) => ({ taskId, collect: false })))
        }
      }
      const prepared = Array.from(new Set(preparation))
      const fields = [targetTask ? "1" : "0", String(prepared.length), ...prepared]
      if (fields.some((field) => /[\u0000\r\n]/.test(field))) fail("Turbo task fields cannot contain newline characters")
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

# Prepare the union of selected test tasks' resolved prerequisites once. Turbo still
# owns each preparation task's environment, local config, and dependency ordering.
declare -a turbo_prep_requests=()
seen_turbo_prep_requests=$'\n'
index=0
while [ "$index" -lt "$target_count" ]; do
  package_config=()
  while IFS= read -r field; do package_config+=("$field"); done < "${package_config_paths[index]}"
  turbo_prep_count=${package_config[1]}
  turbo_prep_index=2
  turbo_prep_offset=0
  while [ "$turbo_prep_offset" -lt "$turbo_prep_count" ]; do
    request=${package_config[turbo_prep_index]}
    case "$seen_turbo_prep_requests" in
      *$'\n'"$request"$'\n'*) ;;
      *)
        seen_turbo_prep_requests+="$request"$'\n'
        turbo_prep_requests+=("$request")
        ;;
    esac
    turbo_prep_index=$((turbo_prep_index + 1))
    turbo_prep_offset=$((turbo_prep_offset + 1))
  done
  index=$((index + 1))
done

declare -a turbo_prep_tasks=()
seen_turbo_prep_tasks=$'\n'
for request in "${turbo_prep_requests[@]}"; do
  prep_task=${request%%|*}
  case "$seen_turbo_prep_tasks" in
    *$'\n'"$prep_task"$'\n'*) ;;
    *)
      seen_turbo_prep_tasks+="$prep_task"$'\n'
      turbo_prep_tasks+=("$prep_task")
      ;;
  esac
done
for prep_task in "${turbo_prep_tasks[@]}"; do
  declare -a prep_filters=()
  seen_prep_filters=$'\n'
  for request in "${turbo_prep_requests[@]}"; do
    if [ "${request%%|*}" != "$prep_task" ]; then continue; fi
    filter="--filter=./${request#*|}"
    case "$seen_prep_filters" in
      *$'\n'"$filter"$'\n'*) ;;
      *)
        seen_prep_filters+="$filter"$'\n'
        prep_filters+=("$filter")
        ;;
    esac
  done
  preparation_status=0
  GITHUB_ACTIONS=false PATH="$config_dir:$PATH" python3 "$root/tests/observe-exec.py" \
    "$config_dir/preparation-$prep_task.execs" "$config_dir/bun" turbo run "$prep_task" "${prep_filters[@]}" --force || preparation_status=$?
  if [ "$preparation_status" -ne 0 ]; then
    printf 'tests/run.sh: Turbo-declared preparation task failed: %s\n' "$prep_task" >&2
    printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=turbo-preparation-failed exit_code=1'
    exit 1
  fi
done

# Keep each owning package's selected files in one task invocation so package-level
# preparation and the Bun pretest/test/posttest lifecycle match its scoped Turbo run.
fast_status=0
coverage_invalid=0
execution_set_invalid=0
attributed_refusal=0
attributed_failure_target=
attributed_failure_phase=
seen_run_packages=$'\n'
index=0
while [ "$index" -lt "$target_count" ]; do
  package_root=${package_paths[index]}
  case "$seen_run_packages" in
    *$'\n'"$package_root"$'\n'*)
      index=$((index + 1))
      continue
      ;;
  esac
  seen_run_packages+="$package_root"$'\n'
  package_config=()
  while IFS= read -r field; do package_config+=("$field"); done < "${package_config_paths[index]}"
  has_turbo_task=${package_config[0]}

  selected_targets=()
  selected_paths=()
  candidate_index=0
  while [ "$candidate_index" -lt "$target_count" ]; do
    if [ "${package_paths[candidate_index]}" = "$package_root" ]; then
      selected_targets+=("${test_paths[candidate_index]}")
      selected_paths+=("./${test_paths[candidate_index]}")
    fi
    candidate_index=$((candidate_index + 1))
  done
  target_label="${selected_targets[*]}"

  target_status=0
  target_failure_phase="package-test"
  report_args=(--reporter=junit --reporter-outfile="$report_file")
  test_status=0
  if [ "$has_turbo_task" = 1 ]; then
    GITHUB_ACTIONS=false PATH="$config_dir:$PATH" python3 "$root/tests/observe-exec.py" \
      "$config_dir/package-$index.execs" "$config_dir/bun" turbo run test "--filter=./$package_root" --only --force -- \
      "${report_args[@]}" "${selected_paths[@]}" || test_status=$?
  else
    GITHUB_ACTIONS=false PATH="$config_dir:$PATH" python3 "$root/tests/observe-exec.py" \
      "$config_dir/package-$index.execs" "$config_dir/bun" --cwd="$root/$package_root" run test -- \
      "${report_args[@]}" "${selected_paths[@]}" || test_status=$?
  fi
  if [ "$test_status" -ne 0 ]; then target_status=$test_status; fi

  if [ -s "$report_file" ]; then
    report_status=0
    printf -v selected_target_lines '%s\n' "${selected_targets[@]}"
    FAST_SUITE_REPORT="$report_file" FAST_SUITE_TARGETS="$selected_target_lines" \
      FAST_SUITE_PACKAGE_ROOT="$package_root" FAST_SUITE_OBSERVED_FILE="$config_dir/observed.$index" bun -e '
      const report = await Bun.file(process.env.FAST_SUITE_REPORT).text()
      const targets = process.env.FAST_SUITE_TARGETS.split("\n").filter(Boolean)
      const expected = new Set(targets)
      const suites = Array.from(report.matchAll(/<testsuite\b([^>]*)>/g), ([, text]) => {
        const attributes = Object.fromEntries(
          Array.from(text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g), (match) => [match[1], match[2]]),
        )
        return { file: attributes.file, tests: Number(attributes.tests), skipped: Number(attributes.skipped ?? 0) }
      })
      const actual = new Set(suites.map((suite) => suite.file))
      const unexpected = Array.from(actual).filter((file) => !expected.has(file))
      const missing = targets.filter((target) => !actual.has(target))
      if (unexpected.length || missing.length) {
        if (unexpected.length) {
          console.error("tests/run.sh: Bun executed files outside the selected manifest entries: " + unexpected.map((file) => String(file ?? "<unattributed>")).join(", "))
        }
        if (missing.length) console.error("tests/run.sh: Bun did not report selected manifest entries: " + missing.join(", "))
        process.exit(2)
      }
      const inactive = targets.filter(
        (target) => !suites.some((suite) => suite.file === target && Number.isFinite(suite.tests) && suite.tests > suite.skipped),
      )
      if (inactive.length) {
        console.error("tests/run.sh: Bun reported no active test cases for: " + inactive.join(", "))
        process.exit(3)
      }
      await Bun.write(
        process.env.FAST_SUITE_OBSERVED_FILE,
        JSON.stringify(Array.from(actual, (file) => process.env.FAST_SUITE_PACKAGE_ROOT + "/" + file)),
      )
      ' || report_status=$?
    if [ "$report_status" -ne 0 ]; then
      if [ "$report_status" -eq 3 ]; then
        coverage_invalid=1
      else
        execution_set_invalid=1
      fi
      target_status=1
    fi
  elif [ "$target_status" -eq 0 ]; then
    coverage_invalid=1
    target_status=1
  elif [ "$target_status" -ne 0 ]; then
    attributed_refusal=1
    if [ -z "$attributed_failure_target" ]; then
      attributed_failure_target=$target_label
      attributed_failure_phase=$target_failure_phase
    fi
  fi
  if [ "$target_status" -ne 0 ] && [ -s "$report_file" ]; then
    printf 'tests/run.sh: JUnit report for %s:\n' "$target_label" >&2
    while IFS= read -r report_line || [ -n "$report_line" ]; do
      printf '%s\n' "$report_line" >&2
    done < "$report_file"
  fi
  rm -f -- "$report_file"
  if [ "$target_status" -ne 0 ]; then fast_status=1; fi
  index=$((index + 1))
done

# A process outside the PATH shim cannot certify a fast run by omitting its report.
launch_observation_status=0
FAST_SUITE_ROOT="$root" FAST_SUITE_CAPTURE_REAL_BUN="$real_bun" FAST_SUITE_REPORT_DIR="$config_dir" FAST_SUITE_MAIN_REPORT="$report_file" bun -e '
  import { readdir } from "node:fs/promises"
  import { basename, join } from "node:path"

  const directory = process.env.FAST_SUITE_REPORT_DIR
  const runtimeBun = (await Bun.file(join(directory, "runtime-bun")).text()).trim()
  const probe = (await Bun.file(join(directory, "probe.execs")).text()).trim().split("\n").map((line) => JSON.parse(line))
  const runtime = probe.find((entry) => entry.event === "exec" && entry.executable === runtimeBun)
  if (!runtime || probe.at(-1)?.event !== "complete") {
    console.error("tests/run.sh: unavailable runtime executable identity")
    process.exit(2)
  }
  const entries = await readdir(directory)
  let invalid = entries.includes("refused-delegation")
  for (const name of entries.filter((entry) => entry.endsWith(".execs") && entry !== "probe.execs")) {
    const records = (await Bun.file(join(directory, name)).text()).trim().split("\n").map((line) => JSON.parse(line))
    const launches = records.filter((entry) => entry.event === "exec")
    const completion = records.at(-1)
    if (completion?.event !== "complete" || completion.execCount !== launches.length || !launches.length || records.length !== launches.length + 1) {
      console.error("tests/run.sh: incomplete process-launch observation: " + name)
      invalid = true
      continue
    }
    for (const entry of launches) {
      const executable = entry.executable
      const args = entry.argv.slice(1)
      if (basename(executable ?? "") === "node" && args.some((argument) => argument === "--test" || argument.startsWith("--test="))) {
        console.error("tests/run.sh: unobservable Node test launch: " + executable)
        invalid = true
      }
      const configuredBun = entry.device === runtime.device && entry.inode === runtime.inode
      const namedBun = executable.startsWith("/") && basename(executable) === "bun"
      if (!configuredBun && !namedBun) continue
      let command = 0
      while (command < args.length && args[command].startsWith("-")) {
        const argument = args[command++]
        if (["--cwd", "--config", "--env-file", "--preload", "--require", "--import", "--conditions", "--define", "--loader", "-r", "-p", "-e", "-d", "-l"].includes(argument)) command++
      }
      if (args[command] !== "test" || args.includes("--test-worker")) continue
      const outfileIndex = args.indexOf("--reporter-outfile")
      const outfile = args.find((argument) => argument.startsWith("--reporter-outfile="))?.slice("--reporter-outfile=".length) ?? (outfileIndex < 0 ? undefined : args[outfileIndex + 1])
      const captured = outfile === process.env.FAST_SUITE_MAIN_REPORT || entries.some((entry) => entry.startsWith("report.") && !entry.endsWith(".cwd") && outfile === join(directory, entry))
      if (!captured || !args.includes("--reporter=junit")) {
        console.error("tests/run.sh: unobserved Bun test launch: " + executable)
        invalid = true
      }
    }
  }
  if (invalid) process.exit(2)
' || launch_observation_status=$?
# Per-package JUnit checks record their actual files; the Bun shim records any other
# test processes launched by preparation or lifecycle scripts. Enforce their union.
declare -a manifest_paths=()
index=0
while [ "$index" -lt "$target_count" ]; do
  manifest_paths+=("${package_paths[index]}/${test_paths[index]}")
  index=$((index + 1))
done
printf -v manifest_target_lines '%s\n' "${manifest_paths[@]}"
observation_status=0
FAST_SUITE_REPORT_DIR="$config_dir" FAST_SUITE_ROOT="$root" FAST_SUITE_MANIFEST_PATHS="$manifest_target_lines" bun -e '
  import { readdir } from "node:fs/promises"
  import { join, relative, resolve, sep } from "node:path"

  const reportDirectory = process.env.FAST_SUITE_REPORT_DIR
  const root = process.env.FAST_SUITE_ROOT
  const expected = new Set(process.env.FAST_SUITE_MANIFEST_PATHS.split("\n").filter(Boolean))
  const observed = new Set()
  const entries = await readdir(reportDirectory)
  for (const name of entries.filter((entry) => entry.startsWith("observed."))) {
    const paths = await Bun.file(join(reportDirectory, name)).json()
    if (!Array.isArray(paths) || paths.some((file) => typeof file !== "string")) {
      console.error("tests/run.sh: invalid observed-file receipt: " + name)
      process.exit(2)
    }
    for (const file of paths) observed.add(file)
  }
  for (const name of entries.filter((entry) => entry.startsWith("report.") && !entry.endsWith(".cwd"))) {
    const report = await Bun.file(join(reportDirectory, name)).text()
    if (!report.trim()) {
      console.error("tests/run.sh: a captured Bun test launch produced an empty JUnit report: " + name)
      process.exit(2)
    }
    const cwdFile = join(reportDirectory, name + ".cwd")
    if (!(await Bun.file(cwdFile).exists())) {
      console.error("tests/run.sh: JUnit report has no execution working directory: " + name)
      process.exit(2)
    }
    const cwd = (await Bun.file(cwdFile).text()).trim()
    const suites = Array.from(report.matchAll(/<testsuite\b([^>]*)>/g), ([, text]) => {
      const attributes = Object.fromEntries(
        Array.from(text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g), (match) => [match[1], match[2]]),
      )
      return attributes.file
    })
    if (suites.length === 0) {
      console.error("tests/run.sh: JUnit report contains no attributed test suites: " + name)
      process.exit(2)
    }
    for (const file of suites) {
      if (typeof file !== "string" || file.length === 0) {
        console.error("tests/run.sh: JUnit report contains an unattributed test suite: " + name)
        process.exit(2)
      }
      observed.add(relative(root, resolve(cwd, file)).split(sep).join("/"))
    }
  }
  const unexpected = Array.from(observed).filter((file) => !expected.has(file))
  const missing = Array.from(expected).filter((file) => !observed.has(file))
  if (unexpected.length || missing.length) {
    if (unexpected.length) {
      console.error("tests/run.sh: JUnit-observed files outside the manifest: " + unexpected.join(", "))
    }
    if (missing.length) console.error("tests/run.sh: manifest files absent from all JUnit reports: " + missing.join(", "))
    process.exit(2)
  }
' || observation_status=$?
if [ "$observation_status" -ne 0 ]; then execution_set_invalid=1; fi

if [ "$coverage_invalid" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=no-test-cases exit_code=1'
  exit 1
fi
if [ "$launch_observation_status" -ne 0 ]; then
  printf '%s\n' 'tests/run.sh: verdict=refused mode=fast reason=unobservable-test-launch exit_code=1'
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
