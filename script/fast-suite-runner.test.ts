import { afterAll, expect, test } from "bun:test"
import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"

const scratch = mkdtempSync(join(tmpdir(), "fast-suite-runner-"))
afterAll(() => rmSync(scratch, { recursive: true, force: true }))

// GOAL: prove fast mode honors owning-package test options and Turbo preparation while running
// exact targets; refusals preserve attributed failures and reject unexpected JUnit files.
test(
  "tests/run.sh preserves package preparation and test options for exact fast targets",
  () => {
    const root = join(scratch, "repo")
    const marker = join(scratch, "executed.txt")
    const packagePreloadLog = join(scratch, "package-preloads.txt")
    const turboLog = join(scratch, "turbo-argv.txt")
    const directPreloadLog = join(scratch, "direct-preloads.txt")
    const hookLog = join(scratch, "hooks.txt")
    const buildLog = join(scratch, "dependency-builds.txt")
    const bunArgsLog = join(scratch, "bun-test-argv.txt")
    const runner = join(root, "tests", "run.sh")
    const manifest = join(root, "tests", "fast-suite.txt")
    const packageRoot = join(root, "packages", "example")
    const packageTests = join(packageRoot, "test")
    const dependencyRoot = join(root, "packages", "dependency")
    const directPackageRoot = join(root, "packages", "direct")
    const directPackageTests = join(directPackageRoot, "test")
    const scriptlessPackageRoot = join(root, "packages", "no-script")
    const scriptlessPackageTests = join(scriptlessPackageRoot, "test")
    const turbo = join(root, "node_modules", ".bin", "turbo")
    const bunBin = join(root, "bin", "bun")
    const packagePath = "packages/example"
    const selectedPath = `${packagePath}/test/selected.test.ts`
    const selectedSecondPath = `${packagePath}/test/selected-second.test.ts`
    const timeoutPath = `${packagePath}/test/timeout.test.ts`
    const literalGlobPath = `${packagePath}/test/literal*.test.ts`
    const whitespacePath = `${packagePath}/test/with space.test.ts`
    const shapeInvalidPath = `${packagePath}/test/selected.test.tsx`
    const directPath = "packages/direct/test/direct.test.ts"
    const directSecondPath = "packages/direct/test/direct-second.test.ts"
    const scriptlessPath = "packages/no-script/test/no-script.test.ts"
    const validManifest = `# only the declared test files run

${selectedPath}
${selectedSecondPath}
${directPath}
${directSecondPath}
`
    const gitEnv = {
      ...process.env,
      GIT_AUTHOR_NAME: "Runner fixture",
      GIT_AUTHOR_EMAIL: "runner@example.invalid",
      GIT_COMMITTER_NAME: "Runner fixture",
      GIT_COMMITTER_EMAIL: "runner@example.invalid",
    }
    const runOptions = {
      cwd: root,
      env: {
        ...process.env,
        FAST_SUITE_FIXTURE_ROOT: root,
        FAST_SUITE_MARKER: marker,
        FAST_SUITE_PACKAGE_PRELOAD_LOG: packagePreloadLog,
        FAST_SUITE_DIRECT_PRELOAD_LOG: directPreloadLog,
        FAST_SUITE_HOOK_LOG: hookLog,
        FAST_SUITE_BUILD_LOG: buildLog,
        FAST_SUITE_BUN_LOG: bunArgsLog,
        FAST_TURBO_LOG: turboLog,
        FAST_SUITE_REAL_BUN: process.execPath,
        FAST_SUITE_FAIL_PRETEST: "0",
        FAST_SUITE_FAIL_POSTTEST: "0",
        PATH: `${join(root, "bin")}:${process.env.PATH}`,
      },
      stdout: "pipe" as const,
      stderr: "pipe" as const,
    }

    mkdirSync(packageTests, { recursive: true })
    mkdirSync(directPackageTests, { recursive: true })
    mkdirSync(scriptlessPackageTests, { recursive: true })
    mkdirSync(dependencyRoot, { recursive: true })
    mkdirSync(dirname(runner), { recursive: true })
    mkdirSync(dirname(turbo), { recursive: true })
    mkdirSync(dirname(bunBin), { recursive: true })
    copyFileSync(join(import.meta.dir, "..", "tests", "run.sh"), runner)
    chmodSync(runner, 0o755)
    writeFileSync(
      bunBin,
      `#!/usr/bin/env bash
set -euo pipefail
has_test=0
for argument in "$@"; do
  if [ "$argument" = test ]; then has_test=1; fi
done
if [ "$has_test" -eq 1 ]; then
  printf '%s\\n' "$@" >> "\${FAST_SUITE_BUN_LOG:?}"
fi
if [ "\${FAST_SUITE_FORCE_EXTRA_FILE:-0}" = 1 ] && [ "$has_test" -eq 1 ]; then
  exec "\${FAST_SUITE_REAL_BUN:?}" "$@" ./test/unlisted.test.ts
fi
exec "\${FAST_SUITE_REAL_BUN:?}" "$@"
`,
    )
    chmodSync(bunBin, 0o755)
    writeFileSync(
      join(root, "package.json"),
      JSON.stringify({
        name: "fast-suite-fixture",
        private: true,
        packageManager: "bun@1.4.2",
        workspaces: { packages: ["packages/*"] },
      }),
    )
    writeFileSync(join(root, "bunfig.toml"), '[test]\nroot = "./do-not-run-tests-from-root"\n')
    writeFileSync(join(root, ".gitignore"), "packages/example/test/untracked.test.ts\n")
    writeFileSync(
      join(root, "turbo.json"),
      JSON.stringify({
        tasks: {
          build: { outputs: [] },
          "@fixture/example#test": { outputs: [], dependsOn: ["^build"] },
        },
      }),
    )
    const exampleTestScript =
      'FAST_SUITE_CONFIG_KIND=package FAST_SUITE_CONFIG_VALUE="example\\suite" FAST_SUITE_FORCE_EXTRA_FILE=0 ' +
      "bun test --parallel=2 --timeout 5000 --only-failures --max-concurrency=1"
    const examplePackage = {
      name: "@fixture/example",
      private: true,
      type: "module",
      dependencies: { "@fixture/dependency": "workspace:*" },
      scripts: {
        pretest: "bun ./pretest.ts",
        test: exampleTestScript,
        posttest: "bun ./posttest.ts",
      },
    }
    writeFileSync(join(packageRoot, "package.json"), JSON.stringify(examplePackage))
    writeFileSync(
      join(dependencyRoot, "package.json"),
      JSON.stringify({
        name: "@fixture/dependency",
        private: true,
        type: "module",
        scripts: { build: "bun ./build.ts" },
      }),
    )
    writeFileSync(
      join(dependencyRoot, "build.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'appendFileSync(process.env.FAST_SUITE_BUILD_LOG!, "dependency-build\\n")',
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageRoot, "package.json"),
      JSON.stringify({
        name: "@fixture/direct",
        private: true,
        type: "module",
        scripts: {
          test: "FAST_SUITE_CONFIG_KIND=direct-package FAST_SUITE_CONFIG_VALUE=direct bun test --timeout 5000 --max-concurrency=1 ./test/direct.test.ts ./test/direct.test.tsx",
        },
      }),
    )
    writeFileSync(
      join(scriptlessPackageRoot, "package.json"),
      JSON.stringify({ name: "@fixture/no-script", private: true, type: "module" }),
    )
    writeFileSync(
      join(scriptlessPackageTests, "no-script.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("has a real package test target despite missing scripts.test", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "no-script\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(join(packageRoot, "bunfig.toml"), '[test]\npreload = ["./test/preload.ts"]\n')
    writeFileSync(join(directPackageRoot, "bunfig.toml"), '[test]\npreload = ["./test/preload.ts"]\n')
    writeFileSync(
      join(packageTests, "preload.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        "process.env.FAST_SUITE_PRELOAD = process.cwd()",
        'appendFileSync(process.env.FAST_SUITE_PACKAGE_PRELOAD_LOG!, "preload\\n")',
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageTests, "preload.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        "process.env.FAST_SUITE_PRELOAD = process.cwd()",
        'appendFileSync(process.env.FAST_SUITE_DIRECT_PRELOAD_LOG!, "preload\\n")',
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageRoot, "pretest.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'appendFileSync(process.env.FAST_SUITE_HOOK_LOG!, "pretest\\n")',
        'if (process.env.FAST_SUITE_FAIL_PRETEST === "1") process.exit(1)',
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageRoot, "posttest.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'appendFileSync(process.env.FAST_SUITE_HOOK_LOG!, "posttest\\n")',
        'if (process.env.FAST_SUITE_FAIL_POSTTEST === "1") process.exit(1)',
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "selected.test.ts"),
      [
        'import { appendFileSync, readFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses package test options and Turbo preparation", () => {',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  expect(process.env.FAST_SUITE_CONFIG_KIND).toBe("package")',
        '  expect(process.env.FAST_SUITE_CONFIG_VALUE).toBe("example\\\\suite")',
        '  expect(readFileSync(process.env.FAST_SUITE_BUILD_LOG!, "utf8")).toContain("dependency-build")',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "selected-configured\\n")',
        '  appendFileSync(process.env.FAST_SUITE_HOOK_LOG!, "selected\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "selected.test.tsx"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is an unlisted sibling whose path extends the selected target", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "selected-sibling\\n")',
        "})",
        "",
      ].join("\n"),
    )
    mkdirSync(join(packageTests, "nested", "test"), { recursive: true })
    writeFileSync(
      join(packageTests, "nested", "test", "selected.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is an unlisted nested path containing the selected target", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "selected-nested\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "selected-second.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses package test options for each selected file", () => {',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  expect(process.env.FAST_SUITE_CONFIG_KIND).toBe("package")',
        '  expect(process.env.FAST_SUITE_CONFIG_VALUE).toBe("example\\\\suite")',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "second-configured\\n")',
        '  appendFileSync(process.env.FAST_SUITE_HOOK_LOG!, "selected-second\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageTests, "direct.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses the direct package test environment and preload", () => {',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  expect(process.env.FAST_SUITE_CONFIG_KIND).toBe("direct-package")',
        '  expect(process.env.FAST_SUITE_CONFIG_VALUE).toBe("direct")',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "direct-configured\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageTests, "direct-second.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses direct package test options and preload for each target", () => {',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  expect(process.env.FAST_SUITE_CONFIG_KIND).toBe("direct-package")',
        '  expect(process.env.FAST_SUITE_CONFIG_VALUE).toBe("direct")',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "direct-second-configured\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageTests, "direct.test.tsx"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is an unlisted direct-package sibling", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "direct-sibling\\n")',
        "})",
        "",
      ].join("\n"),
    )
    mkdirSync(join(directPackageTests, "nested", "test"), { recursive: true })
    writeFileSync(
      join(directPackageTests, "nested", "test", "direct.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is an unlisted direct-package nested path", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "direct-nested\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "unlisted.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is an unlisted suite that a widened Bun invocation can reach", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "unlisted\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "failing.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("a real assertion failure reaches the runner", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "failing\\n")',
        '  expect("red").toBe("green")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "timeout.test.ts"),
      [
        'import { expect, test } from "bun:test"',
        'test("honors the package test timeout", async () => {',
        "  await Bun.sleep(100)",
        '  expect(true).toBe(true)',
        "})",
        "",
      ].join("\n"),
    )
    // This committed target registers only a skipped case; it must not count as active coverage.
    writeFileSync(
      join(packageTests, "empty.test.ts"),
      ['import { test } from "bun:test"', 'test.skip("does not count as active coverage", () => {})', ""].join("\n"),
    )
    writeFileSync(
      join(packageTests, "literal*.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is a real tracked target with a reserved glob character", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "glob\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(packageTests, "with space.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is a real tracked target with whitespace in its path", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "whitespace\\n")',
        "})",
        "",
      ].join("\n"),
    )
    const untrackedTarget = join(packageTests, "untracked.test.ts")
    writeFileSync(
      untrackedTarget,
      [
        'import { appendFileSync } from "node:fs"',
        'import { test } from "bun:test"',
        'test("is ignored and not part of the committed selection", () => {',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "untracked\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      turbo,
      `#!/usr/bin/env bash
set -euo pipefail
root="\${FAST_SUITE_FIXTURE_ROOT:?}"
printf '%s\\n' "$*" >> "\${FAST_TURBO_LOG:?}"
if [ "\${1:-}" = run ] && [ "\${2:-}" = build ]; then
  for argument in "$@"; do
    if [ "$argument" = "--filter=./packages/example^..." ]; then
      (cd "$root/packages/dependency" && bun run build)
    fi
  done
  exit "$?"
fi
package=
read_targets=0
targets=()
for argument in "$@"; do
  if [ "$argument" = "--" ]; then
    read_targets=1
    continue
  fi
  if [ "$read_targets" -eq 1 ]; then
    targets+=("$argument")
    continue
  fi
  case "$argument" in
    --filter=./*) package="\${argument#--filter=./}" ;;
  esac
done
if [ "$1" = test ] && [ -n "$package" ]; then
  (cd "$root/$package" && bun run test -- "\${targets[@]}")
  exit "$?"
fi
if [ "$#" -eq 1 ] && [ "$1" = test ]; then
  (cd "$root/packages/example" && bun run test)
  exit "$?"
fi
exit 97
`,
    )
    chmodSync(turbo, 0o755)
    writeFileSync(manifest, validManifest)
    writeFileSync(marker, "")
    writeFileSync(packagePreloadLog, "")
    writeFileSync(directPreloadLog, "")
    writeFileSync(hookLog, "")
    writeFileSync(buildLog, "")
    writeFileSync(bunArgsLog, "")
    writeFileSync(turboLog, "")

    const init = Bun.spawnSync(["git", "init", "-q", "-b", "swxtch"], { cwd: root, env: gitEnv })
    expect(init.exitCode).toBe(0)
    for (const [key, value] of [
      ["user.name", "Runner fixture"],
      ["user.email", "runner@example.invalid"],
      ["commit.gpgsign", "false"],
    ] as const) {
      const configured = Bun.spawnSync(["git", "config", key, value], { cwd: root, env: gitEnv })
      expect(configured.exitCode).toBe(0)
    }
    expect(Bun.spawnSync(["git", "add", "."], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "runner fixture"], { cwd: root, env: gitEnv }).exitCode).toBe(0)

    const fast = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    if (fast.exitCode !== 0) console.error(fast.stdout.toString(), fast.stderr.toString())
    expect(fast.exitCode).toBe(0)
    expect(fast.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=4 exit_code=0")).toBe(true)
    expect(readFileSync(marker, "utf8").trim().split("\n").sort()).toEqual([
      "direct-configured",
      "direct-second-configured",
      "second-configured",
      "selected-configured",
    ])
    expect(readdirSync(root).filter((name) => name.startsWith(".fast-suite-report."))).toEqual([])
    expect(readFileSync(turboLog, "utf8").trim().split("\n")).toEqual(["run build --filter=./packages/example^..."])
    expect(readFileSync(buildLog, "utf8").trim().split("\n")).toEqual(["dependency-build"])
    expect(readFileSync(packagePreloadLog, "utf8").trim().split("\n")).toEqual(["preload", "preload"])
    expect(readFileSync(directPreloadLog, "utf8").trim().split("\n")).toEqual(["preload", "preload"])
    const bunTestArgs = readFileSync(bunArgsLog, "utf8")
    expect(bunTestArgs).toContain("--parallel=2\n")
    expect(bunTestArgs).toContain("--timeout\n5000\n")
    expect(bunTestArgs).toContain("--only-failures\n")
    expect(bunTestArgs).toContain("--max-concurrency=1\n")
    expect(readFileSync(hookLog, "utf8").trim().split("\n")).toEqual([
      "pretest",
      "selected",
      "posttest",
      "pretest",
      "selected-second",
      "posttest",
    ])

    const unexpectedFileScript = exampleTestScript.replace("FAST_SUITE_FORCE_EXTRA_FILE=0", "FAST_SUITE_FORCE_EXTRA_FILE=1")
    writeFileSync(
      join(packageRoot, "package.json"),
      JSON.stringify({ ...examplePackage, scripts: { ...examplePackage.scripts, test: unexpectedFileScript } }),
    )
    writeFileSync(manifest, `${selectedPath}\n`)
    expect(Bun.spawnSync(["git", "add", "packages/example/package.json", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "trigger an extra Bun test path"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    writeFileSync(marker, "")
    writeFileSync(hookLog, "")
    const unexpectedFile = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    if (unexpectedFile.exitCode === 0) console.error(unexpectedFile.stdout.toString(), unexpectedFile.stderr.toString())
    expect(unexpectedFile.exitCode).toBe(1)
    expect(
      unexpectedFile.stdout.toString().trim().endsWith("verdict=refused mode=fast reason=execution-set-mismatch exit_code=1"),
    ).toBe(true)
    expect(unexpectedFile.stderr.toString()).toContain("Bun executed files outside manifest entry")
    expect(readFileSync(marker, "utf8").trim().split("\n").sort()).toEqual(["selected-configured", "unlisted"])
    expect(readdirSync(root).filter((name) => name.startsWith(".fast-suite-report."))).toEqual([])

    writeFileSync(join(packageRoot, "package.json"), JSON.stringify(examplePackage))
    writeFileSync(manifest, validManifest)
    expect(Bun.spawnSync(["git", "add", "packages/example/package.json", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "restore package scripts and fast targets"], { cwd: root, env: gitEnv }).exitCode).toBe(0)

    const shortTimeoutScript = exampleTestScript.replace("--timeout 5000", "--timeout 20")
    writeFileSync(
      join(packageRoot, "package.json"),
      JSON.stringify({ ...examplePackage, scripts: { ...examplePackage.scripts, test: shortTimeoutScript } }),
    )
    writeFileSync(manifest, `${timeoutPath}\n`)
    expect(Bun.spawnSync(["git", "add", "packages/example/package.json", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "select a target that exceeds the package timeout"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    writeFileSync(marker, "")
    writeFileSync(hookLog, "")
    const shortTimeout = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(shortTimeout.exitCode).toBe(1)
    expect(shortTimeout.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=1 exit_code=1")).toBe(true)
    expect(shortTimeout.stderr.toString().toLowerCase()).toContain("timeout")
    expect(readFileSync(hookLog, "utf8").trim().split("\n")).toEqual(["pretest", "posttest"])
    expect(readdirSync(root).filter((name) => name.startsWith(".fast-suite-report."))).toEqual([])

    writeFileSync(join(packageRoot, "package.json"), JSON.stringify(examplePackage))
    writeFileSync(manifest, validManifest)
    expect(Bun.spawnSync(["git", "add", "packages/example/package.json", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "restore package scripts and fast targets"], { cwd: root, env: gitEnv }).exitCode).toBe(0)

    writeFileSync(manifest, `${selectedPath}\n`)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "select one target for hook failure proofs"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    writeFileSync(marker, "")
    writeFileSync(hookLog, "")
    runOptions.env.FAST_SUITE_FAIL_PRETEST = "1"
    const failedPretest = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(failedPretest.exitCode).toBe(1)
    expect(
      failedPretest.stdout
        .toString()
        .trim()
        .endsWith("verdict=refused mode=fast reason=target-failed-without-junit exit_code=1"),
    ).toBe(true)
    expect(failedPretest.stderr.toString()).toContain("failed during pretest without JUnit attribution")
    expect(readFileSync(marker, "utf8")).toBe("")
    expect(readFileSync(hookLog, "utf8").trim().split("\n")).toEqual(["pretest", "posttest"])

    runOptions.env.FAST_SUITE_FAIL_PRETEST = "0"
    runOptions.env.FAST_SUITE_FAIL_POSTTEST = "1"
    writeFileSync(marker, "")
    writeFileSync(hookLog, "")
    const failedPosttest = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(failedPosttest.exitCode).toBe(1)
    expect(
      failedPosttest.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=1 exit_code=1"),
    ).toBe(true)
    expect(readFileSync(marker, "utf8").trim().split("\n")).toEqual(["selected-configured"])
    expect(readFileSync(hookLog, "utf8").trim().split("\n")).toEqual(["pretest", "selected", "posttest"])

    runOptions.env.FAST_SUITE_FAIL_POSTTEST = "0"
    writeFileSync(manifest, validManifest)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "restore fast targets"], { cwd: root, env: gitEnv }).exitCode).toBe(0)

    writeFileSync(join(root, ".gitignore"), "packages/example/test/untracked.test.ts\ntests/fast-suite.txt\n")
    expect(Bun.spawnSync(["git", "add", ".gitignore"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(
      Bun.spawnSync(["git", "rm", "--cached", "--", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode,
    ).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "hide the manifest from the index"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    const beforeUnavailableManifest = readFileSync(marker, "utf8")
    const unavailableManifest = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(readFileSync(marker, "utf8")).toBe(beforeUnavailableManifest)
    expect(unavailableManifest.exitCode).toBe(1)
    expect(
      unavailableManifest.stdout
        .toString()
        .trim()
        .endsWith("verdict=refused mode=fast reason=fast-manifest-unavailable exit_code=1"),
    ).toBe(true)
    writeFileSync(join(root, ".gitignore"), "packages/example/test/untracked.test.ts\n")
    expect(Bun.spawnSync(["git", "add", ".gitignore", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "restore the tracked manifest"], { cwd: root, env: gitEnv }).exitCode).toBe(0)

    const beforeDirty = readFileSync(marker, "utf8")
    writeFileSync(manifest, `${validManifest}# pending change\n`)
    const help = Bun.spawnSync(["bash", runner, "--help"], runOptions)
    expect(help.exitCode).toBe(0)
    expect(help.stdout.toString().trim().endsWith("verdict=completed mode=help exit_code=0")).toBe(true)
    expect(readFileSync(marker, "utf8")).toBe(beforeDirty)

    const dirtyFast = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(dirtyFast.exitCode).toBe(1)
    expect(
      dirtyFast.stdout.toString().trim().endsWith("verdict=refused mode=fast reason=dirty-checkout exit_code=1"),
    ).toBe(true)
    const dirtyFull = Bun.spawnSync(["bash", runner], runOptions)
    expect(dirtyFull.exitCode).toBe(1)
    expect(dirtyFull.stdout.toString().trim().endsWith("verdict=completed mode=full exit_code=1")).toBe(true)
    const fullRunsWhileDirty = readFileSync(marker, "utf8").slice(beforeDirty.length).trim().split("\n")
    expect(fullRunsWhileDirty).toContain("failing")
    expect(fullRunsWhileDirty).toContain("unlisted")
    writeFileSync(manifest, validManifest)

    for (const [selection, reason] of [
      ["", "empty-fast-manifest"],
      [`${packagePath}/test/missing.test.ts\n`, "invalid-fast-manifest"],
      [`${packagePath}/test/untracked.test.ts\n`, "invalid-fast-manifest"],
      [`${selectedPath}\n${selectedPath}\n`, "invalid-fast-manifest"],
      [`${shapeInvalidPath}\n`, "invalid-fast-manifest"],
      [`${literalGlobPath}\n`, "invalid-fast-manifest"],
      [`${whitespacePath}\n`, "invalid-fast-manifest"],
      [`${scriptlessPath}\n`, "invalid-fast-manifest"],
      [`${packagePath}/test/../test/selected.test.ts\n`, "invalid-fast-manifest"],
      [`${selectedPath} # inline comment\n`, "invalid-fast-manifest"],
    ] as const) {
      writeFileSync(manifest, selection)
      expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
      const invalidCommit = Bun.spawnSync(["git", "commit", "-qm", "change runner manifest"], {
        cwd: root,
        env: gitEnv,
      })
      expect(invalidCommit.exitCode).toBe(0)
      const priorRuns = readFileSync(marker, "utf8")
      const invalid = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
      expect(readFileSync(marker, "utf8")).toBe(priorRuns)
      expect(invalid.exitCode).toBe(1)
      expect(
        invalid.stdout.toString().trim().endsWith(`verdict=refused mode=fast reason=${reason} exit_code=1`),
      ).toBe(true)
    }

    writeFileSync(manifest, `${packagePath}/test/empty.test.ts\n`)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    const inactiveSelectionCommit = Bun.spawnSync(["git", "commit", "-qm", "select skipped-only target"], {
      cwd: root,
      env: gitEnv,
    })
    expect(inactiveSelectionCommit.exitCode).toBe(0)
    const beforeInactiveFile = readFileSync(marker, "utf8")
    const inactiveFile = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(inactiveFile.exitCode).toBe(1)
    expect(
      inactiveFile.stdout.toString().trim().endsWith("verdict=refused mode=fast reason=no-test-cases exit_code=1"),
    ).toBe(true)
    expect(readFileSync(marker, "utf8")).toBe(beforeInactiveFile)
    expect(readdirSync(root).filter((name) => name.startsWith(".fast-suite-report."))).toEqual([])

    writeFileSync(manifest, `${packagePath}/test/failing.test.ts\n${selectedPath}\n${directPath}\n`)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    const selectFailureCommit = Bun.spawnSync(["git", "commit", "-qm", "select failing target"], {
      cwd: root,
      env: gitEnv,
    })
    expect(selectFailureCommit.exitCode).toBe(0)
    writeFileSync(marker, "")
    writeFileSync(hookLog, "")
    const fastFailure = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(fastFailure.exitCode).not.toBe(0)
    expect(
      fastFailure.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=3 exit_code=1"),
    ).toBe(true)
    expect(readFileSync(marker, "utf8").trim().split("\n").sort()).toEqual([
      "direct-configured",
      "failing",
      "selected-configured",
    ])
    expect(readFileSync(hookLog, "utf8").trim().split("\n")).toEqual([
      "pretest",
      "posttest",
      "pretest",
      "selected",
      "posttest",
    ])

    writeFileSync(manifest, validManifest)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    expect(Bun.spawnSync(["git", "commit", "-qm", "restore fast target"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    rmSync(untrackedTarget)
    writeFileSync(marker, "")
    const fullFailure = Bun.spawnSync(["bash", runner], runOptions)
    expect(fullFailure.exitCode).not.toBe(0)
    expect(fullFailure.stdout.toString().trim().endsWith("verdict=completed mode=full exit_code=1")).toBe(true)
    const fullRuns = readFileSync(marker, "utf8").trim().split("\n").sort()
    expect(fullRuns).toEqual([
      "failing",
      "glob",
      "second-configured",
      "selected-configured",
      "selected-nested",
      "selected-sibling",
      "unlisted",
      "whitespace",
    ])

    const fullWithArguments = Bun.spawnSync(["bash", runner, "--filter=./packages/example"], runOptions)
    expect(fullWithArguments.exitCode).not.toBe(0)
    expect(fullWithArguments.stdout.toString().trim().endsWith("verdict=completed mode=full exit_code=1")).toBe(true)
    expect(readFileSync(turboLog, "utf8").trim().split("\n").at(-1)).toBe("test --filter=./packages/example")
  },
  60_000,
)
