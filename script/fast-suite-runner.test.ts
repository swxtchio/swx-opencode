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

// GOAL: prove the root entrypoint executes only committed manifest targets in their package
// context, refuses dirty and unusable runs before tests start, and propagates real Bun test failures.
test(
  "tests/run.sh dispatches registered Turbo tasks and direct package tests for the declared targets",
  () => {
    const root = join(scratch, "repo")
    const marker = join(scratch, "executed.txt")
    const turboLog = join(scratch, "turbo-argv.txt")
    const runner = join(root, "tests", "run.sh")
    const manifest = join(root, "tests", "fast-suite.txt")
    const packageRoot = join(root, "packages", "example")
    const packageTests = join(packageRoot, "test")
    const directPackageRoot = join(root, "packages", "direct")
    const directPackageTests = join(directPackageRoot, "test")
    const turbo = join(root, "node_modules", ".bin", "turbo")
    const packagePath = "packages/example"
    const selectedPath = `${packagePath}/test/selected.test.ts`
    const selectedSecondPath = `${packagePath}/test/selected-second.test.ts`
    const literalGlobPath = `${packagePath}/test/literal*.test.ts`
    const directPath = "packages/direct/test/direct.test.ts"
    const validManifest = `# only the declared test files run

${selectedPath}
${selectedSecondPath}
${directPath}
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
        FAST_TURBO_LOG: turboLog,
      },
      stdout: "pipe" as const,
      stderr: "pipe" as const,
    }

    mkdirSync(packageTests, { recursive: true })
    mkdirSync(directPackageTests, { recursive: true })
    mkdirSync(dirname(runner), { recursive: true })
    mkdirSync(dirname(turbo), { recursive: true })
    copyFileSync(join(import.meta.dir, "..", "tests", "run.sh"), runner)
    chmodSync(runner, 0o755)
    writeFileSync(
      join(root, "package.json"),
      JSON.stringify({
        name: "fast-suite-fixture",
        private: true,
        packageManager: "bun@1.4.2",
        workspaces: { packages: ["packages/*"] },
      }),
    )
    writeFileSync(join(root, ".gitignore"), "packages/example/test/untracked.test.ts\n")
    writeFileSync(
      join(root, "turbo.json"),
      JSON.stringify({ tasks: { "@fixture/example#test": { outputs: [], dependsOn: ["^build"] } } }),
    )
    writeFileSync(
      join(packageRoot, "package.json"),
      JSON.stringify({
        name: "@fixture/example",
        private: true,
        type: "module",
        scripts: { test: "FAST_SUITE_SCRIPT=package bun test --timeout 5000 --max-concurrency=1" },
      }),
    )
    writeFileSync(
      join(directPackageRoot, "package.json"),
      JSON.stringify({
        name: "@fixture/direct",
        private: true,
        type: "module",
        scripts: { test: "FAST_SUITE_SCRIPT=direct-package bun test --timeout 5000 --max-concurrency=1" },
      }),
    )
    writeFileSync(join(packageRoot, "bunfig.toml"), '[test]\npreload = ["./test/preload.ts"]\n')
    writeFileSync(join(directPackageRoot, "bunfig.toml"), '[test]\npreload = ["./test/preload.ts"]\n')
    writeFileSync(join(packageTests, "preload.ts"), "process.env.FAST_SUITE_PRELOAD = process.cwd()\n")
    writeFileSync(join(directPackageTests, "preload.ts"), "process.env.FAST_SUITE_PRELOAD = process.cwd()\n")
    writeFileSync(
      join(packageTests, "selected.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses the package script and preload", () => {',
        '  expect(process.env.FAST_SUITE_SCRIPT).toBe("package")',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "selected\\n")',
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
        'test("uses the package test task for every selected file", () => {',
        '  expect(process.env.FAST_SUITE_SCRIPT).toBe("package")',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "second\\n")',
        "})",
        "",
      ].join("\n"),
    )
    writeFileSync(
      join(directPackageTests, "direct.test.ts"),
      [
        'import { appendFileSync } from "node:fs"',
        'import { expect, test } from "bun:test"',
        'test("uses the direct package script and preload", () => {',
        '  expect(process.env.FAST_SUITE_SCRIPT).toBe("direct-package")',
        '  expect(process.env.FAST_SUITE_PRELOAD).toBe(process.cwd())',
        '  appendFileSync(process.env.FAST_SUITE_MARKER!, "direct\\n")',
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
        'test("runs only in the full suite", () => {',
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
    expect(fast.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=3 exit_code=0")).toBe(true)
    expect(readFileSync(marker, "utf8").trim().split("\n").sort()).toEqual(["direct", "second", "selected"])
    expect(readdirSync(root).filter((name) => name.startsWith(".fast-suite-report."))).toEqual([])
    const turboCalls = readFileSync(turboLog, "utf8").trim().split("\n")
    expect(turboCalls).toHaveLength(1)
    expect(turboCalls[0]).toContain("test --filter=./packages/example --force -- --reporter=junit")
    expect(turboCalls[0]).toContain("test/selected.test.ts")
    expect(turboCalls[0]).toContain("test/selected-second.test.ts")

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
      [`${literalGlobPath}\n`, "invalid-fast-manifest"],
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
      expect(invalid.exitCode).toBe(1)
      expect(
        invalid.stdout.toString().trim().endsWith(`verdict=refused mode=fast reason=${reason} exit_code=1`),
      ).toBe(true)
      expect(readFileSync(marker, "utf8")).toBe(priorRuns)
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

    writeFileSync(manifest, `${packagePath}/test/failing.test.ts\n`)
    expect(Bun.spawnSync(["git", "add", "tests/fast-suite.txt"], { cwd: root, env: gitEnv }).exitCode).toBe(0)
    const selectFailureCommit = Bun.spawnSync(["git", "commit", "-qm", "select failing target"], {
      cwd: root,
      env: gitEnv,
    })
    expect(selectFailureCommit.exitCode).toBe(0)
    writeFileSync(marker, "")
    const fastFailure = Bun.spawnSync(["bash", runner, "--fast"], runOptions)
    expect(fastFailure.exitCode).not.toBe(0)
    expect(
      fastFailure.stdout.toString().trim().endsWith("verdict=completed mode=fast target_count=1 exit_code=1"),
    ).toBe(true)
    expect(readFileSync(marker, "utf8")).toBe("failing\n")

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
      "second",
      "selected",
      "selected-nested",
      "selected-sibling",
      "unlisted",
    ])

    const fullWithArguments = Bun.spawnSync(["bash", runner, "--filter=./packages/example"], runOptions)
    expect(fullWithArguments.exitCode).not.toBe(0)
    expect(fullWithArguments.stdout.toString().trim().endsWith("verdict=completed mode=full exit_code=1")).toBe(true)
    expect(readFileSync(turboLog, "utf8").trim().split("\n").at(-1)).toBe("test --filter=./packages/example")
  },
  20_000,
)
