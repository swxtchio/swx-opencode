import { afterAll, describe, expect, test } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { localGit, syncUpstream, type Git, type SyncOptions } from "./sync-upstream"

// Every test builds a throwaway world with real git and no network: a bare `upstream`
// (the upstream repository), a bare `origin` (the fork), and a `work` checkout wired to
// both, with `dev` mirroring upstream/dev and `swxtch` carrying one fork-only commit.
// Global and system git config are ignored so a developer's hooks or identity cannot
// change the outcome.

const root = mkdtempSync(path.join(tmpdir(), "sync-upstream-"))
afterAll(() => rmSync(root, { recursive: true, force: true }))

const env = {
  ...process.env,
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_AUTHOR_NAME: "sync-test",
  GIT_AUTHOR_EMAIL: "sync-test@example.invalid",
  GIT_COMMITTER_NAME: "sync-test",
  GIT_COMMITTER_EMAIL: "sync-test@example.invalid",
}

const git = (dir: string, ...args: string[]) => {
  const result = localGit(dir, env)(...args)
  if (result.code !== 0) throw new Error(`git ${args.join(" ")} failed in ${dir}: ${result.stderr}`)
  return result.stdout
}

function world() {
  const dir = mkdtempSync(path.join(root, "world-"))
  const paths = {
    upsrc: path.join(dir, "upsrc"),
    up: path.join(dir, "up.git"),
    origin: path.join(dir, "origin.git"),
    work: path.join(dir, "work"),
    dir,
  }
  git(dir, "init", "-q", "-b", "dev", paths.upsrc)
  commit(paths.upsrc, "upstream.txt", "u0", "C0")
  git(dir, "clone", "-q", "--bare", paths.upsrc, paths.up)
  git(dir, "clone", "-q", paths.up, paths.work)
  git(paths.work, "remote", "rename", "origin", "upstream")
  git(paths.work, "checkout", "-q", "-b", "swxtch")
  commit(paths.work, "fork.txt", "f0", "fork-only commit on swxtch")
  git(dir, "clone", "-q", "--bare", paths.work, paths.origin)
  git(paths.work, "remote", "add", "origin", paths.origin)
  git(paths.work, "fetch", "-q", "origin")
  return paths
}

function commit(dir: string, file: string, content: string, message: string) {
  writeFileSync(path.join(dir, file), `${content}\n`)
  git(dir, "add", file)
  git(dir, "commit", "-qm", message)
}

function advanceUpstream(w: ReturnType<typeof world>, file: string, content: string) {
  commit(w.upsrc, file, content, `upstream: ${file}=${content}`)
  git(w.upsrc, "push", "-q", w.up, "dev")
}

// Bring local dev and origin/dev up to upstream/dev, so a following run finds the mirror
// already current while swxtch still lags upstream.
function presyncMirror(w: ReturnType<typeof world>) {
  git(w.work, "fetch", "-q", "upstream")
  git(w.work, "branch", "-f", "--no-track", "dev", "upstream/dev")
  git(w.work, "push", "-q", "origin", "dev")
  git(w.work, "fetch", "-q", "origin")
}

function run(repo: string, options: Partial<SyncOptions> = {}, wrap?: (real: Git) => Git) {
  const real = localGit(repo, env)
  const lines: string[] = []
  const result = syncUpstream({
    git: wrap ? wrap(real) : real,
    log: (line) => lines.push(line),
    allowPrimary: true,
    ...options,
  })
  return { ...result, out: lines.join("\n") }
}

const syncBranches = (repo: string) => git(repo, "branch", "--list", "sync-upstream-*")

// Let the exact sync merge populate MERGE_HEAD and the index, then fail before it
// commits, the way a rejecting hook would. Records every merge call so the test can prove
// the production cleanup ran.
function rejectCleanMerge(calls: string[]) {
  return (real: Git): Git =>
    (...args) => {
      if (args[0] === "merge") calls.push(args.join(" "))
      const target = args[3]
      if (args[0] !== "merge" || args[1] !== "--no-ff" || args[2] !== "--no-edit" || !target) return real(...args)
      const merged = real("merge", "--no-ff", "--no-commit", target)
      if (merged.code !== 0) return merged
      return { code: 1, stdout: "", stderr: "TEST-REJECTED-CLEAN-MERGE" }
    }
}

function failSubcommand(name: string) {
  return (real: Git): Git =>
    (...args) =>
      args[0] === name ? { code: 128, stdout: "", stderr: `injected fault: git ${name}` } : real(...args)
}

describe("syncUpstream", () => {
  // GOAL: the common case. Upstream advanced with a non-conflicting change, so the run
  // fast-forwards and publishes the mirror, then commits a --no-ff merge on a fresh
  // branch whose second parent is exactly the upstream commit the mirror moved to.
  test("prepares a clean merge on a new sync branch and pushes the mirror", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const upSha = git(w.up, "rev-parse", "dev")
    const swxtch = git(w.work, "rev-parse", "swxtch")
    const originSwxtch = git(w.origin, "rev-parse", "swxtch")

    const result = run(w.work, { now: new Date("2026-09-25T12:34:56Z") })

    expect(result.token).toBe("SYNC_CLEAN")
    expect(result.code).toBe(0)
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("sync-upstream-20260925-123456")
    expect(git(w.work, "rev-parse", "HEAD^1")).toBe(swxtch)
    expect(git(w.work, "rev-parse", "HEAD^2")).toBe(upSha)
    expect(git(w.work, "rev-parse", "dev")).toBe(upSha)
    expect(git(w.origin, "rev-parse", "dev")).toBe(upSha)
    expect(result.out).toContain("upstream: feature.txt=new")
    expect(result.out).toContain("--- changed files ---\nfeature.txt")
    // It prepares a branch for review; the fork's branch is never moved or pushed.
    expect(git(w.work, "rev-parse", "swxtch")).toBe(swxtch)
    expect(git(w.origin, "rev-parse", "swxtch")).toBe(originSwxtch)
  })

  // GOAL: conflicts are handed to a resolver, never auto-resolved or thrown away. The
  // merge stays in progress (MERGE_HEAD and markers intact) on the sync branch.
  test("leaves a conflicting merge in progress for a resolver", () => {
    const w = world()
    advanceUpstream(w, "fork.txt", "upstream edit")
    const swxtch = git(w.work, "rev-parse", "swxtch")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_CONFLICTS")
    expect(result.code).toBe(3)
    expect(result.out).toContain("--- conflicted files ---\nfork.txt")
    expect(git(w.work, "rev-parse", "--verify", "MERGE_HEAD")).toBe(git(w.up, "rev-parse", "dev"))
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toStartWith("sync-upstream-")
    expect(git(w.work, "rev-parse", "swxtch")).toBe(swxtch)
  })

  // GOAL: no empty branch or empty PR when upstream has nothing new for swxtch.
  test("reports up to date without creating a sync branch", () => {
    const w = world()

    const result = run(w.work)

    expect(result.token).toBe("SYNC_UPTODATE")
    expect(result.code).toBe(4)
    expect(syncBranches(w.work)).toBe("")
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
  })

  // GOAL: the landing rule in the header holds. Landing the sync branch as a merge keeps
  // upstream/dev an ancestor of swxtch, so the next run is a no-op rather than an empty
  // re-merge of the same commits.
  test("a landed sync makes the next run up to date", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const first = run(w.work, { now: new Date("2026-09-25T00:00:00Z") })
    expect(first.token).toBe("SYNC_CLEAN")
    const syncBranch = git(w.work, "symbolic-ref", "--short", "HEAD")
    git(w.work, "checkout", "-q", "swxtch")
    git(w.work, "merge", "-q", "--ff-only", syncBranch)
    git(w.work, "push", "-q", "origin", "swxtch")

    expect(run(w.work, { now: new Date("2026-09-25T00:00:01Z") }).token).toBe("SYNC_UPTODATE")
  })

  // GOAL: the mirror is never forced. A commit placed on dev makes it not a pure mirror,
  // which must abort before anything moves or is pushed.
  test("aborts when the mirror has commits upstream does not", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "checkout", "-q", "dev")
    commit(w.work, "local.txt", "x", "commit placed on the mirror")
    git(w.work, "checkout", "-q", "swxtch")
    const dev = git(w.work, "rev-parse", "dev")
    const originDev = git(w.origin, "rev-parse", "dev")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.code).toBe(2)
    expect(result.out).toContain("not fast-forwardable")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
    expect(git(w.origin, "rev-parse", "dev")).toBe(originDev)
    expect(syncBranches(w.work)).toBe("")
  })

  test("creates the local mirror when it does not exist yet", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "branch", "-D", "dev")

    const result = run(w.work)

    expect(result.out).toContain("MIRROR: created dev")
    expect(git(w.work, "rev-parse", "dev")).toBe(git(w.up, "rev-parse", "dev"))
    expect(result.token).toBe("SYNC_CLEAN")
  })

  // GOAL: update-ref never moves a branch out from under another worktree's HEAD.
  test("aborts when the mirror is checked out in another worktree", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "worktree", "add", "-q", path.join(w.dir, "dev-tree"), "dev")
    const dev = git(w.work, "rev-parse", "dev")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("checked out in another worktree")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
  })

  test("a failed mirror push warns and the sync still completes", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "remote", "set-url", "--push", "origin", path.join(w.dir, "missing.git"))

    const result = run(w.work)

    expect(result.out).toContain("MIRROR: WARNING could not push dev")
    expect(git(w.work, "rev-parse", "dev")).toBe(git(w.up, "rev-parse", "dev"))
    expect(result.token).toBe("SYNC_CLEAN")
  })

  test.each([
    ["an uncommitted edit", (w: ReturnType<typeof world>) => writeFileSync(path.join(w.work, "fork.txt"), "dirty\n")],
    [
      "an untracked file hidden by status.showUntrackedFiles=no",
      (w: ReturnType<typeof world>) => {
        git(w.work, "config", "status.showUntrackedFiles", "no")
        writeFileSync(path.join(w.work, "stray.txt"), "stray\n")
      },
    ],
  ])("aborts on a dirty tree with %s before moving anything", (_, dirty) => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    dirty(w)
    const dev = git(w.work, "rev-parse", "dev")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("dirty")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
    expect(syncBranches(w.work)).toBe("")
  })

  test("aborts naming the missing remote", () => {
    const w = world()
    git(w.work, "remote", "remove", "upstream")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("missing required remote 'upstream'")
  })

  test("aborts when the base branch does not exist", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "checkout", "-q", "--detach")
    git(w.work, "branch", "-D", "swxtch")
    const dev = git(w.work, "rev-parse", "dev")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("base branch swxtch does not exist")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
  })

  // GOAL: a teammate's pushed fork work is not silently left out of the sync base.
  test("warns when local swxtch is behind origin/swxtch", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const teammate = path.join(w.dir, "teammate")
    git(w.dir, "clone", "-q", "--branch", "swxtch", w.origin, teammate)
    commit(teammate, "teammate.txt", "t", "teammate work")
    git(teammate, "push", "-q", "origin", "swxtch")

    expect(run(w.work).out).toContain("BASE: WARNING local swxtch is behind origin/swxtch")
  })

  test("does not warn when local swxtch matches origin/swxtch", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    expect(run(w.work).out).not.toContain("BASE: WARNING")
  })

  // GOAL: fail closed. A probe that errors must not be read as "clean" or "safe".
  test.each(["status", "worktree"])("aborts when the %s probe itself fails", (probe) => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const dev = git(w.work, "rev-parse", "dev")

    const result = run(w.work, {}, failSubcommand(probe))

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("injected fault")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
  })

  // GOAL: a merge that fails without conflicts, such as a rejecting hook, is neither
  // misreported as a conflict nor as "nothing happened" once the mirror has moved. The
  // half-merge is aborted and the empty sync branch deleted.
  test("a non-conflict merge failure after the mirror moved is SYNC_MERGE_FAILED", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const calls: string[] = []

    const result = run(w.work, {}, rejectCleanMerge(calls))

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.code).toBe(5)
    expect(result.out).toContain("TEST-REJECTED-CLEAN-MERGE")
    expect(result.out).toContain("deleted the empty sync branch")
    expect(result.out).toContain("the dev mirror was advanced and pushed to origin")
    expect(calls).toContain("merge --abort")
    expect(localGit(w.work, env)("rev-parse", "--verify", "--quiet", "MERGE_HEAD").code).not.toBe(0)
    expect(git(w.work, "status", "--porcelain", "--untracked-files=all")).toBe("")
    expect(syncBranches(w.work)).toBe("")
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
  })

  // GOAL: the failure report states what actually happened, never overclaiming a push.
  test("SYNC_MERGE_FAILED reports a failed mirror push truthfully", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "remote", "set-url", "--push", "origin", path.join(w.dir, "missing.git"))

    const result = run(w.work, {}, rejectCleanMerge([]))

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("the push to origin FAILED")
    expect(result.out).not.toContain("advanced and pushed to origin")
  })

  // GOAL: the token follows what changed, not how far the run got. With the mirror
  // already current nothing durable moved, so the same merge failure is a SYNC_ABORT.
  test("a merge failure with an already-current mirror is SYNC_ABORT", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    presyncMirror(w)

    const result = run(w.work, {}, rejectCleanMerge([]))

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("the dev mirror was already current")
    expect(syncBranches(w.work)).toBe("")
  })

  // GOAL: a post-checkout hook that fails after git switched branches does not strand
  // HEAD on the sync branch.
  test("a failed sync-branch checkout after switching cleans up and restores HEAD", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = run(w.work, {}, (real) => (...args) => {
      if (args[0] !== "checkout" || args[2] !== "-b") return real(...args)
      const switched = real(...args)
      if (switched.code !== 0) return switched
      return { code: 1, stdout: "", stderr: "POST-CHECKOUT-FAIL" }
    })

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("could not create sync branch")
    expect(result.out).toContain("deleted the empty sync branch")
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
    expect(syncBranches(w.work)).toBe("")
  })

  // GOAL: cleanup only ever deletes a branch this run created. A concurrent process that
  // takes the sync-branch name after the freeness check keeps its branch.
  test("a checkout race leaves the other process's branch untouched", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const racePoint = git(w.work, "rev-parse", "swxtch^")

    const result = run(w.work, {}, (real) => (...args) => {
      const name = args[3]
      if (args[0] !== "checkout" || args[2] !== "-b" || !name) return real(...args)
      real("branch", name, racePoint)
      return { code: 128, stdout: "", stderr: `fatal: a branch named '${name}' already exists.` }
    })

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("was not created by this run - left untouched")
    const raced = syncBranches(w.work).trim()
    expect(raced).toStartWith("sync-upstream-")
    expect(git(w.work, "rev-parse", raced)).toBe(racePoint)
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
  })

  // GOAL: the primary checkout is never left on a sync branch. The guard reads the real
  // worktree layout: refused in the primary, allowed in a linked worktree.
  test("refuses the primary checkout and runs in a linked worktree", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const primary = run(w.work, { allowPrimary: false })
    expect(primary.token).toBe("SYNC_ABORT")
    expect(primary.out).toContain("refusing to run in the PRIMARY checkout")
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
    expect(syncBranches(w.work)).toBe("")

    const linked = path.join(w.dir, "linked")
    git(w.work, "worktree", "add", "-q", "--detach", linked, "swxtch")
    const result = run(linked, { allowPrimary: false })
    expect(result.token).toBe("SYNC_CLEAN")
    expect(git(linked, "symbolic-ref", "--short", "HEAD")).toStartWith("sync-upstream-")
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
  })
})

describe("sync-upstream CLI", () => {
  const cli = (cwd: string, ...args: string[]) => {
    const proc = Bun.spawnSync(["bun", path.join(import.meta.dir, "sync-upstream.ts"), ...args], {
      cwd,
      env,
      stdout: "pipe",
      stderr: "pipe",
    })
    return { code: proc.exitCode, out: `${proc.stdout.toString()}${proc.stderr.toString()}` }
  }

  test("--help exits 0 and an unknown argument exits 2", () => {
    expect(cli(root, "--help").code).toBe(0)
    expect(cli(root, "--bogus").code).toBe(2)
  })

  // GOAL: the real entrypoint keeps the primary-checkout guard on.
  test("refuses to run from the primary checkout", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = cli(w.work)

    expect(result.code).toBe(2)
    expect(result.out).toContain("refusing to run in the PRIMARY checkout")
    expect(syncBranches(w.work)).toBe("")
  })
})
