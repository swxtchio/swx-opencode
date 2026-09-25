import { afterAll, describe, expect, test } from "bun:test"
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { changesetMarker, localGit, syncUpstream, type Git, type SyncOptions } from "./sync-upstream"

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
  commit(paths.work, "CHANGESET.md", changeset, "fork changeset")
  git(dir, "clone", "-q", "--bare", paths.work, paths.origin)
  git(paths.work, "remote", "add", "origin", paths.origin)
  git(paths.work, "fetch", "-q", "origin")
  return paths
}

const changeset = `# Fork changeset\n\n## Upstream syncs\n\n${changesetMarker} entries below -->\n\n- **2026-01-01** earlier sync.\n\n## Fork changes\n`

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

// A failed sync leaves no merge in progress, a clean tree, no sync branch, and HEAD back
// where it started.
function expectAbandoned(w: ReturnType<typeof world>) {
  expect(localGit(w.work, env)("rev-parse", "--verify", "--quiet", "MERGE_HEAD").code).not.toBe(0)
  expect(git(w.work, "status", "--porcelain", "--untracked-files=all")).toBe("")
  expect(syncBranches(w.work)).toBe("")
  expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toBe("swxtch")
}

// Let the sync merge populate MERGE_HEAD and the index, then reject the commit that would
// record it, the way a failing pre-commit hook would. Records every merge call so the test
// can prove the production cleanup ran.
function rejectMergeCommit(calls: string[]) {
  return (real: Git): Git =>
    (...args) => {
      if (args[0] === "merge") calls.push(args.join(" "))
      if (args[0] !== "commit") return real(...args)
      return { code: 1, stdout: "", stderr: "TEST-REJECTED-MERGE-COMMIT" }
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
    expect(git(w.work, "log", "-1", "--format=%s")).toBe(
      `chore: sync swxtch with upstream/dev at ${git(w.work, "rev-parse", "--short", upSha)}`,
    )
    expect(git(w.work, "rev-parse", "dev")).toBe(upSha)
    expect(git(w.origin, "rev-parse", "dev")).toBe(upSha)
    expect(result.out).toContain("upstream: feature.txt=new")
    expect(result.out).toContain("the dev mirror was advanced and pushed to origin")
    expect(result.out).toContain("--- changed files ---\nfeature.txt")
    // The changeset entry is part of the merge commit, newest first above earlier entries.
    const recorded = git(w.work, "show", "HEAD:CHANGESET.md")
    const entry = `- **2026-09-25** \`${git(w.work, "rev-parse", "--short", git(w.work, "merge-base", "HEAD^1", "HEAD^2"))}..${git(w.work, "rev-parse", "--short", upSha)}\`, 1 upstream commit (sync-upstream-20260925-123456). Conflicts: none.`
    expect(recorded).toContain(`entries below -->\n\n${entry}\n- **2026-01-01** earlier sync.`)
    expect(git(w.work, "status", "--porcelain")).toBe("")
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
    // The entry is staged with the in-progress merge, naming the file to resolve.
    expect(git(w.work, "show", ":CHANGESET.md")).toContain("Conflicts: `fork.txt` - record how each was resolved.")
    expect(git(w.work, "show", ":CHANGESET.md")).toContain("- **2026-01-01** earlier sync.")
    // The resolver's `git commit` gets the conventional message, not "Merge commit '<sha>'".
    const mergeMsg = git(w.work, "rev-parse", "--path-format=absolute", "--git-path", "MERGE_MSG")
    expect(readFileSync(mergeMsg, "utf8")).toStartWith("chore: sync swxtch with upstream/dev at ")
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

  // GOAL: fetching never touches local tags. An abort leaves no new tag behind, and a local
  // tag that clashes with upstream's does not block the sync.
  test("fetches without tags", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.upsrc, "tag", "v-new")
    git(w.upsrc, "tag", "v-clash")
    git(w.upsrc, "push", "-q", w.up, "v-new", "v-clash")
    git(w.work, "tag", "v-clash", "swxtch")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_CLEAN")
    expect(git(w.work, "tag", "--list")).toBe("v-clash")
    expect(git(w.work, "rev-parse", "v-clash")).toBe(git(w.work, "rev-parse", "swxtch"))
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

  // GOAL: a sync is never reported ready without its changeset entry. When the entry cannot
  // be written, the merge is abandoned like any other non-conflict failure.
  test.each([
    [
      "CHANGESET.md is missing",
      (w: ReturnType<typeof world>) => git(w.work, "rm", "-q", "CHANGESET.md"),
      "could not read",
    ],
    [
      "the marker is not under the sync heading",
      (w: ReturnType<typeof world>) =>
        writeFileSync(
          path.join(w.work, "CHANGESET.md"),
          `# Fork changeset\n\n## Fork changes\n\n${changesetMarker} -->\n`,
        ),
      'is not under "## Upstream syncs"',
    ],
    [
      "CHANGESET.md has no marker",
      (w: ReturnType<typeof world>) => writeFileSync(path.join(w.work, "CHANGESET.md"), "# Fork changeset\n"),
      'no "<!-- upstream-syncs:" line',
    ],
  ])("fails without committing when %s", (_, breakChangeset, reason) => {
    const w = world()
    breakChangeset(w)
    git(w.work, "commit", "-qam", "break changeset")
    git(w.work, "push", "-q", "origin", "swxtch")
    advanceUpstream(w, "feature.txt", "new")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain(reason)
    expectAbandoned(w)
  })

  // GOAL: a filesystem failure is reported through the same cleanup as a git failure,
  // instead of throwing past it and leaving a half-merged sync branch.
  test.skipIf(process.getuid?.() === 0)("fails without committing when CHANGESET.md cannot be written", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    chmodSync(path.join(w.work, "CHANGESET.md"), 0o444)

    const result = run(w.work)

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("could not write")
    expectAbandoned(w)
  })

  // GOAL: CHANGESET.md is never staged as resolved while it holds conflict markers. If it
  // conflicts itself, it stays unmerged and the resolver gets the entry to add.
  test("leaves a conflicted CHANGESET.md unmerged and hands over the entry", () => {
    const w = world()
    advanceUpstream(w, "CHANGESET.md", "upstream's own changeset")

    const result = run(w.work, { now: new Date("2026-09-25T12:34:56Z") })

    expect(result.token).toBe("SYNC_CONFLICTS")
    expect(git(w.work, "diff", "--name-only", "--diff-filter=U")).toBe("CHANGESET.md")
    expect(result.out).toContain("CHANGESET: WARNING CHANGESET.md itself conflicted")
    expect(result.out).toContain("(sync-upstream-20260925-123456). Conflicts: `CHANGESET.md`")
  })

  // GOAL: the insertion handles a marker on the file's last line with no trailing newline.
  test("records the entry when the marker ends the file", () => {
    const w = world()
    writeFileSync(path.join(w.work, "CHANGESET.md"), `# Fork changeset\n\n## Upstream syncs\n${changesetMarker} -->`)
    git(w.work, "commit", "-qam", "marker at end of file")
    git(w.work, "push", "-q", "origin", "swxtch")
    advanceUpstream(w, "feature.txt", "new")

    expect(run(w.work).token).toBe("SYNC_CLEAN")
    // Read the checked-out file, not `git show`, whose output the helper trims: the test
    // pins that the file ends in exactly one newline.
    expect(readFileSync(path.join(w.work, "CHANGESET.md"), "utf8")).toMatch(
      new RegExp(
        `^# Fork changeset\\n\\n## Upstream syncs\\n${changesetMarker} -->\\n\\n- \\*\\*\\d{4}-\\d{2}-\\d{2}\\*\\* [^\\n]+Conflicts: none\\.\\n$`,
      ),
    )
  })

  // GOAL: the mirror is never moved out from under another worktree's HEAD.
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

  test("aborts when origin has no base branch", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const dev = git(w.work, "rev-parse", "dev")

    const result = run(w.work, { base: "missing" })

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("refs/remotes/origin/missing does not exist")
    expect(git(w.work, "rev-parse", "dev")).toBe(dev)
  })

  // GOAL: the sync PR targets origin, so its base is origin/swxtch. A teammate's pushed work
  // is included even when local swxtch lags.
  test("bases the sync on origin/swxtch when local swxtch is behind", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const teammate = path.join(w.dir, "teammate")
    git(w.dir, "clone", "-q", "--branch", "swxtch", w.origin, teammate)
    commit(teammate, "teammate.txt", "t", "teammate work")
    git(teammate, "push", "-q", "origin", "swxtch")

    expect(run(w.work).token).toBe("SYNC_CLEAN")
    expect(git(w.work, "rev-parse", "HEAD^1")).toBe(git(w.origin, "rev-parse", "swxtch"))
  })

  // GOAL: unpushed local commits never ride into the sync PR unreviewed.
  test("keeps unpushed local commits out of the sync", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    commit(w.work, "local.txt", "l", "unpushed local work")

    expect(run(w.work).token).toBe("SYNC_CLEAN")
    expect(git(w.work, "rev-parse", "HEAD^1")).toBe(git(w.origin, "rev-parse", "swxtch"))
    expect(localGit(w.work, env)("cat-file", "-e", "HEAD:local.txt").code).not.toBe(0)
  })

  test("aborts when the sync branch name is already taken", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const now = new Date("2026-09-25T12:34:56Z")
    expect(run(w.work, { now }).token).toBe("SYNC_CLEAN")
    git(w.work, "checkout", "-q", "swxtch")

    const result = run(w.work, { now })

    expect(result.token).toBe("SYNC_ABORT")
    expect(result.out).toContain("sync-upstream-20260925-123456 already exists")
  })

  // GOAL: an up-to-date run still says the mirror moved when it did.
  test("reports a moved mirror on an up-to-date run", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "fetch", "-q", "upstream")
    git(w.work, "merge", "-q", "--no-edit", "upstream/dev")
    git(w.work, "push", "-q", "origin", "swxtch")

    const result = run(w.work)

    expect(result.token).toBe("SYNC_UPTODATE")
    expect(result.out).toContain("the dev mirror was advanced and pushed to origin")
  })

  // GOAL: fail closed. A probe that errors must not be read as "clean" or "safe".
  test.each(["status", "fetch"])("aborts when git %s itself fails", (probe) => {
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

    const result = run(w.work, {}, rejectMergeCommit(calls))

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.code).toBe(5)
    expect(result.out).toContain("TEST-REJECTED-MERGE-COMMIT")
    expect(result.out).toContain("deleted the empty sync branch")
    expect(result.out).toContain("the dev mirror was advanced and pushed to origin")
    expect(calls).toContain("merge --abort")
    expectAbandoned(w)
  })

  // GOAL: a staging failure after the entry is written leaves nothing behind. merge --abort
  // is reset --merge, which refuses an unstaged change, so the file is restored first.
  test("a failed stage of the changeset entry is cleaned up", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = run(
      w.work,
      {},
      (real) =>
        (...args) =>
          args[0] === "add" ? { code: 128, stdout: "", stderr: "fatal: index.lock exists" } : real(...args),
    )

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("could not stage")
    expect(result.out).not.toContain("NOT clean")
    expectAbandoned(w)
  })

  // GOAL: a failure to write the merge message is reported, not thrown past cleanup.
  test("a failed merge message write is cleaned up", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = run(
      w.work,
      {},
      (real) =>
        (...args) =>
          args.includes("--git-path") ? { code: 128, stdout: "", stderr: "fatal: injected" } : real(...args),
    )

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("could not write the merge message")
    expectAbandoned(w)
  })

  // GOAL: cleanup is read back, never assumed. A merge --abort that fails leaves the merge in
  // progress, and the report says so instead of claiming the branch was cleaned up.
  test("a failed merge --abort is reported, not claimed as cleanup", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = run(w.work, {}, (real) => (...args) => {
      if (args[0] === "commit") return { code: 1, stdout: "", stderr: "TEST-REJECTED-MERGE-COMMIT" }
      if (args[0] === "merge" && args[1] === "--abort") return { code: 128, stdout: "", stderr: "fatal: injected" }
      return real(...args)
    })

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("the merge is STILL in progress")
    expect(result.out).not.toContain("deleted the empty sync branch")
    // HEAD stays on the sync branch with the merge, never carried onto swxtch.
    expect(git(w.work, "symbolic-ref", "--short", "HEAD")).toStartWith("sync-upstream-")
    expect(localGit(w.work, env)("rev-parse", "--verify", "--quiet", "MERGE_HEAD").code).toBe(0)
  })

  // GOAL: the failure report says when cleanup left the tree dirty instead of implying it
  // is clean.
  test("reports a working tree that cleanup left dirty", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")

    const result = run(w.work, {}, (real) => (...args) => {
      if (args[0] === "commit") return { code: 1, stdout: "", stderr: "TEST-REJECTED-MERGE-COMMIT" }
      const done = real(...args)
      if (args[0] === "merge" && args[1] === "--abort") writeFileSync(path.join(w.work, "left-behind.txt"), "x\n")
      return done
    })

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("the working tree is NOT clean")
  })

  // GOAL: the token follows the remote, not the push exit code. A push that updated
  // origin/dev and then exited nonzero still moved something, so a later failure is not the
  // nothing-happened SYNC_ABORT; a push whose outcome cannot be read back is treated the same.
  test.each([
    ["landed despite a nonzero exit", "landed", "MIRROR: pushed dev to origin"],
    ["could not be read back", "unknown", "whether origin/dev moved is UNKNOWN"],
  ])("a push that %s counts as a possible mutation", (_, outcome, report) => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "fetch", "-q", "upstream")
    git(w.work, "branch", "-f", "--no-track", "dev", "upstream/dev")

    const result = run(w.work, {}, (real) => (...args) => {
      if (args[0] === "commit") return { code: 1, stdout: "", stderr: "TEST-REJECTED-MERGE-COMMIT" }
      if (args[0] === "push") {
        if (outcome === "landed") real(...args)
        return { code: 1, stdout: "", stderr: "error: failed to push some refs" }
      }
      if (args[0] === "ls-remote" && outcome === "unknown") return { code: 128, stdout: "", stderr: "fatal: injected" }
      return real(...args)
    })

    expect(result.out).toContain(report)
    expect(result.token).toBe("SYNC_MERGE_FAILED")
  })

  // GOAL: a tree the cleanup cannot inspect is reported as unverified, never implied clean.
  test("reports a working tree that cleanup could not inspect", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const statuses: string[] = []

    const result = run(w.work, {}, (real) => (...args) => {
      if (args[0] === "commit") return { code: 1, stdout: "", stderr: "TEST-REJECTED-MERGE-COMMIT" }
      // The first status is the precondition check, which must pass for the run to start.
      if (args[0] === "status" && statuses.push("status") > 1)
        return { code: 128, stdout: "", stderr: "fatal: injected" }
      return real(...args)
    })

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("whether the working tree is clean could NOT be verified")
  })

  // GOAL: a run started detached (the documented worktree setup) returns to that commit on
  // failure, since there is no branch to go back to.
  test("a failure in a detached worktree returns HEAD to the start commit", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    const linked = path.join(w.dir, "linked")
    git(w.work, "worktree", "add", "-q", "--detach", linked, "origin/swxtch")
    const start = git(linked, "rev-parse", "HEAD")

    const result = run(linked, { allowPrimary: false }, rejectMergeCommit([]))

    expect(result.token).toBe("SYNC_MERGE_FAILED")
    expect(result.out).toContain("deleted the empty sync branch")
    expect(localGit(linked, env)("symbolic-ref", "-q", "HEAD").code).not.toBe(0)
    expect(git(linked, "rev-parse", "HEAD")).toBe(start)
    expect(syncBranches(w.work)).toBe("")
  })

  // GOAL: the failure report states what actually happened, never overclaiming a push.
  test("SYNC_MERGE_FAILED reports a failed mirror push truthfully", () => {
    const w = world()
    advanceUpstream(w, "feature.txt", "new")
    git(w.work, "remote", "set-url", "--push", "origin", path.join(w.dir, "missing.git"))

    const result = run(w.work, {}, rejectMergeCommit([]))

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

    const result = run(w.work, {}, rejectMergeCommit([]))

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
