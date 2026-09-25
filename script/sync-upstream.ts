#!/usr/bin/env bun
// Prepare a merge-based sync of this fork with upstream, safely and non-destructively.
//
// Ported from swx-firstmate's bin/fm-sync-upstream.sh. Branch model it assumes and enforces:
//   dev    = a read-only mirror of upstream/dev. Nothing is ever committed to it, so it
//            always fast-forwards to upstream/dev.
//   swxtch = the fork's default branch, holding fork work on top of upstream. The sync
//            merge lands on a fresh branch off it, never on swxtch itself.
//
// Merge, never rebase: swxtch is published, so a rebase would force-push it and rewrite
// commits that merged PRs reference. A --no-ff merge takes everything new from upstream,
// and the fork's changes are reviewed as the conflict resolutions on the sync PR.
//
// Run it from a dedicated worktree, not the primary checkout, because it leaves the tree
// on the new sync branch (enforced below):
//
//   git worktree add --detach ../swx-opencode-sync origin/swxtch
//   cd ../swx-opencode-sync && bun script/sync-upstream.ts
//
// In order it:
//   1. Requires a clean tree and the upstream/origin remotes, fetches both, and warns when
//      local swxtch lags or diverges from origin/swxtch (the sync bases off local).
//   2. Validates every abort condition before moving or pushing anything.
//   3. Fast-forwards dev to upstream/dev (FF only) and pushes it to origin.
//   4. Stops with SYNC_UPTODATE when swxtch already contains upstream/dev.
//   5. Creates sync-upstream-<UTC timestamp> off swxtch and runs `git merge --no-ff`.
//
// It never pushes swxtch, opens a PR, or resolves conflicts. On conflicts the merge is left
// in progress so a resolver keeps git's partial auto-merge. Land the sync PR with a merge
// commit, never a squash: squashing drops the upstream/dev parent, so the next run would
// re-merge the same commits into an empty diff.
//
// Report tokens (line-anchored) and exit codes:
//   SYNC_CLEAN         0  merge committed on the sync branch, ready for review
//   SYNC_ABORT         2  nothing durable moved or was pushed; fix the cause and re-run
//   SYNC_CONFLICTS     3  merge left in progress on the sync branch for a resolver
//   SYNC_UPTODATE      4  swxtch already contains upstream/dev; no sync branch created
//   SYNC_MERGE_FAILED  5  the mirror moved or was pushed, then the sync branch could not be
//                         created or the merge failed for a non-conflict reason
//
// Every decision reads actual git state. The whole run pins one upstream and one base
// commit, so a concurrent fetch or branch move cannot change what it acts on.

export type Git = (...args: string[]) => { code: number; stdout: string; stderr: string }

export type SyncOptions = {
  git: Git
  log?: (line: string) => void
  upstream?: string
  origin?: string
  mirror?: string
  base?: string
  // The guard refusing the primary checkout. Only tests of throwaway single-worktree
  // repositories turn it off.
  allowPrimary?: boolean
  now?: Date
}

export const exitCodes = {
  SYNC_CLEAN: 0,
  SYNC_ABORT: 2,
  SYNC_CONFLICTS: 3,
  SYNC_UPTODATE: 4,
  SYNC_MERGE_FAILED: 5,
} as const

export type SyncToken = keyof typeof exitCodes

export function localGit(repo: string, env: Record<string, string | undefined> = process.env): Git {
  return (...args) => {
    const proc = Bun.spawnSync(["git", "-C", repo, ...args], { env, stdout: "pipe", stderr: "pipe" })
    return { code: proc.exitCode, stdout: proc.stdout.toString().trim(), stderr: proc.stderr.toString().trim() }
  }
}

export function syncUpstream(options: SyncOptions) {
  const git = options.git
  const log = options.log ?? console.log
  const upstream = options.upstream ?? "upstream"
  const origin = options.origin ?? "origin"
  const mirror = options.mirror ?? "dev"
  const base = options.base ?? "swxtch"
  const finish = (token: SyncToken, message: string) => {
    log(`${token}: ${message}`)
    return { token, code: exitCodes[token] }
  }
  const abort = (message: string) => finish("SYNC_ABORT", message)
  const commit = (ref: string) => {
    const result = git("rev-parse", "--verify", "--quiet", `${ref}^{commit}`)
    return result.code === 0 ? result.stdout : ""
  }
  const short = (sha: string) => git("rev-parse", "--short", sha).stdout || sha
  const isAncestor = (ancestor: string, descendant: string) =>
    git("merge-base", "--is-ancestor", ancestor, descendant).code === 0
  const currentBranch = () => git("symbolic-ref", "--quiet", "--short", "HEAD").stdout

  // --- phase 1: preconditions and fetch (moves no local branch, pushes nothing) ---

  if (git("rev-parse", "--is-inside-work-tree").stdout !== "true") return abort("not inside a git working tree")

  // A linked worktree's git dir is <common>/worktrees/<name>; the primary's is the common
  // dir itself. Fail closed when the layout cannot be read.
  if (!options.allowPrimary) {
    const gitDir = git("rev-parse", "--absolute-git-dir")
    const commonDir = git("rev-parse", "--path-format=absolute", "--git-common-dir")
    if (gitDir.code !== 0 || commonDir.code !== 0) return abort("could not determine the worktree layout")
    if (gitDir.stdout === commonDir.stdout)
      return abort(
        "refusing to run in the PRIMARY checkout: it would be left on the sync branch. Run it from a dedicated worktree: `git worktree add --detach ../swx-opencode-sync origin/swxtch`",
      )
  }

  // --untracked-files=all overrides a status.showUntrackedFiles=no config that would hide
  // untracked files. Check the exit code too: a broken repository prints nothing and exits
  // nonzero, which an output-only check would misread as clean.
  const status = git("status", "--porcelain", "--untracked-files=all")
  if (status.code !== 0)
    return abort(`could not read working-tree status (git exited ${status.code}): ${firstLine(status.stderr)}`)
  if (status.stdout) return abort("working tree is dirty; commit or stash before syncing")

  const missing = [upstream, origin].find((remote) => git("remote", "get-url", remote).code !== 0)
  if (missing)
    return abort(
      `missing required remote '${missing}' (found: ${git("remote").stdout.split("\n").join(",") || "none"})`,
    )

  const upstreamFetch = git("fetch", upstream, "--tags")
  if (upstreamFetch.code !== 0) return abort(`git fetch ${upstream} failed: ${firstLine(upstreamFetch.stderr)}`)
  const originFetch = git("fetch", origin)
  if (originFetch.code !== 0) return abort(`git fetch ${origin} failed: ${firstLine(originFetch.stderr)}`)

  const upstreamRef = `refs/remotes/${upstream}/${mirror}`
  const upSha = commit(upstreamRef)
  if (!upSha) return abort(`${upstreamRef} does not exist after fetch (does ${upstream} have ${mirror}?)`)

  // The sync bases off LOCAL swxtch. Warn when a teammate pushed work local does not have,
  // so its conflicts are not silently deferred to PR-merge time. Local being ahead is fine.
  const localBase = commit(`refs/heads/${base}`)
  const originBase = commit(`refs/remotes/${origin}/${base}`)
  if (localBase && originBase && localBase !== originBase && !isAncestor(originBase, localBase)) {
    const pair = `${short(localBase)} vs ${short(originBase)}`
    log(
      isAncestor(localBase, originBase)
        ? `BASE: WARNING local ${base} is behind ${origin}/${base} - ${pair}; fast-forward local ${base} first`
        : `BASE: WARNING local ${base} has DIVERGED from ${origin}/${base} - ${pair}; reconcile local ${base} first`,
    )
  }

  // --- phase 2: validate every abort condition before any mutation ---

  const baseSha = localBase
  if (!baseSha) return abort(`base branch ${base} does not exist`)

  const syncBranch = `sync-upstream-${(options.now ?? new Date()).toISOString().slice(0, 19).replace(/[-:]/g, "").replace("T", "-")}`
  if (commit(`refs/heads/${syncBranch}`))
    return abort(`sync branch ${syncBranch} already exists (run again in a moment)`)

  // Fail closed: if the worktree probe errors, the mirror cannot be proven safe to move.
  const worktrees = git("worktree", "list", "--porcelain")
  if (worktrees.code !== 0)
    return abort(`could not list worktrees (git exited ${worktrees.code}): ${firstLine(worktrees.stderr)}`)
  const startBranch = currentBranch()
  const mirrorRef = `refs/heads/${mirror}`
  const mirrorElsewhere = worktrees.stdout.split("\n").includes(`branch ${mirrorRef}`) && startBranch !== mirror

  const mirrorSha = commit(mirrorRef)
  const mirrorAction = !mirrorSha
    ? "create"
    : mirrorSha === upSha
      ? "current"
      : isAncestor(mirrorSha, upSha)
        ? "ff"
        : "diverged"
  if (mirrorAction === "diverged")
    return abort(
      `${mirror} is not fast-forwardable to ${upstreamRef} (it has commits not on upstream); refusing to force or merge onto ${mirror}`,
    )
  // update-ref must not move a ref out from under another worktree's HEAD.
  if (mirrorAction === "ff" && mirrorElsewhere)
    return abort(`${mirror} is checked out in another worktree; cannot fast-forward it safely`)

  // --- phase 3: advance the mirror (FF only) and push it ---

  if (mirrorAction === "create") {
    if (git("branch", mirror, upSha).code !== 0)
      return abort(`could not create local ${mirror} mirror at ${upstreamRef}`)
    log(`MIRROR: created ${mirror} at ${short(upSha)}`)
  }
  if (mirrorAction === "ff") {
    // Both paths move to the PINNED upstream commit and refuse anything but the checked
    // fast-forward: --ff-only on a checked-out mirror, compare-and-swap otherwise.
    const moved =
      startBranch === mirror
        ? git("merge", "--ff-only", upSha)
        : git("update-ref", "-m", `sync-upstream: fast-forward ${mirror}`, mirrorRef, upSha, mirrorSha)
    if (moved.code !== 0)
      return abort(`could not fast-forward ${mirror} to ${upstreamRef} (did ${mirror} move concurrently?)`)
    log(`MIRROR: fast-forwarded ${mirror} ${short(mirrorSha)}..${short(upSha)}`)
  }
  if (mirrorAction === "current") log(`MIRROR: ${mirror} already at ${upstreamRef} (${short(upSha)})`)
  const mirrorMoved = mirrorAction !== "current"

  // A failed push does not stop the sync, but it is reported loudly and in the final line.
  const pushNeeded = commit(`refs/remotes/${origin}/${mirror}`) !== upSha
  const pushed = pushNeeded && git("push", origin, `${upSha}:refs/heads/${mirror}`).code === 0
  if (!pushNeeded) log(`MIRROR: ${origin}/${mirror} already current`)
  if (pushed) log(`MIRROR: pushed ${mirror} to ${origin}`)
  if (pushNeeded && !pushed) log(`MIRROR: WARNING could not push ${mirror} to ${origin} - push it by hand`)

  const mirrorState = [
    mirrorMoved ? `the ${mirror} mirror was advanced` : `the ${mirror} mirror was already current`,
    !pushNeeded
      ? ` (${origin} was already current, nothing pushed)`
      : pushed
        ? ` and pushed to ${origin}`
        : ` but the push to ${origin} FAILED (push it by hand)`,
  ].join("")

  // --- phase 4: nothing to merge ---

  if (isAncestor(upSha, baseSha))
    return finish(
      "SYNC_UPTODATE",
      `${base} already contains ${upstreamRef} (${short(upSha)}); nothing to merge, no sync branch created`,
    )

  // --- phase 5: create the sync branch and merge the pinned upstream commit ---

  // Any failure from here is reported by what ACTUALLY changed: SYNC_MERGE_FAILED when the
  // mirror moved or a push landed, otherwise a true nothing-happened SYNC_ABORT. The sync
  // branch is cleaned up first so HEAD is never left on it.
  const fail = (message: string, owned: boolean) => {
    const cleanup = owned
      ? abandonBranch(git, syncBranch, base, baseSha)
      : `the sync branch ${syncBranch} was not created by this run - left untouched`
    return finish(mirrorMoved || pushed ? "SYNC_MERGE_FAILED" : "SYNC_ABORT", `${message} (${cleanup}; ${mirrorState})`)
  }

  // Ownership comes from where HEAD landed, not the exit code: `checkout -b` fails before
  // switching when the name is taken (a concurrent creator owns it), while a failing
  // post-checkout hook exits nonzero after switching (this run owns it).
  const checkout = git("checkout", "-q", "-b", syncBranch, baseSha)
  const owned = currentBranch() === syncBranch
  if (checkout.code !== 0)
    return fail(
      `could not create sync branch ${syncBranch} off ${base} @ ${short(baseSha)}: ${firstLine(checkout.stderr)}`,
      owned,
    )
  log(`SYNC_BRANCH: ${syncBranch} (off ${base} @ ${short(baseSha)})`)

  const mergeBase = git("merge-base", baseSha, upSha).stdout
  const range = mergeBase
    ? [
        `--- upstream range (${short(mergeBase)}..${short(upSha)}) ---`,
        git("log", "--oneline", `${mergeBase}..${upSha}`).stdout,
      ]
    : [`--- upstream range --- (no common merge base with ${upstreamRef})`]

  const merge = git("merge", "--no-ff", "--no-edit", upSha)
  const mergeOutput = [merge.stdout, merge.stderr].filter(Boolean).join("\n") || "<no output>"
  if (merge.code === 0) {
    // Phase 4 proved upstream is not contained, so a successful merge must create a commit;
    // none means the base moved concurrently.
    if (git("rev-parse", "HEAD").stdout === baseSha)
      return fail(
        `merge of ${upSha} reported success but created no commit (base moved concurrently?): ${mergeOutput}`,
        owned,
      )
    range.forEach((line) => log(line))
    const files = mergeBase ? git("diff", "--name-only", mergeBase, upSha).stdout : ""
    log(files ? `--- changed files ---\n${files}` : "--- changed files --- (none)")
    return finish("SYNC_CLEAN", `merged ${upstreamRef} into ${syncBranch} (committed, ready for review)`)
  }

  // A real conflict leaves MERGE_HEAD and at least one unmerged path. Anything else, such
  // as a failing hook, or a classification probe that itself errored, is not a conflict.
  const conflicts = git("diff", "--name-only", "--diff-filter=U")
  if (commit("MERGE_HEAD") && conflicts.code === 0 && conflicts.stdout) {
    log(`--- conflicted files ---\n${conflicts.stdout}`)
    range.forEach((line) => log(line))
    return finish(
      "SYNC_CONFLICTS",
      `merge of ${upstreamRef} into ${syncBranch} left conflicts (merge IN PROGRESS - resolve, commit, then open the sync PR)`,
    )
  }

  git("merge", "--abort")
  return fail(`merge of ${upSha} into ${syncBranch} failed without conflicts: ${mergeOutput}`, owned)
}

// Step off and delete a sync branch this run created. Every outcome is read back from git,
// so the report never claims a cleanup that did not happen.
function abandonBranch(git: Git, branch: string, base: string, baseSha: string) {
  const onBranch = () => git("symbolic-ref", "--quiet", "--short", "HEAD").stdout === branch
  // A failing post-checkout hook exits nonzero even when the switch worked, so re-read HEAD
  // after each attempt instead of trusting the exit code.
  if (onBranch()) git("checkout", "-q", base)
  if (onBranch()) git("checkout", "-q", "--detach", baseSha)
  if (onBranch())
    return `HEAD is STILL on the sync branch ${branch} and it could NOT be abandoned - step off and delete it by hand`
  const exists = () => git("rev-parse", "--verify", "--quiet", `refs/heads/${branch}`).code === 0
  if (!exists()) return `the sync branch ${branch} was never created`
  git("branch", "-q", "-D", branch)
  if (exists()) return `the empty sync branch ${branch} could NOT be deleted - remove it by hand`
  return `deleted the empty sync branch ${branch}`
}

function firstLine(text: string) {
  return (text.split("\n")[0] ?? "").replace(/\s+/g, " ")
}

if (import.meta.main) {
  const usage = "usage: bun script/sync-upstream.ts [--help]"
  const args = process.argv.slice(2)
  if (args[0] === "--help" || args[0] === "-h") {
    console.log(usage)
    process.exit(0)
  }
  if (args.length > 0) {
    console.error(usage)
    process.exit(2)
  }
  const top = localGit(process.cwd())("rev-parse", "--show-toplevel")
  if (top.code !== 0) {
    console.log("SYNC_ABORT: not inside a git repository")
    process.exit(2)
  }
  process.exit(syncUpstream({ git: localGit(top.stdout) }).code)
}
