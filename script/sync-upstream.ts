#!/usr/bin/env bun
// Prepare a merge-based sync of this fork with upstream, safely and non-destructively.
//
// Ported from swx-firstmate's bin/fm-sync-upstream.sh. Branch model it assumes and enforces:
//   dev    = a read-only mirror of upstream/dev. Nothing is ever committed to it, so it
//            always fast-forwards to upstream/dev.
//   swxtch = the fork's default branch, holding fork work on top of upstream. The sync
//            merge lands on a fresh branch off origin/swxtch, never on swxtch itself.
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
//   1. Requires a clean tree and the upstream/origin remotes, and fetches both.
//   2. Validates every abort condition before moving or pushing anything.
//   3. Fast-forwards dev to upstream/dev (FF only) and pushes it to origin.
//   4. Stops with SYNC_UPTODATE when origin/swxtch already contains upstream/dev.
//   5. Creates sync-upstream-<UTC timestamp> off origin/swxtch, runs `git merge --no-ff`, and
//      adds the sync's entry to CHANGESET.md inside that same merge.
//
// The base is the freshly fetched origin/swxtch, not local swxtch: the sync PR targets
// origin, and unpushed local commits must not ride into it unreviewed.
//
// It never pushes swxtch, opens a PR, or resolves conflicts. On conflicts the merge is left
// in progress with the CHANGESET.md entry staged, so a resolver keeps git's partial
// auto-merge and records the resolutions in that entry and the commit message. If
// CHANGESET.md itself conflicted, it is left unmerged and the entry is printed for the
// resolver to add after resolving it. Land the
// sync PR with a merge commit, never a squash: squashing drops the upstream/dev parent, so
// the next run would re-merge the same commits into an empty diff.
//
// Report tokens (line-anchored) and exit codes:
//   SYNC_CLEAN         0  merge committed on the sync branch, ready for review
//   SYNC_ABORT         2  no local branch moved and nothing was pushed (a fetch may have
//                         updated remote-tracking refs); fix the cause and re-run, after
//                         any cleanup the report asks for (such as a failed merge --abort)
//   SYNC_CONFLICTS     3  merge left in progress on the sync branch for a resolver
//   SYNC_UPTODATE      4  origin/swxtch already contains upstream/dev; no sync branch
//   SYNC_MERGE_FAILED  5  the mirror moved or was pushed (or may have), then the sync branch could not be
//                         created or the merge could not be prepared and committed
// From the mirror step on, every final line also states what happened to the mirror, and a
// failure states whether cleanup actually left the tree clean.
//
// Every decision reads actual git state. The whole run pins one upstream and one base
// commit, so a concurrent fetch or branch move cannot change what it acts on.

import { readFileSync, writeFileSync } from "node:fs"
import path from "node:path"

export const changesetMarker = "<!-- upstream-syncs:"

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
    // exitCode is null when git could not be spawned at all; treat that as a failure.
    return { code: proc.exitCode ?? 1, stdout: proc.stdout.toString().trim(), stderr: proc.stderr.toString().trim() }
  }
}

export function syncUpstream(options: SyncOptions) {
  const git = options.git
  const log = options.log ?? console.log
  const upstream = options.upstream ?? "upstream"
  const origin = options.origin ?? "origin"
  const mirror = options.mirror ?? "dev"
  const base = options.base ?? "swxtch"
  // One clock read, so the branch name and the changeset date cannot straddle midnight.
  const now = (options.now ?? new Date()).toISOString()
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
  const top = git("rev-parse", "--show-toplevel")
  if (top.code !== 0) return abort("could not determine the repository root")

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

  // No --tags: nothing here uses tags, and a local tag that differs from upstream's would
  // fail the whole fetch.
  const upstreamFetch = git("fetch", upstream)
  if (upstreamFetch.code !== 0) return abort(`git fetch ${upstream} failed: ${firstLine(upstreamFetch.stderr)}`)
  const originFetch = git("fetch", origin)
  if (originFetch.code !== 0) return abort(`git fetch ${origin} failed: ${firstLine(originFetch.stderr)}`)

  const upstreamRef = `refs/remotes/${upstream}/${mirror}`
  const upSha = commit(upstreamRef)
  if (!upSha) return abort(`${upstreamRef} does not exist after fetch (does ${upstream} have ${mirror}?)`)
  const baseRef = `refs/remotes/${origin}/${base}`
  const baseSha = commit(baseRef)
  if (!baseSha) return abort(`${baseRef} does not exist after fetch (does ${origin} have ${base}?)`)

  // --- phase 2: validate every abort condition before any mutation ---

  const syncBranch = `sync-upstream-${now.slice(0, 19).replace(/[-:]/g, "").replace("T", "-")}`
  if (commit(`refs/heads/${syncBranch}`))
    return abort(`sync branch ${syncBranch} already exists (run again in a moment)`)

  const startBranch = currentBranch()
  const startSha = git("rev-parse", "HEAD").stdout
  const mirrorRef = `refs/heads/${mirror}`
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

  // --- phase 3: advance the mirror (FF only) and push it ---

  if (mirrorAction === "create") {
    if (git("branch", mirror, upSha).code !== 0)
      return abort(`could not create local ${mirror} mirror at ${upstreamRef}`)
    log(`MIRROR: created ${mirror} at ${short(upSha)}`)
  }
  if (mirrorAction === "ff") {
    // Both paths move to the PINNED upstream commit as one git operation that refuses
    // anything but a fast-forward. Fetching into the branch also refuses when another
    // worktree has it checked out, with no window between that check and the move.
    const moved =
      startBranch === mirror ? git("merge", "--ff-only", upSha) : git("fetch", "-q", ".", `${upSha}:${mirrorRef}`)
    if (moved.code !== 0)
      return abort(
        `could not fast-forward ${mirror} to ${upstreamRef} (checked out in another worktree, or moved concurrently?): ${firstLine(moved.stderr)}`,
      )
    log(`MIRROR: fast-forwarded ${mirror} ${short(mirrorSha)}..${short(upSha)}`)
  }
  if (mirrorAction === "current") log(`MIRROR: ${mirror} already at ${upstreamRef} (${short(upSha)})`)
  const mirrorMoved = mirrorAction !== "current"

  // A failed push does not stop the sync, since the sync PR carries the same commits, but
  // it is reported here and in the final line. Only the mirror ref is pushed, never tags,
  // and a nonzero exit is read back from the remote: a push can update the ref and still
  // fail, and the report token depends on whether it did.
  const pushMirror = () => {
    if (git("push", "--no-follow-tags", origin, `${upSha}:${mirrorRef}`).code === 0) return "pushed"
    const remote = git("ls-remote", origin, mirrorRef)
    if (remote.code !== 0) return "unknown"
    return remote.stdout.startsWith(upSha) ? "pushed" : "failed"
  }
  const push = commit(`refs/remotes/${origin}/${mirror}`) === upSha ? "current" : pushMirror()
  const mutated = mirrorMoved || push === "pushed" || push === "unknown"
  if (push === "current") log(`MIRROR: ${origin}/${mirror} already current`)
  if (push === "pushed") log(`MIRROR: pushed ${mirror} to ${origin}`)
  if (push === "failed") log(`MIRROR: WARNING could not push ${mirror} to ${origin} - push it by hand`)
  if (push === "unknown")
    log(
      `MIRROR: WARNING the push of ${mirror} to ${origin} failed and whether ${origin}/${mirror} moved could not be read`,
    )

  const mirrorState = [
    mirrorMoved ? `the ${mirror} mirror was advanced` : `the ${mirror} mirror was already current`,
    {
      current: ` (${origin} was already current, nothing pushed)`,
      pushed: ` and pushed to ${origin}`,
      failed: ` but the push to ${origin} FAILED (push it by hand)`,
      unknown: ` but the push to ${origin} failed and whether ${origin}/${mirror} moved is UNKNOWN (check it by hand)`,
    }[push],
  ].join("")

  // --- phase 4: nothing to merge ---

  if (isAncestor(upSha, baseSha))
    return finish(
      "SYNC_UPTODATE",
      `${origin}/${base} already contains ${upstreamRef} (${short(upSha)}); nothing to merge, no sync branch created (${mirrorState})`,
    )

  // --- phase 5: create the sync branch and merge the pinned upstream commit ---

  // Any failure from here is reported by what ACTUALLY changed: SYNC_MERGE_FAILED when the
  // mirror moved or a push landed (or may have), otherwise a true nothing-happened
  // SYNC_ABORT. The sync branch is cleaned up first, and the report says when HEAD could not
  // be moved off it or the tree is not provably clean.
  const failToken = mutated ? "SYNC_MERGE_FAILED" : "SYNC_ABORT"
  const fail = (message: string, owned: boolean) => {
    const cleanup = owned
      ? abandonBranch(git, syncBranch, startBranch, startSha)
      : `the sync branch ${syncBranch} was not created by this run - left untouched`
    const tree = git("status", "--porcelain", "--untracked-files=all")
    const treeState =
      tree.code !== 0
        ? "; whether the working tree is clean could NOT be verified - inspect it before re-running"
        : tree.stdout
          ? "; the working tree is NOT clean - inspect it before re-running"
          : ""
    return finish(failToken, `${message} (${cleanup}${treeState}; ${mirrorState})`)
  }
  // Read the abort back rather than assume it: `merge --abort` is `reset --merge`, which
  // refuses when a file it must restore has unstaged changes. If the merge survives, stay on
  // the sync branch: git would let a checkout carry the staged merge onto another branch,
  // where the next ordinary commit would silently merge unreviewed upstream work.
  const abandonMerge = (message: string, owned: boolean) => {
    const aborted = git("merge", "--abort")
    if (!commit("MERGE_HEAD")) return fail(message, owned)
    return finish(
      failToken,
      `${message}; merge --abort failed (${firstLine(aborted.stderr) || "no output"}), so the merge is STILL in progress on ${syncBranch} - run \`git merge --abort\` there, then delete the branch (${mirrorState})`,
    )
  }

  // Ownership comes from where HEAD landed, not the exit code: `checkout -b` fails before
  // switching when the name is taken (a concurrent creator owns it), while a failing
  // post-checkout hook exits nonzero after switching (this run owns it).
  const checkout = git("checkout", "-q", "-b", syncBranch, baseSha)
  const owned = currentBranch() === syncBranch
  if (checkout.code !== 0)
    return fail(
      `could not create sync branch ${syncBranch} off ${origin}/${base} @ ${short(baseSha)}: ${firstLine(checkout.stderr)}`,
      owned,
    )
  log(`SYNC_BRANCH: ${syncBranch} (off ${origin}/${base} @ ${short(baseSha)})`)

  const mergeBase = git("merge-base", baseSha, upSha).stdout
  const range = mergeBase
    ? [
        `--- upstream range (${short(mergeBase)}..${short(upSha)}) ---`,
        git("log", "--oneline", `${mergeBase}..${upSha}`).stdout,
      ]
    : [`--- upstream range --- (no common merge base with ${upstreamRef})`]

  // Merge without committing, so the changeset entry lands inside the sync merge itself.
  const merge = git("merge", "--no-ff", "--no-commit", upSha)
  const mergeOutput = [merge.stdout, merge.stderr].filter(Boolean).join("\n") || "<no output>"
  // A real conflict leaves MERGE_HEAD and at least one unmerged path. Anything else, such
  // as a failing hook, or a classification probe that itself errored, is not a conflict.
  const inProgress = !!commit("MERGE_HEAD")
  const conflicts = git("diff", "--name-only", "--diff-filter=U")
  const conflicted = merge.code !== 0 && inProgress && conflicts.code === 0 && !!conflicts.stdout
  // Phase 4 proved upstream is not contained, so a clean merge must leave one to commit;
  // none means the base moved concurrently.
  if (merge.code === 0 && !inProgress)
    return fail(`merge of ${upSha} reported success but left nothing to commit (base moved concurrently?)`, owned)
  if (merge.code !== 0 && !conflicted)
    return abandonMerge(`merge of ${upSha} into ${syncBranch} failed without conflicts: ${mergeOutput}`, owned)

  const count = mergeBase ? git("rev-list", "--count", `${mergeBase}..${upSha}`).stdout : "?"
  const conflictedFiles = conflicted ? conflicts.stdout.split("\n") : []
  const recorded = recordSync(git, top.stdout, {
    date: now.slice(0, 10),
    range: `${mergeBase ? short(mergeBase) : "?"}..${short(upSha)}`,
    count,
    branch: syncBranch,
    conflicts: conflictedFiles,
  })
  if (!recorded.ok) return abandonMerge(recorded.message, owned)
  log(recorded.message)

  // An explicit message, since merging a bare sha would default to "Merge commit '<sha>'".
  // Written to MERGE_MSG so a resolver's `git commit` after conflicts picks it up too.
  const mergeMsg = git("rev-parse", "--path-format=absolute", "--git-path", "MERGE_MSG")
  const message = `chore: sync ${base} with ${upstream}/${mirror} at ${short(upSha)}\n\nBrings in ${count} upstream commits.\n`
  if (mergeMsg.code !== 0 || !attempt(() => writeFileSync(mergeMsg.stdout, message)))
    return abandonMerge(`could not write the merge message to ${mergeMsg.stdout || "MERGE_MSG"}`, owned)

  if (conflicted) {
    log(`--- conflicted files ---\n${conflicts.stdout}`)
    range.forEach((line) => log(line))
    return finish(
      "SYNC_CONFLICTS",
      `merge of ${upstreamRef} into ${syncBranch} left conflicts (merge IN PROGRESS - resolve, record the resolutions in CHANGESET.md and the commit message, commit, then open the sync PR; ${mirrorState})`,
    )
  }

  const committed = git("commit", "--no-edit")
  if (committed.code !== 0)
    return abandonMerge(
      `committing the merge of ${upSha} into ${syncBranch} failed: ${[committed.stdout, committed.stderr].filter(Boolean).join("\n") || "<no output>"}`,
      owned,
    )
  range.forEach((line) => log(line))
  const changed = mergeBase ? git("diff", "--name-only", mergeBase, upSha).stdout : ""
  log(changed ? `--- changed files ---\n${changed}` : "--- changed files --- (none)")
  return finish("SYNC_CLEAN", `merged ${upstreamRef} into ${syncBranch} (committed, ready for review; ${mirrorState})`)
}

// Add this sync's entry to CHANGESET.md and stage it. The entry names the sync branch, which
// is how script/changeset-check.ts confirms a sync PR recorded itself.
function recordSync(
  git: Git,
  root: string,
  entry: { date: string; range: string; count: string; branch: string; conflicts: string[] },
) {
  const conflicts = entry.conflicts.length
    ? `Conflicts: ${entry.conflicts.map((name) => `\`${name}\``).join(", ")} - record how each was resolved.`
    : "Conflicts: none."
  const line = `- **${entry.date}** \`${entry.range}\`, ${entry.count} upstream ${entry.count === "1" ? "commit" : "commits"} (${entry.branch}). ${conflicts}`
  // Writing into a conflicted CHANGESET.md and staging it would mark the conflict resolved
  // with its markers still in the file. Leave it unmerged and hand the entry to the resolver.
  if (entry.conflicts.includes("CHANGESET.md"))
    return {
      ok: true,
      message: `CHANGESET: WARNING CHANGESET.md itself conflicted; after resolving it, add this entry below the "${changesetMarker}" line:\n${line}`,
    }
  const file = path.join(root, "CHANGESET.md")
  const read = attempt(() => readFileSync(file, "utf8"))
  if (!read) return { ok: false, message: `could not read ${file} to record the sync` }
  const text = read.value
  const marker = text.indexOf(changesetMarker)
  if (marker === -1) return { ok: false, message: `no "${changesetMarker}" line in ${file} to record the sync under` }
  const eol = text.indexOf("\n", marker)
  const rest = eol === -1 ? "" : text.slice(eol).replace(/^\n+/, "")
  const updated = `${text.slice(0, eol === -1 ? text.length : eol)}\n\n${line}\n${rest.startsWith("- ") || !rest ? "" : "\n"}${rest}`
  if (!attempt(() => writeFileSync(file, updated))) return { ok: false, message: `could not write ${file}` }
  // Put the file back from the index when staging fails, so `merge --abort` has no unstaged
  // change to trip over.
  if (git("add", "--", file).code !== 0) {
    git("checkout", "--", file)
    return { ok: false, message: `could not stage ${file}` }
  }
  return { ok: true, message: `CHANGESET: recorded ${entry.range}` }
}

// Step off and delete a sync branch this run created, returning to where the run started.
// Every outcome is read back from git, so the report never claims a cleanup that did not
// happen.
function abandonBranch(git: Git, branch: string, startBranch: string, startSha: string) {
  const onBranch = () => git("symbolic-ref", "--quiet", "--short", "HEAD").stdout === branch
  // A failing post-checkout hook exits nonzero even when the switch worked, so re-read HEAD
  // after each attempt instead of trusting the exit code.
  if (onBranch() && startBranch) git("checkout", "-q", startBranch)
  if (onBranch()) git("checkout", "-q", "--detach", startSha)
  if (onBranch())
    return `HEAD is STILL on the sync branch ${branch} and it could NOT be abandoned - step off and delete it by hand`
  const exists = () => git("rev-parse", "--verify", "--quiet", `refs/heads/${branch}`).code === 0
  if (!exists()) return `the sync branch ${branch} was never created`
  git("branch", "-q", "-D", branch)
  if (exists()) return `the empty sync branch ${branch} could NOT be deleted - remove it by hand`
  return `deleted the empty sync branch ${branch}`
}

// The only filesystem calls. A failure has to reach the caller's cleanup and report token
// instead of throwing past them mid-merge.
function attempt<T>(action: () => T) {
  try {
    return { value: action() }
  } catch {
    return undefined
  }
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
