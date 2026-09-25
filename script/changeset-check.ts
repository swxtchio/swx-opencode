#!/usr/bin/env bun
// Fail a pull request against swxtch that does not record itself in CHANGESET.md.
//
// CHANGESET.md records what this fork carries on top of upstream. It only stays true if
// the PR making a change records it, so CI requires the PR to ADD its entry, compared with
// the base, in the section it belongs to:
//   - a fork change needs a `- **#<number>**` item under "## Fork changes";
//   - a sync branch (sync-upstream-<UTC timestamp>) needs a `- **<date>** ... (<branch>)` item
//     under "## Upstream syncs", which sync-upstream.ts writes inside the sync merge.
// There is no name-based exemption: a branch that only looks like a sync still has to add a
// sync entry, and reusing an old sync's branch name fails because the base already has it.
// Dependabot is exempt; its author login cannot be forged.
//
// This is a guard against forgetting, not against a hostile author: like every check in
// this CI, it runs the PR's own copy of this file, so an edit to it shows in the reviewed
// diff rather than being prevented.
//
// Reads the PR from PR_NUMBER, PR_AUTHOR, PR_HEAD_REF, PR_BASE_REF and PR_BASE_SHA; with no
// PR_NUMBER (a push or a manual run) there is nothing to check.

import path from "node:path"
import { localGit, type Git } from "./sync-upstream"

export type PullRequest = { number?: string; author?: string; head?: string; base?: string }

export function checkChangeset(doc: string | undefined, pr: PullRequest, baseDoc: string | undefined) {
  const number = pr.number?.trim()
  if (!number) return { ok: true, reason: "not a pull request" }
  if (pr.base !== "swxtch") return { ok: true, reason: `#${number} does not target swxtch` }
  if (pr.author === "dependabot[bot]") return { ok: true, reason: `#${number} is a Dependabot update` }
  if (!/^\d+$/.test(number)) return { ok: false, reason: `PR_NUMBER is not a number: ${number}` }
  if (doc === undefined) return { ok: false, reason: "CHANGESET.md does not exist at the repository root" }
  if (baseDoc === undefined) return { ok: false, reason: "could not read CHANGESET.md from the PR's base" }
  const head = pr.head ?? ""
  // The head name is only interpolated once it matches this pattern, so it is regex-safe.
  const sync = /^sync-upstream-\d{8}-\d{6}$/.test(head)
  const section = sync ? "Upstream syncs" : "Fork changes"
  const entry = sync
    ? new RegExp(`^\\*\\*\\d{4}-\\d{2}-\\d{2}\\*\\* .*\\(${head}\\)`)
    : new RegExp(`^\\*\\*#${number}\\*\\*`)
  const recorded = (text: string) => listItems(text, section).some((item) => entry.test(item))
  if (recorded(doc) && !recorded(baseDoc))
    return { ok: true, reason: sync ? `the ${head} sync is recorded` : `#${number} is recorded` }
  if (recorded(baseDoc))
    return {
      ok: false,
      reason: `the base already records ${sync ? head : `#${number}`}; this PR must add its own entry`,
    }
  if (sync)
    return {
      ok: false,
      reason:
        `CHANGESET.md has no entry under "## Upstream syncs" naming ${head}. sync-upstream.ts writes it inside the ` +
        `sync merge, or prints it when CHANGESET.md itself conflicted; add that entry.`,
    }
  return {
    ok: false,
    reason:
      `CHANGESET.md has no entry for #${number} under "## Fork changes". Add one under the matching area, newest ` +
      `first: - **#${number}** <what changed and why>. _Fork-only._`,
  }
}

// The list items under a `## <section>` heading, each joined with its indented continuation
// lines, so a wrapped entry matches the same as a one-line one.
function listItems(doc: string, section: string) {
  const lines = doc.split("\n")
  const start = lines.findIndex((line) => line.trim() === `## ${section}`)
  if (start === -1) return []
  const end = lines.findIndex((line, index) => index > start && line.startsWith("## "))
  return lines.slice(start + 1, end === -1 ? undefined : end).reduce<string[]>((items, line) => {
    if (line.startsWith("- ")) return [...items, line.slice(2).trim()]
    if (items.length > 0 && /^\s+\S/.test(line)) return [...items.slice(0, -1), `${items.at(-1)} ${line.trim()}`]
    return items
  }, [])
}

// CHANGESET.md at the base commit: "" when the base has no such file, undefined when the base
// itself cannot be read. CI checks out depth 1, so the base commit is fetched first.
export function readBaseChangeset(git: Git, sha: string) {
  if (!/^[0-9a-f]{40}$/.test(sha)) return undefined
  if (git("cat-file", "-e", `${sha}^{commit}`).code !== 0 && git("fetch", "-q", "--depth=1", "origin", sha).code !== 0)
    return undefined
  if (git("cat-file", "-e", `${sha}^{commit}`).code !== 0) return undefined
  const file = git("show", `${sha}:CHANGESET.md`)
  return file.code === 0 ? file.stdout : ""
}

if (import.meta.main) {
  const root = path.join(import.meta.dir, "..")
  const file = Bun.file(path.join(root, "CHANGESET.md"))
  const number = process.env.PR_NUMBER
  const result = checkChangeset(
    (await file.exists()) ? await file.text() : undefined,
    { number, author: process.env.PR_AUTHOR, head: process.env.PR_HEAD_REF, base: process.env.PR_BASE_REF },
    number?.trim() ? readBaseChangeset(localGit(root), process.env.PR_BASE_SHA ?? "") : "",
  )
  console.log(`${result.ok ? "ok" : "FAIL"}: ${result.reason}`)
  process.exit(result.ok ? 0 : 1)
}
