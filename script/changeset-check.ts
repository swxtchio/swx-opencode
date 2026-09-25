#!/usr/bin/env bun
// Fail a pull request against swxtch that does not record itself in CHANGESET.md.
//
// CHANGESET.md records what this fork carries on top of upstream. It only stays true if
// the PR making a change records it, so CI requires the PR's entry:
//   - a fork change needs `- **#<number>**` under "Fork changes";
//   - a sync branch (sync-upstream-<UTC timestamp>) needs the sync entry naming that branch,
//     which sync-upstream.ts writes inside the sync merge.
// There is no name-based exemption: a branch that only looks like a sync still has to carry
// a sync entry. Dependabot is exempt, because its author login cannot be forged.
//
// Reads the PR from PR_NUMBER, PR_AUTHOR, PR_HEAD_REF and PR_BASE_REF; with no PR_NUMBER
// (a push or a manual run) there is nothing to check.

import path from "node:path"

export type PullRequest = { number?: string; author?: string; head?: string; base?: string }

export function checkChangeset(doc: string | undefined, pr: PullRequest) {
  const number = pr.number?.trim()
  if (!number) return { ok: true, reason: "not a pull request" }
  if (pr.base !== "swxtch") return { ok: true, reason: `#${number} does not target swxtch` }
  if (pr.author === "dependabot[bot]") return { ok: true, reason: `#${number} is a Dependabot update` }
  if (!/^\d+$/.test(number)) return { ok: false, reason: `PR_NUMBER is not a number: ${number}` }
  if (doc === undefined) return { ok: false, reason: "CHANGESET.md does not exist at the repository root" }
  const head = pr.head ?? ""
  if (/^sync-upstream-\d{8}-\d{6}$/.test(head)) {
    if (new RegExp(`^- \\*\\*\\d{4}-\\d{2}-\\d{2}\\*\\* .*\\(${head}\\)`, "m").test(doc))
      return { ok: true, reason: `the ${head} sync is recorded` }
    return {
      ok: false,
      reason: `CHANGESET.md has no sync entry naming ${head}. sync-upstream.ts writes it inside the sync merge; restore it rather than hand-writing one.`,
    }
  }
  if (new RegExp(`^- \\*\\*#${number}\\*\\*`, "m").test(doc)) return { ok: true, reason: `#${number} is recorded` }
  return {
    ok: false,
    reason:
      `CHANGESET.md has no entry for #${number}. Add one under the matching area in "Fork changes", newest ` +
      `first: - **#${number}** <what changed and why>. _Fork-only._`,
  }
}

if (import.meta.main) {
  const file = Bun.file(path.join(import.meta.dir, "..", "CHANGESET.md"))
  const result = checkChangeset((await file.exists()) ? await file.text() : undefined, {
    number: process.env.PR_NUMBER,
    author: process.env.PR_AUTHOR,
    head: process.env.PR_HEAD_REF,
    base: process.env.PR_BASE_REF,
  })
  console.log(`${result.ok ? "ok" : "FAIL"}: ${result.reason}`)
  process.exit(result.ok ? 0 : 1)
}
