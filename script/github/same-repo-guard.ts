/**
 * These maintenance scripts carry a hardcoded default target of
 * `anomalyco/opencode`, so a fork that inherits them will, on its own cron,
 * attempt to comment on and close the UPSTREAM repository's issues and pull
 * requests. That has happened here: close-issues reported
 * `Failed to comment #38226: 403 Forbidden` from this fork, an issue number
 * that exists only upstream. It failed for lack of token scope, not because
 * anything stopped it.
 *
 * The invariant this enforces is narrower and more durable than "is this the
 * upstream repository": a script may only write to the repository it is
 * running in. That holds upstream (where target and GITHUB_REPOSITORY agree,
 * so behaviour is unchanged), holds for any future fork, and does not depend
 * on a hardcoded organisation name.
 *
 * Fails closed: with GITHUB_REPOSITORY unset - a local invocation by hand -
 * the target cannot be confirmed, so the write is refused rather than aimed
 * at whatever the default happens to be.
 */

export type RepoGuardResult = { ok: true } | { ok: false; reason: string }

export function checkSameRepository(target: string, env: Record<string, string | undefined>): RepoGuardResult {
  const actual = env.GITHUB_REPOSITORY?.trim()

  if (!actual)
    return {
      ok: false,
      reason:
        `refusing to act on ${target}: GITHUB_REPOSITORY is not set, so the target cannot be confirmed. ` +
        `Set GITHUB_REPOSITORY=${target} to run this deliberately.`,
    }

  if (actual !== target)
    return {
      ok: false,
      reason:
        `refusing to act on ${target} while running in ${actual}. ` +
        `These scripts may only write to the repository they run in.`,
    }

  return { ok: true }
}

/**
 * Exits 0 on refusal, not 1: a fork skipping inherited maintenance is the
 * correct outcome, not a build failure to be investigated every night.
 */
export function requireSameRepository(target: string, env: Record<string, string | undefined> = process.env): void {
  const result = checkSameRepository(target, env)
  if (result.ok) return
  console.log(result.reason)
  process.exit(0)
}
