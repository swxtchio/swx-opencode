# Fork changeset

`swxtch` is upstream [`anomalyco/opencode`](https://github.com/anomalyco/opencode) `dev` plus the changes below.
This file records both halves: every upstream sync merged in, and every change this fork carries on top.

How it stays current:

- **Upstream syncs** are recorded by `bun script/sync-upstream.ts`, which writes the entry inside the sync merge
  itself, naming its `sync-upstream-<UTC timestamp>` branch. Record conflict resolutions in the sync merge commit
  message and summarize them in the entry.
- **Fork changes** are added by the PR that makes them, under the matching area, newest first.
- CI (`script/changeset-check.ts`) fails a PR against `swxtch` that does not add its own entry, compared with the
  base: a `- **#<number>**` item under "Fork changes" for a fork change, or, for a sync PR, the item under "Upstream
  syncs" naming its branch. Entries may wrap onto indented lines. Only Dependabot PRs are exempt.
- When upstream adopts or supersedes a fork change, update its status instead of deleting the entry. That PR still
  adds its own entry, since CI checks for one.

Status: **fork-only** means the change exists only here; **upstreamable** means it fixes upstream's own code and
could be offered back; **partly upstream** means upstream landed an equivalent for part of it.

## Upstream syncs

Newest first. Each entry names the upstream range brought in.

<!-- upstream-syncs: sync-upstream.ts inserts new entries below this line -->

- **2026-09-26** `2406400f0a..696f41bc8e`, 18 upstream commits (sync-upstream-20260926-095526). Conflicts: none.
  Upstream changed the local `setup-bun` action (explicit Bun version, Windows pinned to 1.4.2, the hoisted-linker
  workaround dropped, cache keyed by Bun version); reviewed and its pinned digest in `check-workflow-guards.ts`
  updated.
- **2026-09-22** `d870e22c70..2406400f0a`, 17 upstream commits (PR #38, merge `75eced9fee`). Conflicts:
  `packages/core/src/filesystem/search.ts` took upstream's, since #50439 fixes the same cycle as #2;
  `packages/core/src/npm.ts` kept both import sets; `bun.lock` regenerated.
- **2026-09-20** Fork base: `swxtch` branched from upstream `dev` at `d870e22c70`,
  `fix(stats): retry transient query failures (#50205)`.

## Fork changes

### Fork maintenance and CI

- **#52** `sync-upstream.ts` publishes the `dev` mirror through GitHub's fork sync (`gh repo sync --source
<upstream>`) for a GitHub fork, since the `upstream` ruleset allows only fetch-and-merge on `dev`; the first real
  sync's push was refused. Every destination is read back: a mirror at or past the pinned commit on upstream's own
  history counts as published, and a diverged mirror is refused rather than published onto. _Fork-only._
- **#46** Upstream sync script and this changeset: `script/sync-upstream.ts` fast-forwards the `dev` mirror and prepares
  a `--no-ff` merge for review; `script/changeset-check.ts` keeps this file current; `script/` fork files are now
  typechecked. _Fork-only._
- **#44** `local-release.sh` builds and installs a local binary from a `swxtch` commit, with database continuity
  checks and rollback. _Fork-only._
- **#38** (in the sync PR) Scope the correctness lint gate to fork-authored code, and stop gating merges on upstream's
  e2e suite, whose known flake (#26) this fork does not maintain. _Fork-only._
- **#33** Fail CI when `bun.lock` does not match the manifests; Dependabot's npm ecosystem does not maintain `bun.lock`.
  _Fork-only._
- **#30** The workflow guard check accepts a guard written with its operands reversed. _Fork-only._
- **#22** Guard every inherited workflow to `anomalyco/opencode` and enforce it in CI; a scheduled job was attempting
  daily writes to the upstream repository. _Fork-only._
- **#7** Point `test` and `typecheck` at this fork, on GitHub-hosted Linux runners. _Fork-only._
- **#1** Dependabot: `astro` 5.7.13 to 7.1.1 in `packages/web`. _Fork-only._

### Runtime fixes

- **#80** Codex OAuth accepts complete canonical GPT versions and catalog-backed named variants (including GPT-6 Astra from the [models.dev OpenAI catalog](https://models.dev/api.json)); malformed numeric aliases and unsupported suffixes stay filtered. _Fork-only._
- **#78** Coordinate snapshot maintenance with a box-wide hourly gc cooldown (issue #70 Fix-1) and serialized cleanups; `Snapshot.track()` waits through contention and never silently skips snapshot updates. _Fork-only._
- **#76** Classify mid-stream OpenAI-compatible context overflow errors as `ContextOverflowError` so automatic compaction can resume; cover SDK error shapes and session continuation. The unpatched-router reproduction is recorded in [docs/stream-overflow-e2e.md](docs/stream-overflow-e2e.md). (swxtchio/swx-opencode#66). _Fork-only._
- **#89** Hold framed machine messages until the active turn ends, then run them in admission order; the built-in
  `[fm-level:critical]` token default and unmarked captain prompts remain immediately eligible. Marked `noReply`
  prompts are held only during an active run; idle `noReply` keeps direct-write/no-drain behavior. Add `noReply` to the
  V1 User schema and persist order in `admission_seq` (migration `20260929045002`, a partial unique index on
  `(session_id, admission_seq)` for positive values, and an insert trigger that assigns admission order to legacy
  writers that omit the new column). Pagination orders by admission sequence, emits `{id, seq}` cursors and accepts
  legacy `{id, time}` cursors. _Fork-only._
- **#49** `--effort` works on `opencode`, `--mini` and `attach` (one rule: it applies to whichever model declares
  it, until an in-app choice, and is never saved); an unknown CLI argument is now named after the help; and `run`
  prints the server's real validation error instead of a generic 500. _Fork-only; the unknown-argument message is
  upstreamable._
- **#41** Offer max reasoning variants for GPT-6 Sol and Luna through OpenAI, Azure and compatible gateways.
  _Upstreamable._
- **#31** `run`: fit the turn summary to the terminal width instead of cutting its tail. _Upstreamable._
- **#28** `Npm.install` compares requested versions, not just dependency names, so a changed pin reinstalls.
  _Upstreamable._
- **#27** Plugin compatibility accepts prerelease builds against `engines.opencode` ranges. _Upstreamable._
- **#17** One custom tool file that fails to import no longer kills the prompt with zero parts and no error.
  _Upstreamable._
- **#2** Break the filesystem search import cycle that crashed compiled binaries, and guard the layer-node walk.
  _Partly upstream:_ upstream #50439 fixed the cycle and its version was taken in the 2026-09-22 sync; the
  layer-node guard is fork-only.

### Features

- **#73** V1 server prompt queue core (swxtchio/swx-opencode#68): every V1 prompt except `noReply` is admitted to the durable `session_prompt_queue` table before it becomes a user message. `prompt`, `prompt_async` and `command` take `delivery: "steer" | "queue"`; the default steer reaches the next step as before, while `queue` waits until the run would go idle and then runs as its own turn, one per boundary in admission order. Steers wait behind a pending compaction; an abort or error parks pending prompts until the next admission or wake; `/session/:id/queue` lists, withdraws (404 `QueueItemNotPending` once delivered), restores and re-delivers them, with `session.queue.updated` carrying the full list. A sync prompt answers with the final reply of the turn its prompt joined, recorded for it where the loop ends that turn: steers share the reply of the turn they steered, and a queued prompt gets its own turn's; an editor's withdraw that wins answers it with 409 `PromptWithdrawn`. Deliberately, a compaction whose summary turn stopped on an abort or error is not retried, where upstream V1 retried it on the next prompt with that prompt as its parent (summarising it instead of answering it); if the context is still too large, the next overflow check starts a new compaction. Also closes the V1 lost wakeup where a prompt admitted as a run finished joined it and was never answered. Known ordering-only residuals: a `noReply` prompt and the synthetic message a subtask writes to have its output summarised are stored directly rather than through the queue, so they can land out of admission order relative to pending items. _Fork-only; the lost-wakeup fix is upstreamable._
- **#64** Show each configured router member's request count and share so far in `llmrouter/auto` labels in the TUI and run CLI. (swxtchio/swx-opencode#60). _Fork-only._
- **#58** Price each routed step at its configured serving model and show per-model cost shares in `opencode stats`; `db export-usage` exposes per-served-model token/cost splits and derives record-level tokens, costs and summary `reportedCostTotal` from step-finish parts when present, falling back to message totals under the requested model for legacy rows. (swxtchio/swx-opencode#54). _Fork-only._
- **#34** Export per-session usage as JSONL, verified against the live database. _Fork-only._
- **#29** `--effort` flag, which rejects an unknown value instead of ignoring it. _Upstreamable._
- **#12** The TUI turn footer shows the model that actually served a routed turn. _Fork-only._
- **#6** Record the model the provider reports for each turn, not only the requested route. _Fork-only._

### Tests

- **#32** Scale the e2e per-call timeouts the shared scaling did not cover. _Upstreamable._
- **#25** Environment-sensitive tests attribute their own failures instead of reporting a synthesized timeout.
  _Upstreamable._
