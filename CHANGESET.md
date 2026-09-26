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

- **#34** Export per-session usage as JSONL, verified against the live database. _Fork-only._
- **#29** `--effort` flag, which rejects an unknown value instead of ignoring it. _Upstreamable._
- **#12** The TUI turn footer shows the model that actually served a routed turn. _Fork-only._
- **#6** Record the model the provider reports for each turn, not only the requested route. _Fork-only._

### Tests

- **#32** Scale the e2e per-call timeouts the shared scaling did not cover. _Upstreamable._
- **#25** Environment-sensitive tests attribute their own failures instead of reporting a synthesized timeout.
  _Upstreamable._
