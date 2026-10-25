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

- **#367** Record the 2026-10-25 source-pinned native release `v1.18.32-swxtch.832df553` carrying #358 and #361 (closing #352), with native arm64/x64 candidate probes, real V2 `propose_netlist` execution and completed waits in both jail modes, V1 and manual-compaction controls, the x64 jail coverage limit, the Abbe SessionPool compatibility follow-up, focused tests, downloaded-byte read-back, and committed raw evidence. See `RELEASE-CUT-6-EVIDENCE.md`. _Fork-only._
- **#357** Record the 2026-10-24 source-pinned native release `v1.18.32-swxtch.d7f78881` carrying V2 small-model parity (#354) on top of #350 and #347, with its native arm64/x64 acceptance, Abbe's jail V2 egress rig passing all four modes (including `compact`) on the unpublished arm64 candidate, the x64 rig caveat, focused tests, downloaded-byte verification and committed raw evidence. See `RELEASE-CUT-5-EVIDENCE.md`. _Fork-only._
- **#351** Record the 2026-10-23 source-pinned native release `v1.18.32-swxtch.cd3f0ce2` carrying the V2 plugin-boot gate (#350) on top of #347 for Abbe B-16.10, with its native arm64/x64 acceptance, Abbe's jail V2 egress rig passing on the unpublished arm64 candidate, focused tests, downloaded-byte verification, SaaS wake-log delivery, the Abbe handoff delivered by firstmate's relay with Abbe's acknowledgment and bump-brief citation as read-back, and committed raw evidence. The V2 project-tool gap the rig exposed is #352. See `RELEASE-CUT-4-EVIDENCE.md`. _Fork-only._
- **#349** Record the 2026-10-23 source-pinned native release `v1.18.32-swxtch.774d90c7` carrying the V2 env-config and egress default-model fix (#347) for Abbe B-16.10, with its native arm64/x64 acceptance, focused tests, downloaded-byte verification, Abbe/SaaS wake-log delivery and committed raw evidence. See `RELEASE-CUT-3-EVIDENCE.md`. _Fork-only._
- **#345** Record the 2026-10-22 source-pinned native release carrying independently configured small models (#338, #342), its new native acceptance, focused tests and downloaded-byte verification, and committed raw evidence. SaaS delivery is confirmed; the Abbe artifact handoff is delivered and durable via firstmate, while the direct task wake-log append is firstmate-owned pending with no direct Abbe receipt claimed. See `RELEASE-CUT-2-EVIDENCE.md`. _Fork-only._
- **#329** Harden the fast-suite observer (swxtchio/swx-opencode#324): use Linux procfs/ptrace with Python 3 to capture the loaded interpreter identity and kernel-expanded argv at exec stops, preserving successful launches across temporary cwd cleanup and detecting scripts replaced or symlinks repointed after execution. Preserve job-control stops until SIGCONT using seized tracing and PTRACE_LISTEN. Share position- and value-aware Bun/Node option tables between the shim and audit, refusing unclassified options so values cannot masquerade as worker modes or scripts and user-code arguments cannot masquerade as test flags. Refuse the version-dependent bare Node config-file alias while retaining attached paths. Keep the declared-help check compatible with Node 24 per-alias/optional spellings and explicit refusal of future unclassified Node options; executable regressions bind actual runtime behavior. Refuse incomplete observation, invalidate unobserved Bun tests and Node `--test` launches, and refuse Bun `--bun` delegation even when its caller ignores failure. Correct optional inspector parsing and preserve child-tool flags. Pin non-build prerequisite admission and producer-derived wrong-owner Turbo plans before execution. Qualify public execution-set claims; see `docs/fast-suite-observer.md` and PR #329 for runtime evidence. _Fork-only._
- **#331** Record the 2026-10-21 published linux-x64 V2 admission/wait/native-history/restart/reconnect verification and explicit post-crash join-only/pending controls in an isolated full-system x64 guest, including raw producer evidence, SQLite backup and TCG/JIT qualifications. Hold Abbe cutover for the observed tool-locality mismatch (#1559), V2 replacement proof (#1556) and unestablished cohort inventory. See `docs/verification/native-history-2026-10-21/README.md`. _Fork-only._
- **#326** Add an explicit prepublication native x64 candidate path to `.github/workflows/opencode-arm-release-x64.yml`; the selected source stays clean and unmodified, and the extracted archive runs the native acceptance probe before its bytes and build inputs are retained. Candidate input resolution permits an absent tag only with an explicit source pin and still refuses an existing conflicting tag. _Fork-only._
- **#311** Add a maintained fast test manifest and `tests/run.sh --fast`; validate committed per-package test paths, dispatch each package's exact selected files through one owning Turbo test lifecycle when registered and package scripts otherwise, preserving strict env, package-local config, options, and declared preparation; traverse commandless prerequisites and refuse test-running/non-build prerequisites outside JUnit validation; collect JUnit from test, preparation, and lifecycle launches and require its observed file set to exactly match the manifest for PATH-resolved Bun test launches (scope and follow-up: swxtchio/swx-opencode#324). Bare full execution remains intact; only `--fast` refuses dirty checkouts. _Fork-only._
- **#306** Recognize GLM-5.3 ids in the provider variant family match (swxtchio/swx-opencode#305): config-defined
  GLM-5.3 models exposed no effort variants and were refused by effort-proof consumers; the match now covers
  `glm-5.3|glm-5-3|glm-5p3` alongside 5.2. _Fork-only._
- **#282** Add producer-internal V2 Session wait registration/resolution logs and a bounded native linux-x64 proof workflow. The held-wait battery runs on a same-source instrumented rebuild; a separate downloaded-published-bytes run retains the release archive checksum, version, sidecar and database read-back. _Fork-only._
- **#268** Raise the unit CI step budget from 20 to 30 minutes (swxtchio/swx-opencode#231): the step consumed 1,010s of its
  1,200s limit on the #260 merge head and timed out on the #237 head with every test passing. No check is loosened.
- **#150** The repository-read-only `fff-platform-validation.yml` builds and runs compiled FFF distributions on native Windows x64 and ARM64 runners, computes and checks Nix node_modules hashes on native Linux and macOS runners, retains runner evidence, and builds OpenCode with Nix after each hash matches. _Fork-only._
- **#98** `tests/run.sh` runs the unit suite from the repository root as CI's "Run unit tests" step does
  (`GITHUB_ACTIONS=false bun turbo test`), returning its exit status, and fails when Bun or the installed workspace
  is missing. The root `npm test` still refuses. _Fork-only._
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

### Database maintenance

- **#343** Extend commanded `db prune` to confirmed missing-directory sessions with optional age/session narrowing, per-table inventory and actual deletion/residue counts, offline SQLite exclusion, checkpoint/full-vacuum/final-checkpoint compaction for existing NONE-mode stores, and honest committed-failure reporting without false failure on small deletions. Add `db retire --database ... --directory ... --apply` for external teardown callers, consistent filesystem identity/subdirectory selection gated by the target's own confirmed absence, lexical retention of uncertain in-scope paths, partial-retirement reporting for removed targets, and distinguished zero-change no-ops that skip vacuum. See `docs/dead-session-retirement.md` and the dated verification under `docs/verification/`. _Fork-only._
- **#279** Reuse exited producer-derived stores for prune tests and retain child failure context. _Fork-only._
- **#265** Add `opencode db prune --before <timestamp>` inventory and explicit apply for whole old Sessions. Apply revalidates under its write lock, refuses unknown Session storage or aggregate ownership, and uses incremental auto-vacuum plus a WAL truncate checkpoint. Reports allocated bytes across the database and its sidecars, integrity results, Firstmate's caller-reported measurement state, and the loss of selected raw transcripts. _Fork-only._
- **#148** Set incremental auto-vacuum before creating a fresh store's schema; existing stores keep their auto-vacuum mode and apply pending migrations in WAL mode (swxtchio/swx-opencode#97). _Fork-only._
- **#130** On pinned Linux Bun 1.3.14, the fixture-only compactor clears its source query cache under the exclusive writer fence and reports success only after the replaced inode has no open owner. The held-query fixture pins this release; independent live owners still refuse. `close(true)` also checks SQLite close errors. _Fork-only._
- **#137** Add dormant guarded Session-retention inventory and fixture-only redaction tooling, with fail-closed reads for altered aggregates and an isolated backup, compaction, integrity-check, and restore rehearsal. Real apply and production cutover remain disabled pending captain-approved policy and evidence. _Fork-only._
- **#228** Gate Session-retention apply on strictly more than 7 days of inactivity, registered cross-process writers and holders rescanned inside each batch's write lock. Redaction keeps usage metrics. `/sync/history` now returns the other Sessions' events instead of refusing a request that touches a pruned Session. Pruned Sessions are excluded and listed best-effort as ranges in a 2 KiB `X-Pruned-Sessions` header, which is exposed to CORS clients. Production apply stays refused pending the measurement receipt, customer classification and a captain go (#223, #224, #225, #226). _Fork-only._

### Runtime fixes

- **#350** Run each V2 turn only after its Location's background plugin boot has ended (`PluginInternal.ready`).
  The jail's adapter prompts a new session at once, so its first turn raced the boot that fills the catalog,
  agents and tools: it found no model, could not find the named `swxtch-egress` model, or ran on a bundled one,
  and the egress provider got nothing (swxtchio/swx-abbe#1952 on `v1.18.32-swxtch.774d90c7`). _Fork-only;
  upstream has the same race._
- **#347** Load `OPENCODE_CONFIG` and `OPENCODE_CONFIG_CONTENT` in the V2 core config at V1's precedence (global
  directory, explicit file, project files, `.opencode` directories, inline content last), so V2 sessions in the
  OpenCode jail resolve its config-declared `swxtch-egress` provider and configured default model
  (swxtchio/swx-abbe#1952). With no `model` configured, the V2 catalog default comes, as in V1, from the first
  config-declared provider with an available model, ahead of credential-free catalog models. _Fork-only; upstream
  `43c08387f1` adds the same config sources to its rewritten core config, and a sync bringing it supersedes that
  half. Upstream has no config-provider default preference._
- **#342** Add independently configured V1 small models at global, agent and persisted session scopes (swxtchio/swx-opencode#338), wire titles and compaction summaries to the effective selection, and report unresolvable configured models without a substitute request. See the model settings in `packages/web/src/content/docs/config.mdx`. _Fork-only._
- **#325** Port bounded provider output-limit continuation from `anomalyco/opencode` commit `a3f7f42ac96800aec06fb26365cd482024a52cd1` into V1 execution (swxtchio/swx-opencode#304, output-limit sub-scope). Preserve partial history, tool custody and queue delivery; keep capacity compaction from renewing the allowance and buffer argument fragments until settlement. Expose exhausted continuation through the existing V1 output-length error. _Fork-only V1 port._
- **#314** Add `Ctrl+f` in the pending-prompt review to give only the selected prepared item steer timing, releasing its confirmed review hold only when no other live holder remains. Consolidating a selected human-composer item now combines the complete ready, delivery- and execution-compatible human set in admission order, preserving original inputs, parts, receipts and constituents. `Ctrl+e` enhances the selected pending prompt through the existing model and transcript context, retaining row identity, order, receipts and non-text parts. A late pre-force renewal triggers a holder-status read rather than being discarded, so the view reflects server-applied lease changes after force. _Fork-only._
- **#313** Add a persisted per-project context-percentage auto-compaction threshold in Ctrl+P; defer soft compaction until a true idle boundary, retain terminal structured-output capacity maintenance, and automatically resume compacted work or deliver queued prompts. _Fork-only._
- **#303** Extend the OpenAI-compatible SSE stall deadline to non-empty Responses/Chat generation deltas, including reasoning and tool arguments, so transport-only keepalives cannot hold a stalled attempt open. Preserve the raw-read timeout and classify the bounded failure as `ProviderResponseStreamError` (swxtchio/swx-opencode#302). _Fork-only._
- **#299** Exclude shell-mode rows from Ctrl+r and keep cleared or discarded drafts out of history, stash and prompt recovery. Preserve submitted-history recovery and picker eligibility for ambiguous legacy rows; rename the draft action and keybind to `prompt.discard` and `prompt_discard` (swxtchio/swx-opencode#298). _Fork-only._
- **#294** Rehydrate composer history from each session's full transcript so prompts erased from the cache remain available after resume; fingerprint retained rows so cache eviction invalidates completion and triggers later recovery. Treat unmarked direct and queue-produced prompts as human in UP recall and Ctrl+r search; reject undecodable queue provenance and exclude marked automation from both surfaces. Unmarked pre-#272 history follows Firstmate's ruling (swxtchio/swx-opencode#293). _Fork-only._
- **#276** Add `watcher.fff: false` to disable the FFF background watcher while retaining native file, directory, glob and content search; the index reflects the initial scan when watching is disabled (swxtchio/swx-opencode#274). _Fork-only._
- **#271** Make `DialogQueue` explain locked and no-op results; `SessionQueue.consolidate` groups normalized or prefix-related prompts and compatible command targets only within one delivery class, preserving command steps and receipts (swxtchio/swx-opencode#270). _Fork-only._
- **#266** Settle SDK tool-call custody durably before effects (swxtchio/swx-opencode#234): admitted tool writes are keyed on their execution rather than the raw call ID, so a retry that reuses a call ID can no longer adopt the failed attempt's admitted part; a tool call settles only for the owner that committed its outcome, an earlier outcome's publication is held across a retry's takeover, and V1 tool custody is pinned at its lifecycle boundaries. Cancelling after dispatch leaves no pending orphan. A shell command that ignores SIGTERM is force-killed when its run is aborted or interrupted, even after the call was settled elsewhere and its output write is refused (swxtchio/swx-opencode#267).
- **#264** Keep user `!` shell running snapshots bounded while preserving complete settled output (`SessionPrompt.shellImpl`). _Upstreamable._

- **#262** Route only configured Google Gemini 3.8+ models that migrate from the OpenAI-compatible entry through native Google generation. Translate model and agent effort options, preserve already-native models, and omit the unsupported `minimal` variant (swxtchio/swx-opencode#259). _Fork-only._

- **#260** Write the shell tool's running output at most once per 500 ms instead of once per output chunk
  (swxtchio/swx-opencode#196). Each write is one immediate transaction on the shared database. The live view lags
  by at most the interval, and the settled part keeps its exact output. An abort, timeout or interruption writes the
  unwritten tail first, and output read while the command is killed is written at once, so a part that cleanup
  settles keeps it. The SQLite lock retry window grows from 5 to 10 seconds, and the queue consume's acquisition
  backstop, two windows, from 10 to 20. _Upstreamable._

- **#258** Keep a synchronous V1 prompt that a review hold leaves pending on an existing session waiting for its own
  delivery (swxtchio/swx-opencode#215): it no longer returns the previous turn's reply. After its run the caller
  decides from its own item's `SessionQueue.disposition`, observed after the hold ends by release or lease lapse, and
  gets the reply of the turn that consumed it; an item withdrawn or deleted meanwhile ends it with `PromptWithdrawn`.
  _Fork-only._

- **#244** Repair pending-work starvation, active-turn peer invisibility, history-read stalls, retry identity loss and failure coupling across independent originals. Reconcile represented prompt input before side effects, retain current direct noReply originals for exact retries, and refuse ambiguous historical or executable identity reuse before admission/expansion. Report init identity refusals as BadRequest while fresh init requests still succeed. Use two isolated Bun workers for OpenCode unit files to reduce CI runtime, and bound queue/provider SDK fixtures to their own project configuration. Reserve command identity through unlocked preparation, deferred interruption through actual settlement and short locked admission, preserving unrelated session progress; reject peer transport IDs owned by unrelated projections, and verify provider readiness against an isolated exact workspace SDK fixture. Defer peer continuation while a structured root remains unanswered. Queue/caller custody belongs to `session/queue.ts`, transaction publication to Core Database, and additive lookup/alias migrations to the core generator. See `docs/v1-queue-dispatch.md` and the dated task plan for policy and companion owners. _Fork-only._

- **#202** Recover a consume that meets a held write lock (swxtchio/swx-opencode#167, #180): `SessionQueue.consume` re-attempts a consume whose write never began, up to 5 attempts, each with a 10 s acquisition backstop, then ends the run with `SessionQueueConsumeLockedError` and keeps the queued prompts for the next one. No transaction body is replayed. _Fork-only._
- **#201** Log SQLite write-lock holds and lock-stop call sites, diagnostics only (firstmate task
  opencode-stop-instrument-180, follows #194): an immediate transaction holding the write lock over 250 ms logs its
  pid, purpose and duration; a lock failure leaving the retry gate logs one summary with the statement head, attempts,
  elapsed time and native code; and halt's error line names the failed write's event, stream case and call site. No
  behaviour changes. _Fork-only._
- **#194** Fix dropped accepted prompts under concurrent SQLite writers (swxtchio/swx-opencode#162, #165): queue writes begin with BEGIN IMMEDIATE, so a write waits on the lock instead of failing on a snapshot pinned by an earlier read, and the busy-retry gate has one owner in effect-sqlite-node/src/retry.ts. Queue writes on the consumption path use the same lock, which is beyond #167's scope. _Fork-only._
- **#163** Revert #155 (swxtchio/swx-opencode#153): under concurrent writers it dropped more accepted prompts than its parent #150, so the busy waits return to the pre-#155 schedule. The fix-forward is tracked in swxtchio/swx-opencode#162. _Fork-only._
- **#144** Make `opencode db path` print the selected path without initializing `AppRuntime` or SQLite, defer global directory creation to runtime initialization, and preserve explicit and channel-specific database path selection. _Upstreamable._
- **#141** Reconcile `.git/HEAD` after an early or missed native callback through a shared read/change/publish path; keep a
  closed native watch `unavailable` while polling still delivers changed branch values. Cover root-off switches, native
  error/close, unreadable HEAD recovery and real EventV2 delivery; extend the root-event test bound beyond readiness and
  serial waits. _Upstreamable._
- **#135** Healthy overlapping `project.initGit` reloads carry disposer ownership through successor entries. Promise
  owners are tracked only for work awaited by `InstanceBootstrap.run`, including `Config.get` and plugin
  initialization/loading/config callbacks; concurrent loads join a positively live bootstrap and reload waits for
  that boot before handoff. Unconfirmed producers are refused without poisoning a still-live entry; ordinary runtime
  plugin hooks do not create boot owners. When a worktree boot has not reached `InstanceStore.load`, removal interrupts
  it and waits for its fiber to terminate before disposing the instance. It removes the checkout only after boot
  termination and instance disposal are confirmed; if either cannot settle or disposal is interrupted, removal
  refuses and leaves the checkout in place. `disposeAll` continues across cached directories and reports combined
  cleanup failures. _Fork-only._
- **#130** Carry `activeAssistantMessageID` through legacy busy/retry status events and status-map responses, publish the owner from processor, shell, and direct-subtask producers, and have the TUI keep explicit `null` neutral while falling back to history only when older servers omit the field; historical activity stays inert. _Fork-only._
- **#126** A removed Session stays removed: a write that was already in flight when `Session.remove` ran is now refused instead of quietly recreating that Session's event history, and the HTTP routes that already promise a missing-Session 404 give it in that race (swxtchio/swx-opencode#97). Prompt-like routes and `sync.steal` still need a separate decision (swxtchio/swx-opencode#125). _Fork-only._
- **#123** `SessionRunState.cancel` no longer emits idle without a local runner or retained status, preventing a false completion signal for another process’s active turn (swxtchio/swx-opencode#118). Genuine local cancellation still emits idle. _Fork-only._
- **#122** A failed turn is persisted as failed once SQLite is writable again, even when the lock outlasts every write that would end it (swxtchio/swx-opencode#96). The processor hands a terminal assistant write that loses the lock to a background retry with capped backoff (`terminalRecoverySchedule` and `recoverTerminalMessage` in `packages/opencode/src/session/processor.ts`), which runs until the write commits or fails for another reason, so the row gets its completion and error without a new prompt. The retry is update-only: `Session.updateExistingMessage` checks the row inside the event's own write transaction through a new `EventV2.publish` `precondition`, so an assistant removed while the lock was held is not recreated. While the lock keeps refusing it, the retry logs a rate-limited warning naming the session, message and attempt count, so a stuck recovery stays visible. Only the process that ran the turn writes it, and driver busy waits are unchanged. _Fork-only._
- **#115** `prompt.cancel` returns after interrupting a running shell (swxtchio/swx-opencode#110). Interrupted
  straight after spawn, `Stream.merge`'s output sides never start, so the stdout listener they already attached is
  left with nobody reading; in the measured hang that pipe never ended, and the spawner's release waited forever for
  the child's `close`. The release now destroys the child's pipes after signalling it, so `close` still confirms the
  exit. Seen on Bun 1.3.14. _Upstreamable._
- **#108** A fresh `opencode serve` answers an HTTP request that arrives as its port opens (swxtchio/swx-abbe#441).
  `NodeHttpServer` listened before attaching its request handler, so such a request was read and never answered;
  the server now holds early HTTP requests and hands them to the handler once it attaches. Early WebSocket upgrades
  are not covered. _Upstreamable._
- **#98** A command that exits or closes its stdin before reading all of it no longer raises an uncaught `EPIPE`:
  Bun fails the buffered stdin write after reporting it finished, when nothing is listening, so the spawner now
  keeps a listener on child stdin and leaves the outcome to the exit code. _Upstreamable._
- **#98** A refused inotify instance no longer parks the server thread (swxtchio/swx-opencode#90): the git
  `HEAD` watch uses a non-recursive `fs.watch` that reports the refusal with its errno, and the opt-in root watch
  runs its `@parcel/watcher` subscription in a worker, active only once the worker acknowledges it. Each watch's
  state is on `Watcher.Service.status`, and `OPENCODE_EXPERIMENTAL_WATCHER_SUBSCRIBE_TIMEOUT_MS` sets when an
  unacknowledged root watch is reported unconfirmed. The desktop build ships the worker beside its rebundled server.
  CI runs the real-native refusal and root-status cases, and the desktop built-sidecar check. _Upstreamable._
- **#88** Retry SQLite BUSY/LOCKED statements with a bounded driver schedule and serialized same-connection
  backoff; terminalize failed assistant turns when writes recover, including prelude interrupts and compaction
  setup/cleanup failures; preserve completed answers through later cleanup failures, publish follow-up persistence
  errors without replacing existing provider errors, stop failed overflow turns before compaction, allow explicit
  message-ID retries of terminal failures while retaining their error rows without queue-wake retries, and show idle
  or absent-status sessions with non-terminal assistant rows as failed or unknown, respectively, in the TUI while
  busy/retry statuses remain working. Persist and return only the classified lock diagnosis in messages and HTTP
  errors. _Fork-only._
- **#80** Codex OAuth accepts complete canonical GPT versions and catalog-backed named variants (including GPT-6 Astra from the [models.dev OpenAI catalog](https://models.dev/api.json)); malformed numeric aliases and unsupported suffixes stay filtered. _Fork-only._
- **#78** Coordinate snapshot maintenance with a box-wide hourly gc cooldown (issue #70 Fix-1) and serialized cleanups; `Snapshot.track()` waits through contention and never silently skips snapshot updates. _Fork-only._
- **#76** Classify mid-stream OpenAI-compatible context overflow errors as `ContextOverflowError` so automatic compaction can resume; cover SDK error shapes and session continuation. The unpatched-router reproduction is recorded in [docs/stream-overflow-e2e.md](docs/stream-overflow-e2e.md). (swxtchio/swx-opencode#66). _Fork-only._
- **#89** Hold framed machine messages until the active turn ends, then run them in admission order; the built-in
  `[fm-level:critical]` token default and unmarked captain prompts remain immediately eligible. Fresh idle marked
  `noReply` inputs keep direct-write/no-drain behavior; an idle retry reusing a persisted message ID reconciles to the
  existing row without delivering changed text. Add `noReply` to the
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

### Native FFF distribution

- **#150** Use the root `packageManager` Bun pin for Nix-built OpenCode and desktop binaries (`package.json`,
  `nix/bun.nix`, and `flake.nix`). _Fork-only (swxtchio/swx-opencode#97)._
- **#150** Patch `@ff-labs/fff-bun` (`patches/@ff-labs%2Ffff-bun@0.9.4.patch`) so compiled builds load the
  content-addressed, target-specific FFF sidecar beside the executable; carry the sidecar through build, install,
  postinstall, local-release, Docker, and generated release-package paths. _Fork-only (swxtchio/swx-opencode#97)._
- **#150** Enable native FFF by default in packaged Windows builds (`packages/core/src/flag/flag.ts`), preserve
  `OPENCODE_DISABLE_FFF=1` as the ripgrep fallback, and report the expected sidecar path when loading is unavailable
  (`packages/core/src/filesystem/search.ts`). _Fork-only (swxtchio/swx-opencode#97)._

### Features

- **#361** Load project-local modules from `.opencode/tool` and `.opencode/tools` through V2 Location config before provider tool materialization; register their canonical tools with Location scope and rediscover them when the Location is recreated. See `packages/core/src/tool/project.ts`. _Fork-only._
- **#358** Add global and per-agent V2 small-model selection (swxtchio/swx-opencode#355): preserve global `small_model` in native configuration and V1 migration, load agent-level `small_model` into V2 agents, and resolve auxiliary requests through the auxiliary agent, originating primary agent, session, global selection, title inference, and turn model in V1 order. Extend `Agent.Info` with the generated `smallModel` response field. Focused config, resolver, runner and HTTP regressions include removal proofs; see `specs/v2/session.md` and `specs/v2/schema-changelog.md`. _Fork-only._
- **#354** V2 sessions reach small-model parity with V1 (swxtchio/swx-abbe#1953):
  - `POST /api/session` takes `smallModel`.
  - The V2 runner titles a new session with a new durable `session.next.titled` event.
  - Automatic and overflow summaries run on the session's auxiliary model: the `compaction` agent's model, else the
    session's small model, else the turn's model.
  - `POST /api/session/:id/compact` replaces its 503 with manual compaction. It is exclusive per session and returns
    204, 409 for a session holding a V1 transcript, or 500 when no summary can be produced.
  - The `release-proof` job now overlays its three release-wait core files from `cd3f0ce2`, not from the PR head,
    because their #354 revisions need core surface its release source predates. A later edit to those files' wait
    code is unproven by that job until the pin is bumped; firstmate accepted this narrowing.
  - See `specs/v2/session.md` and `specs/v2/schema-changelog.md`. _Fork-only._
- **#348** Add a configurable `message_window` setting for each session's loaded transcript, applying it consistently to message fetch, hydration reconciliation and part cleanup, and live-update trimming; keep recall-backed search independent of the loaded window and document the default and memory trade-off (swxtchio/swx-opencode#65). _Fork-only._
- **#297** Show the selected Ctrl+r history entry's complete original prompt in a wrapped, page-scrollable preview while
  keeping all other entries as one-line summaries. ENTER still appends the selected original prompt to the existing
  composer draft (swxtchio/swx-opencode#296). _Fork-only._
- **#288** `opencode serve` takes `--questions-command <cmd>` (swxtchio/swx-opencode#286): question choices and
  free text, plan acceptance and permission asks are recorded in the `session_ask` ledger (migration
  `20261018005127`), emitted as JSON to the operator's command, and stay pending until settled once by the new
  `POST /question/:id/answer` and `/permission/:id/answer` routes or a human reply. An answer after a restart
  settles the ask and applies the decision to the stored call in one transaction, then continues the Session; a
  decision the live call received is never reapplied, an answer whose call can never take it closes the ask as
  undelivered, and an answer that cannot be applied yet is a 409, never settled. Prune and retention refuse Sessions
  with an outstanding ask. Off by default; see `docs/supervised-questions.md`. _Fork-only._
- **#287** Repair the composer regressions in `swxtchio/swx-opencode#285`: session-owned recall and retained-history search, append-only picker selection, and invocation-time enhancement model selection. Home-screen drafts without a session owner are no longer retained for later recall. The owning code is `packages/tui/src/prompt/history.tsx`, `packages/tui/src/routes/session/dialog-timeline.tsx`, and `packages/tui/src/component/prompt/index.tsx`; producer-driven regressions are in `packages/tui/test/routes/session/queue-review.test.tsx`. _Fork-only._
- **#272** Search timeline prompts with recognized composer provenance, excluding machine markers and appended text.
  Unknown or unmarked history fails closed; firstmate terminal sends still share `tui-composer` origin until
  `swxtchio/swx-firstmate#2185` adds their machine marker. Busy composer Enter waits for the run's idle boundary and
  batches ordered prompts; explicit steer callers retain their immediate behavior. Ctrl+e enhances an editable draft
  from the last 100 lines of the currently rendered session projection, including visible tool errors, failure
  labels and diagnostics. Set `prompt_enhance_model` at the root of TUI `tui.json` to choose the provider model.
  _Fork-only._
- **#255** A sent prompt is never invisible (swxtchio/swx-opencode#249). `SessionPrompt.prompt` admits a queued
  prompt as its first step, before its config read, as a `time_preparing` row (migration `20261013194721`) that is
  listed and withdrawable but never promoted or merged until its classification, revert cleanup and model selection
  are done, outside the queue lock. Identity reconciliation from #244 runs with that admission, and a retry it matches
  waits for a still-preparing original, then is held to its configured classification before it follows it. Admission
  order is the ascending item id, not the delivery slot a survivor inherits: no row merges past a still-preparing
  one, and when a preparation ends the rows after it are readmitted in that order. A failed
  preparation drops the row and its receipt, and a preparing row a stopped process left is swept, except a row
  withdrawn meanwhile, which keeps its `withdrawn` receipt. The TUI's pending row lists the most recently sent pending prompts, each marked `QUEUED`,
  and shows a prompt still being sent under its `requestID` until any list of the queue carries that request. UP on
  an empty composer withdraws the most recently sent pending prompt, waiting for one still being sent to be admitted
  and following merged receipts to the request that carries it, and puts its text, mentions and attachments back in
  the composer. A prompt promoted first is never copied, and one recall runs at a time through any restore. A prompt
  the composer cannot take is restored once, and the notice reports only what the queue's list or receipt confirms;
  its text goes to the prompt stash only when its receipt confirms it is still withdrawn. Withdrawing an item makes
  its request's receipt `withdrawn` (a new receipt state), restoring makes it `pending` again, and an exact retry of
  a withdrawn request answers `PromptWithdrawn`. Mentions are sent at their offsets in the expanded text.
  _Fork-only._
- **#245** Review and freshen pending queued prompts (swxtchio/swx-opencode#212). The TUI lists a session's pending
  prompts under the prompt box; reviewing them holds promotion (`hold`/`release` in `session/queue.ts`) while they are
  deleted, reordered or consolidated. A pending prompt runs at the session's current model and variant when it is
  delivered, and reaches the model, on the request only, saying when it was queued (provenance timestamps in
  migration `20261012010120`). Prompts take an optional `requestID`: an exact retry admits nothing, a different
  prompt under the same ID is 409 `PromptRequestConflict`, and `GET /session/:id/queue/receipt/:requestID` reports
  `pending`, `delivered`, `merged`, `superseded` or `deleted` (`session_prompt_receipt`, migration
  `20261013020615`, classified by the retention tool). Admission absorbs identical copies, and, when every item names
  a request, merges repeated peer requests, supersedes same-sender `[fm-level:nudge:<key>]` notices and joins verb and
  target continuations; critical messages and replies are never replaced. A waiter on its own prompt no longer
  starts a run on a session a cancel parked. A summary records the variant and effort its request actually sent,
  including when interrupted, without changing what it selects (swxtchio/swx-opencode#227). _Fork-only._
- **#205** Native V2 `SessionV2.wait` and `POST /api/session/:sessionID/wait` return a positive outcome from committed state, either `idle`, `pending`, `completed`, `failed` or `interrupted`, with the highest admitted sequence covered, instead of always answering 503. Wait joins an active drain but never starts or recovers work, still answers 503 when committed state cannot establish the outcome, and attributes a failure only to the work that contained it, using the new optional `Prompted.continuation` marker. An interruption is identified by the new optional assistant error `origin` (swxtchio/swx-opencode#179). _Upstreamable._
- **#161** The TUI collapses each run of consecutive encrypted reasoning parts from OpenAI Responses models, such as GPT-6 Luna and Sol through `swx-azure`, into one `Thought <count> · <duration>` line that updates as each part arrives, in the live stream and in loaded history. A tool call, visible text, or a message error starts a new line; non-encrypted reasoning renders as before (swxtchio/swx-opencode#53). _Upstreamable._
- **#159** Add `skills.exclude`, glob patterns for `SKILL.md` files that skip copies in every skill root, such as hidden per-host installs and `.trash` folders. Patterns match relative to each root, and excluded skills are not loaded, so they cannot shadow the canonical skill of the same name (swxtchio/swx-opencode#59). _Upstreamable._
- **#73** V1 server prompt queue core (swxtchio/swx-opencode#68): every V1 prompt except `noReply` is admitted to the durable `session_prompt_queue` table before it becomes a user message. `prompt`, `prompt_async` and `command` take `delivery: "steer" | "queue"`; the default steer reaches the next step as before, while `queue` waits until the run would go idle and then runs as its own turn, one per boundary in admission order. Steers wait behind a pending compaction; an abort or error parks pending prompts until the next admission or wake; `/session/:id/queue` lists, withdraws (404 `QueueItemNotPending` once delivered), restores and re-delivers them, with `session.queue.updated` carrying the full list. A sync prompt answers with the final reply of the turn its prompt joined, recorded for it where the loop ends that turn: steers share the reply of the turn they steered, and a queued prompt gets its own turn's; an editor's withdraw that wins answers it with 409 `PromptWithdrawn`. Deliberately, a compaction whose summary turn stopped on an abort or error is not retried, where upstream V1 retried it on the next prompt with that prompt as its parent (summarising it instead of answering it); if the context is still too large, the next overflow check starts a new compaction. Also closes the V1 lost wakeup where a prompt admitted as a run finished joined it and was never answered. Known ordering-only residuals: a `noReply` prompt and the synthetic message a subtask writes to have its output summarised are stored directly rather than through the queue, so they can land out of admission order relative to pending items. _Fork-only; the lost-wakeup fix is upstreamable._
- **#64** Show each configured router member's request count and share so far in `llmrouter/auto` labels in the TUI and run CLI. (swxtchio/swx-opencode#60). _Fork-only._
- **#58** Price each routed step at its configured serving model and show per-model cost shares in `opencode stats`; `db export-usage` exposes per-served-model token/cost splits and derives record-level tokens, costs and summary `reportedCostTotal` from step-finish parts when present, falling back to message totals under the requested model for legacy rows. (swxtchio/swx-opencode#54). _Fork-only._
- **#34** Export per-session usage as JSONL, verified against the live database. _Fork-only._
- **#29** `--effort` flag, which rejects an unknown value instead of ignoring it. _Upstreamable._
- **#12** The TUI turn footer shows the model that actually served a routed turn. _Fork-only._
- **#6** Record the model the provider reports for each turn, not only the requested route. _Fork-only._

### Tests

- **#301** Stage the unrelated plugin inside the disposable config owner, replace symlinked install parents
  without following them, and serialize Config's detached SDK install with the fixture request. Require the consumer
  to resolve and use the workspace SDK `tool` export; preserve a linked source surrogate through injected failure
  and child termination. _Fork-only._
- **#115** The declared-schema migration check keeps judging the real `script/migration.ts --check`, now behind a
  180s process-group backstop instead of a 30s test limit that a correct run exceeded under the suite's admitted
  concurrency (swxtchio/swx-opencode#117). A schema drift still fails, and a wedged check fails at the backstop.
  _Fork-only._
- **#115** `routes configured machine_message_markers through prompt admission` waits for the queue to mark the
  critical prompt promoted before asserting the held prompt is the only pending item; it read the queue as soon as
  the message landed, before its row was marked (swxtchio/swx-opencode#114). _Fork-only._
- **#32** Scale the e2e per-call timeouts the shared scaling did not cover. _Upstreamable._
- **#25** Environment-sensitive tests attribute their own failures instead of reporting a synthesized timeout.
  _Upstreamable._
