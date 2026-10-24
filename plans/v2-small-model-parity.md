# V2 small-model parity

Authority: swxtchio/swx-abbe#1953, swxtchio/swx-opencode#355, and the assigned task brief. The V1
behaviour this matches is #342 (`plans/small-model-scopes.md`); the V2 follow-up it closes is "Add
explicit manual compaction" in `specs/v2/session.md`.

## Create parameter

- `POST /api/session` accepts `smallModel: {providerID, id}`, the shape V1 `POST /session` takes.
- `SessionV2.create` records it on the session row's existing `small_model` column through
  `session.created`. `Session.Info` exposes it, so a V1 `PATCH /session/{id}` on a V2 session is read
  back too.

## Auxiliary model selection

`SessionRunnerModel` uses one lazy resolver for titles and summaries. Its order preserves V1's
agent, session, and global choices while retaining title-only inference:

1. The `title` or `compaction` agent's own `model`.
2. That auxiliary agent's `small_model`.
3. The originating primary agent's `small_model`.
4. The session's `smallModel`.
5. The effective global `small_model` from location configuration.
6. For titles only, the catalog's small model for the turn's provider.
7. The turn's own model.

V1 title and compaction paths consult the originating agent's small model before the session and
global selections. A title or compaction agent's `model` override cannot express distinct small
models inherited from different primary agents, so V2 keeps the separate agent-level field.
V1-format configuration migration and native V2 configuration loading both preserve the global and
agent selections without losing other settings or location priority.

Firstmate's ruling for #355 on 2026-10-24 chose V1 agent-scope parity after verifying the V1 title
and compaction call sites in `packages/opencode/src/session/prompt.ts` and
`packages/opencode/src/session/compaction.ts`. V2 therefore carries the originating agent's
`small_model` field between the auxiliary agent override and the session/global selections.

Firstmate revised the round-four rule on 2026-10-24: restore the base's excess-key tolerance at every depth and add only a V2-only top-level guard against V1 migration. Existing V1 markers retain the base predicate; `small_model` acts as a V1 marker only without V2-only top-level keys. Documents with those V2-only keys use native V2 decoding with the base's ignored-excess behavior. The loader adds no new fatal config errors. Compatibility regressions cover base-accepted top-level and nested excess, legacy keys, and both sides of the migration guard. Shared-shape V1/V2 lossiness without V2-only keys is outside this issue; #359 owns it.

A configured selection that does not resolve fails without a substitute, as in V1.
Lower-priority invalid selections do not defeat a usable higher-priority choice. The model is
resolved only when a title or summary is about to be requested, so an unusable auxiliary selection
cannot fail an ordinary turn.

## Runner title

- A drain's first provider turn forks title generation when the session has no parent, keeps its
  default title, and has exactly one user message, and the `title` agent exists. These are V1's
  conditions.
- The request uses the `title` agent's system prompt and the auxiliary model.
- A new durable `session.next.titled` event sets the row's title.
  The V2 session-row events (`agent.switched`, `model.switched`) set a precedent for this.
- The drain awaits its title fiber before it settles, so `wait` returning means the title is
  settled. A title failure is logged and never fails the drain.

## Compaction

- Automatic and overflow compaction summarize on the auxiliary model rather than the turn's model.
  Whether compaction is due is still measured against the turn's model.
- `POST /api/session/{id}/compact` runs one manual compaction through the session's coordinator.
  It never interleaves with a drain, and a wake during it runs after it.
  - When the whole conversation fits the keep budget, it summarizes the whole conversation, as
    V1's manual compaction does.
  - When the summarizer's context window is unknown (a config model with no `limit`, like the jail's
    egress model), it skips the fit check and sends the request.
  - A completed compaction answers 204. A session with no conversation is a 204 no-op.
  - A failed summary or an unresolvable model answers `500 UnknownError` instead of 503.
  - A session holding a V1 transcript, which V2 history never reads, is refused with `409 ConflictError` rather than
    summarized without it (review round 1).
  - An explicit resume requested during a manual compaction starts a drain after it instead of joining it (review
    round 1).

## Proof

- Core config tests cover V1 migration, native V2 loading with `providers`, `agents`, and
  `small_model`, and competing documents at the real loader boundary. Environment tests cover
  config-to-catalog-to-resolver precedence across providers, including distinct primary-agent
  small models and no-substitution for an unavailable selected model.
- Core runner tests cover the selected primary agent on title, automatic, and overflow summaries,
  plus title gates and manual compaction orchestration.
- Opencode HTTP tests use the real server and `TestLLMServer` to observe two primary agents selecting
  different title and manual-summary models, global selection when no session selection exists,
  primary replies staying on their turn models, and the resulting session title and summary.
- Each changed behavior test gets a bounded failure with its production behavior removed, then a
  restored passing run.
- The jail rig (`jail-v2-egress-rig.md`) gets a `v2-compact` mode for abbe, relayed through
  firstmate: compact after a V2 turn must reach the egress provider and return 204.

## Contracts

Regenerate the clients (`packages/client`: `bun run generate`; the legacy SDK script), and add a
`specs/v2/schema-changelog.md` entry, a `specs/v2/session.md` update, and a CHANGESET entry.
