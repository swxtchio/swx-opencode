# V2 small-model parity

Authority: swxtchio/swx-abbe#1953 and the assigned task brief. The V1 behaviour this matches is #342
(`plans/small-model-scopes.md`); the V2 follow-up it closes is "Add explicit manual compaction" in
`specs/v2/session.md`.

## Create parameter

- `POST /api/session` accepts `smallModel: {providerID, id}`, the shape V1 `POST /session` takes.
- `SessionV2.create` records it on the session row's existing `small_model` column through
  `session.created`. `Session.Info` exposes it, so a V1 `PATCH /session/{id}` on a V2 session is read
  back too.

## Auxiliary model selection

`SessionRunnerModel` gains one resolver for title and compaction models, in V1's order:
1. the `title` or `compaction` agent's own `model`;
2. the session's `smallModel`;
3. for titles only, the catalog's small model for the turn's provider;
4. the turn's own model.

A configured selection that does not resolve fails without a substitute, as in V1.
The model is resolved only when a title or summary is about to be requested, so a bad small model
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
  - A failed summary or an unresolvable model answers a named error instead of 503.

## Proof

- Core runner tests cover the title on the auxiliary model, the title gates, the session small model
  for automatic compaction, and manual compaction (completed, fits-whole, failure, serialized behind
  an active drain).
- The opencode HTTP tests cover the real server against `TestLLMServer`: create with `smallModel`,
  compact reaching the provider and returning 204, and title and summary requests naming the small
  model.
- Each new test gets a demonstrated red with its behaviour reverted.
- The jail rig (`jail-v2-egress-rig.md`) gets a `v2-compact` mode for abbe, relayed through
  firstmate: compact after a V2 turn must reach the egress provider and return 204.

## Contracts

Regenerate the clients (`packages/client`: `bun run generate`; the legacy SDK script), and add a
`specs/v2/schema-changelog.md` entry, a `specs/v2/session.md` update, and a CHANGESET entry.
