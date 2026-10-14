# Plan: a held sync prompt on an existing session waits for its own reply (swxtchio/swx-opencode#215)

## Contract (decided before implementation)

A reply-producing synchronous V1 prompt that a review hold keeps pending stays admitted, and its
caller waits. This holds for the default steer and an explicit `delivery: "queue"`, on an existing
session exactly as on a new one. A turn that completed before the prompt was delivered is not its
answer. When the last hold ends, by release or by its lease lapsing, the prompt is delivered and
the caller receives the final reply of the turn that consumed it. If the item is withdrawn or
deleted while held, the caller ends with `PromptWithdrawn`, as a new session's held first prompt
already does.

## Findings (source, base a5e9d35dbc)

- `SessionPrompt.prompt` admits the prompt and calls `drain(sessionID, own)`.
- On a new session `drain` waits in `queue.released(sessionID)` before the first promotion, so the
  caller waits out the hold.
- On an existing session `drain` skips that branch and joins `state.ensureRunning`. The run cannot
  promote a held row, so it ends at once and yields the last assistant message, the previous turn's
  reply. `queue.awaitingDrain` deliberately ignores held pending rows, so `waiting` is false and the
  own-unread re-drain never fires. `queue.reply(own)` is empty, so the caller gets `result`: the
  previous reply, while its prompt is still pending.

## Change

1. `queue.ts`: `disposition(sessionID, itemID)` reports what became of a waiting caller's own item:
   `answered` (its turn recorded a reply), `delivered` (it became a message whose turn has not), or
   `pending`. A pending item first waits out any live hold (`released`: release or lapse), then is
   observed again. It fails with `WithdrawnError` for an item withdrawn or deleted without becoming a
   message. The hold checks in `reserve`/`commit` and the `awaitingDrain` exclusion of held rows stay
   as they are.
2. `prompt.ts` `drain`: after the joined run, and after any background drain for other work, the caller
   drains again when its item is `pending`, or `delivered` while a run is busy. It observes busy after
   the hold barrier, so the re-drain joins the release's own drain.

Round 1 (review at `c98afefdee`) replaced the first version, `awaitHeld`. That version read "no hold
is live now" as "nothing holds this item back". A hold that ended after the caller's
`awaitingDrain`/idle checks therefore still returned the previous reply. That happened both before the
release's promotion committed and after, while the delivered turn was still with the provider. The
caller now decides from its item's state observed after the barrier, never from whether a hold is live.
The one change outside holds: an item deleted while its caller waits now ends the caller with
`PromptWithdrawn`, as a withdrawn one already did. It no longer returns the run's final message.

## Regression

All of these are in `test/server/httpapi-session.test.ts`, over the production HTTP routes and a real
provider server, on an existing session whose prior turn answered "prior done". Each runs once with
the default steer and once with `delivery: "queue"`:

- **Release.** While held, the sync request stays unresolved. Its item is listed, the model sees none
  of it, and the provider call count is unchanged. After release, the response is the new turn's reply.
- **Lease lapse.** The routes run on TestClock (`itClocked`). The request is still pending 5 test
  minutes in, and gets the new turn's reply once the lease lapses at 6. No release route is called.
- **Hold ending during the caller's checks.** This uses `itRacing`/`spanHold`. The caller is held
  after `awaitingDrain` and after its idle check. The release ends the hold, and then the caller
  decides. One ordering holds the release's promotion before it commits; the other lets it commit and
  holds the provider. In both, the request stays unresolved and gets its own turn's reply.

`test/session/queue-review.test.ts` covers `disposition` directly: it waits through a renewed lease and
returns `pending` at lapse, and fails `WithdrawnError` for an item deleted while held.

Red proofs:
- Release, against the pre-fix base: the release test fails.
- Hold ending during the caller's checks, against `c98afefdee`: it fails at both orderings.
- Hold ending during the caller's checks, with the `delivered && busy` re-drain removed: the
  after-commit ordering fails.
- Lease lapse, against the base and with the `drain` hook removed: the lapse test fails.
