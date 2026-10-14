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

1. `queue.ts`: add `awaitHeld(sessionID, itemID)`. If the session has a live hold and the item has not
   become a message, it waits on `released` (release or lapse) and returns true. If the item has
   meanwhile left the queue without becoming a message (deleted or withdrawn), it fails with
   `WithdrawnError`. Otherwise it returns false at once. The hold checks in `reserve`/`commit` and the
   `awaitingDrain` exclusion of held rows stay as they are.
2. `prompt.ts` `drain`: after the joined run, and after any background drain for other work, a caller
   whose own item `awaitHeld` held back drains again. That re-drain delivers its prompt and returns
   that turn's reply.

## Regression

- `test/server/httpapi-session.test.ts`, beside the existing review-hold test: an existing session
  with a completed turn ("prior done"). Under a hold, a sync `POST /session/:id/message` default steer
  is pending and its request unresolved. The item is listed, the model sees no part of it, and the
  provider call count is unchanged. On release the item is delivered and the response is the new
  turn's reply, not "prior done". The same again with `delivery: "queue"`.
- `test/session/queue-review.test.ts`, next to the existing lease test, uses TestClock because lapse
  cannot be reached over live HTTP within a test's run: `awaitHeld` stays blocked under a renewed
  lease and returns when the lease lapses. It fails `WithdrawnError` for an item deleted while held.
  It returns false at once with no hold.
- Red proof: with `awaitHeld` made to return false, both new tests fail.
