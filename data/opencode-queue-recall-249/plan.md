# Plan: submission-time queue visibility and UP recall (swxtchio/swx-opencode#249)

## Findings (source, base 529ed53056)

- Server admission already precedes preparation. `SessionPrompt.prompt` calls `queue.request`
  (which publishes `session.queue.updated` with the new row) before `queue.promote` reserves and
  prepares it, and a reserved row stays in `SessionQueue.list` until `commit` marks it promoted.
  Only session lookup, revert cleanup, config, the receipt check and the model selection run first.
  Existing prompt tests already cover the custody races: a withdraw wins over a promotion still
  preparing, and a delivered row can no longer be withdrawn.
- The invisible window is in the TUI. `submitInner` clears the composer as soon as it starts the
  POST, and the only pending surface, `PromptQueueRow`, renders a count ("N pending · …"), never the
  prompt itself. So a submitted prompt has no visible text from the moment Enter clears the composer
  until it becomes a message: the POST round trip, any pre-admission wait, and every second it waits
  as a pending row (a steer blocked by compaction or a review hold, an explicit queue waiting for idle,
  or a slow preparation).
- UP (`prompt.history.previous`) on an empty composer only walks history: it copies text without
  taking custody, so resending a pending prompt duplicates it.

## Change (TUI only; no server or public API change)

1. Pending surface lists the prompts. `PromptQueueRow` renders each pending prompt's text with a
   `QUEUED` badge (the most recent few, nearest the composer), above the existing count and review
   hint. QUEUED marks pending custody only; steer/queue counts stay as they are.
2. No gap before admission. At submit the TUI records the prompt under the `requestID` it already
   sends, in sync state for its session, and the pending surface shows it as QUEUED at once. The
   entry is dropped when a `session.queue.updated` list carries that request (as the item's request,
   an origin or a repeat, so merges count), or when the POST settles. The server item takes over in
   place, so the prompt is never shown twice and never shown nowhere.
3. UP recall. With an empty composer in normal mode and at least one pending prompt shown for the
   session, UP recalls the last one shown instead of walking history:
   - a server item is withdrawn through `session.queue.withdraw`; only a successful withdrawal fills
     the composer, from the withdrawn item's non-synthetic text and its file/agent parts (no queue
     annotation, no editor context);
   - a refused withdrawal (already promoted, delivered or gone) leaves the composer empty and shows a
     brief notice: no copy, no second send;
   - if the composer gained text or the prompt left this session while the withdrawal was in flight,
     the item is restored to the queue rather than dropped or copied;
   - an entry not yet admitted cannot be withdrawn, so UP leaves it queued with a notice;
   - repeated UP while a recall is in flight does nothing.
   Resending recalled text is an ordinary new send with a new `requestID`. The withdrawn original's
   POST answers 409 PromptWithdrawn; the TUI no longer reports that as a send failure.
4. Commands (`/cmd`) and shell mode keep today's path: a command is expanded before it is admitted
   and names no request, so it cannot be confirmed by receipt. Filed as a follow-up rather than
   widening this change into the command-expansion path that swxtchio/swx-opencode#244 reworks.

## Tests

- `packages/tui/test/routes/session/queue-review.test.tsx` (real app, fake server):
  submit while the POST is held and no queue event has arrived shows the text QUEUED; the server
  list carrying the request replaces it with no duplicate; the empty list plus the message clears it.
  UP on an empty composer withdraws the last item only, fills the composer, and the edited resend
  names a new request; a refused withdrawal leaves the composer empty and sends nothing; a nonempty
  composer never withdraws.
- `packages/opencode/test/server/httpapi-session.test.ts`: recall and resend over HTTP against the
  real queue: a held prompt is listed, withdrawn, its caller answers 409, the edited resend is
  delivered under its new request, and only the edited text reaches the model.
- Red proofs by reverting each TUI behaviour; package typechecks for `tui` and `opencode`.
