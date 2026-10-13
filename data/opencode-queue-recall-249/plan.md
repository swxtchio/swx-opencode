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
   and names no request, so it cannot be confirmed by receipt. Filed as swxtchio/swx-opencode#250 rather than
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

## Follow-ups filed

- swxtchio/swx-opencode#250: slash commands are expanded before admission, so a sent command is not shown yet.
- swxtchio/swx-opencode#251: a withdrawn item's receipt stays `pending`; UP recall makes withdrawal a one-key action.
- swxtchio/swx-opencode#252: "last" is the bottom row in delivery order, not the most recent admission.
- swxtchio/swx-opencode#253: other rendered-app TUI tests write to the user's real state directory.

## Round 2 (review round 1 adjudication)

- Admission first: `SessionPrompt.prompt` admits a queued prompt before revert cleanup and model selection, which
  run as `SessionQueue.request`'s `ready` under the queue lock.
- `withdraw` settles the carried request to a new `withdrawn` receipt state and `restore` makes it pending again,
  so swxtchio/swx-opencode#251 is fixed here.
- The pending row and UP recall order by when a prompt was sent (`time.created`), which settles
  swxtchio/swx-opencode#252 for this change. UP waits for a still-sending prompt's admission via its receipt.
- One recall at a time through any restore; an unconfirmed withdrawal is restored, the queue is read back, and the
  text goes to the prompt stash if nothing shows it queued.
- Every authoritative queue list reconciles `session_sending` (`sync.queue.listed`).
- Recall keeps unmentioned attachments and repeated mentions; the composer sends mention offsets into the expanded
  text.
- Slash commands remain swxtchio/swx-opencode#250, the captain's scope call.

## Round 3 (review round 2 adjudication)

- Admission is the first step of `SessionPrompt.prompt`, before the config read. The row is `time_preparing`
  (migration `20261013194721`) until `SessionQueue.prepared`: listed and withdrawable, never promoted or merged.
  Classification, revert cleanup and model selection run outside the queue lock, so a slow preparation delays neither
  a later send's admission nor a withdrawal.
- A failed or interrupted preparation calls `unprepared`, dropping the row and its receipt, so an exact retry is
  admitted again. A preparing row a stopped process left is swept at the session's next admission or promotion.
- `giveBack` restores once, then reports only what the list or the receipt confirms after it. The prompt stash
  receives the text only when the receipt says the item is still withdrawn; otherwise it polls to a backstop and
  copies nothing.
- UP on a prompt still being sent follows merged receipts, chained, to the carrier and withdraws that.

## Base merge (#244)

- `origin/swxtch` 61e797c551 (#244) merged in, not rebased. #244 replaced `admitLocked` with `reconcileAdmission`,
  which reconciles transport identity (`messageID`, peer envelopes) before any preparation.
- The preparing admission runs `reconcileLocked` and the admission in one queue-locked section, before any config
  read. It classifies by built-in markers only, so it compares no delivery there; a matched retry or receipt retry
  then reads config and runs the strict `reconcile` with its configured classification, so #244's conflict check
  holds. Reconciled and retried prompts skip revert cleanup and model selection, as in #244.
- `eligible` (#244's promotion selection) keeps `promotable`; `reserve` sweeps orphaned preparing rows.

## Round 4 (review round 4 adjudication)

- UP does nothing while a recall is outstanding: a withdrawal can publish the emptied list before it answers, and
  history would then copy the prompt being taken back.
- `prepared` merges a prompt only with rows admitted before it (`replace(..., earlierOnly)`), then readmits each ready
  row admitted after it, oldest first, so a late preparation never supersedes or absorbs newer prompts.
- `dropPreparing` and the orphan sweep keep withdrawn rows and their `withdrawn` receipts; restoring a withdrawn
  preparing row whose sender is gone is refused, so the TUI stashes its text instead.
- An exact retry of a still-preparing original waits on `awaitPrepared` without the lock, then strictly reconciles
  under its classified delivery, or is sent anew if the original was dropped.
- Cross-process ownership of preparing rows stays process-local: swxtchio/swx-opencode#254.
