# Plan: selected queued-prompt actions

## Contract

- `Ctrl+f` forces only the selected pending item. The queue authorizes the action against current server state, changes the selected item's delivery to steer, and releases only the review hold that requested the force. A preparation in progress, missing item, unconfirmed hold, or another live holder refuses without changing queue custody. A successful action wakes the existing Session drain; the provider/tool work already in progress keeps its ownership.
- Consolidating a selected human-composer item considers every pending human-composer item. It succeeds only when every member is ready and their delivery and execution context can be represented by one carrier without changing meaning. It preserves each original input, part metadata, receipt and constituent in admission order. Otherwise it leaves the set intact and reports why. A non-human selection keeps the existing consolidation behavior.

## Implementation

1. Add the force transition to `packages/opencode/src/session/queue.ts`, keeping reservation invalidation, selected-row update, hold release and wake inside one session-locked queue operation; its database writes use the immediate-transaction boundary.
2. Extend queue consolidation with the selected item identity. Use the queue's human-composer and machine-marker classification; build the carrier from original inputs ordered by admission identity, retain the originals needed by later consolidation and delivery, and settle receipts before retiring source rows.
3. Wire the review dialog's `Ctrl+f` and selected-item consolidate actions through the declared HttpApi and generated SDK. Keep the `Ctrl+x`, then `q` review opener. Show refusal reasons and close the view only after force succeeds.
4. Add queue-service, review-dialog, HttpApi and real TUI-to-provider regressions for selection, hold protection, preparation, metadata and receipts. Demonstrate bounded red/green absence proofs for each changed behavior.
5. Regenerate the affected SDK surfaces, add the fork changeset, and run the focused tests, changed-package typechecks, lint and commit-identity gate before pushing the draft.
