# V1 grouped delivery integration

The [dated task plan](../.work/20261012-message-latency-plan.md) records the captain-approved scope and its authority pointers. For implementation, read `selection`, `grouped`, `reserve` and `commit` in [the fork-owned queue](../packages/opencode/src/session/queue.ts), `dispatch` in [machine-message](../packages/opencode/src/session/machine-message.ts), and `prepareUserMessage` / `currentSelection` in [prompt](../packages/opencode/src/session/prompt.ts). These owners keep scheduling, original work and recipient execution selection together; this guide does not define a second scheduler or repeat their operational sequence.

## Frozen emitted surface

Use the [actual emitted checkpoint](../packages/opencode/test/fixture/grouped-delivery/checkpoint.json) and its [generation provenance](../packages/opencode/test/fixture/grouped-delivery/README.md) rather than a handwritten example. The captured scenarios contain original caller inputs, admitted originals, returned message parts, aggregate and ignored witnesses, reads after consume and model-conversion evidence. Read its producer SHA against the generator and source before treating another checkout as compatible; capture and publication are separate gates.

`SessionPromptQueue.Group`, `Constituent` and `QueuedInput` in [the schema owner](../packages/schema/src/session-prompt-queue.ts) define the externally consumed fields and input projection. `admitLocked`, `indexAliases`, `rememberAliases`, `constituentIDs` and `sameQueuedInput` in [the queue owner](../packages/opencode/src/session/queue.ts) define identity reconciliation. Consumers need exact original-input/session/identity validation against those definitions. Metadata appearances are provenance, not new work or acknowledgments: use the fixture's aggregate and singleton witnesses to understand repeated appearances of the same original. Reply obligations remain attached to original response-owed peer identities, never the outer group identifier.

## Firstmate ownership

The localized HTTP exact pending/session evidence consumer is the companion owner. Read `fm_opencode_http_message_surfaced` and its related confirmation functions in [Firstmate's HTTP library](/home/byates/fm-firstmate/bin/fm-opencode-http-lib.sh), then recheck the accepted scope in [its owning coordination record](/home/byates/fm-firstmate/data/grouped-delivery-2081-q2/coordination.md) before adapting confirmation to the emitted original constituents. Its sender/outbox ownership and stock-server compatibility remain part of that companion review.

Watcher2089 independently owns pacing, recurring consolidation and fresh measurements under [Firstmate's watcher ruling](/home/byates/fm-firstmate/data/grouped-delivery-2081-q2/watcher-ruling.md). Re-read that ruling when checking the producer policy; this document does not own its interval. For the watcher's actual acceptance/retirement mechanism, read `deliverPending` and `promptAccepted` in [the installed primary watcher](/home/byates/fm-firstmate/.opencode/plugins/fm-primary-watch-arm.js). There is no separate watcher payload lookup to adapt for this handoff; do not create one to satisfy grouped-confirmation prose.

Queued observations identify historical candidates and acknowledgment obligations. Fresh condition claims belong to the producer's observations under its ruling; selecting later queued text cannot certify measurement freshness. The emitted checkpoint shows the historical-provenance framing needed to keep those responsibilities distinct.

## History-read companion

For lookup eligibility, read `MessageTable` in [the schema source](../packages/core/src/session/sql.ts), the [generated history-order migration](../packages/core/src/database/migration/20261012124815_message-history-order.ts), and the [actual-driver regression](../packages/core/test/message-history-order.test.ts). The source and fixture checks own the query behavior and legacy-message evidence; live index creation, queue alteration and rollout require their separately authorized handoff.

## Failure and publication custody

For failed original ownership and the existing explicit restore control, read `promote` / `hold` and the preparation/write isolation regressions in [queue-dispatch](../packages/opencode/test/session/queue-dispatch.test.ts). For caller lifetime, use `openChannel` / `forget` / `reply` and the actual prompt lifecycle test. [Core transaction ownership](../packages/core/src/database/transaction.ts) and its Database layer integration own savepoint outcomes and post-commit publication lifetime; the generic Drizzle adapter remains a transaction adapter. These sources and the captured alias/heartbeat/individual scenarios are the review route; there is no prose-conformance guard.
