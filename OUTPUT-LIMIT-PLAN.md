# V1 output-limit port

Refs swxtchio/swx-opencode#304, output-limit sub-scope only.

- Re-check `anomalyco/opencode` commit `a3f7f42ac96800aec06fb26365cd482024a52cd1` and map its output-limit outcomes onto the V1 prompt loop and processor.
- Recover partial text/reasoning with retained history and continuation guidance; derive the bounded streak from durable assistant/user history so explicit resume preserves exhaustion and new input or another finish resets it.
- Settle unfinished tool input without executing it, preserve completed effects and V1 admission custody, and publish exhaustion through the existing V1 output-length error and settled status path.
- Exercise recovery, exhaustion, reset, tool input, failure/interruption, and affected queue delivery through the existing production prompt-loop fixture. Demonstrate bounded behavior-removal reds for changed assertions.
- Run focused prompt tests and changed-package typechecks, update `CHANGESET.md`, and deliver the single draft PR to `swxtch` for firstmate review.

## Review fix round 1

- Count the durable provider-turn history independently of compacted model context, excluding V1 compaction control/summary/followup rows while retaining real user rows, including `noReply`. Use a limited database query rather than hydrating the session again.
- Buffer tool-argument fragments in their stream-attempt-owned call and settle incomplete input with its existing terminal write. Preserve execution admission and takeover custody without publishing cumulative raw input for each fragment.
- Drive automatic capacity compaction and fragmented argument streams through the production prompt loop. Prove exhaustion survives compaction, real input resets it, and fragment count does not amplify durable tool writes; retain bounded behavior-removal red evidence.
- File the adjudicated bare malformed-call edge as a follow-up, update handoff evidence, run focused tests/typecheck and mechanical gates, and push the same draft PR for firstmate review.

## Review fix round 2

- Restrict summary exclusion to assistant compaction-summary markers. Keep real user input in the streak even after `SessionSummary.summarize` has attached its diff summary.
- Enable the real summary producer in a focused prompt-loop fixture. Gate fresh input's first length response on its persisted user summary, then verify it receives the renewed continuation allowance; demonstrate the old predicate's bounded failure.
- Preserve round-1 compaction and fragment coverage, update evidence, and push the existing draft PR after the focused and mechanical gates pass.
