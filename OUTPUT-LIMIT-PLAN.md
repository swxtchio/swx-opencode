# V1 output-limit port

Refs swxtchio/swx-opencode#304, output-limit sub-scope only.

- Re-check `anomalyco/opencode` commit `a3f7f42ac96800aec06fb26365cd482024a52cd1` and map its output-limit outcomes onto the V1 prompt loop and processor.
- Recover partial text/reasoning with retained history and continuation guidance; derive the bounded streak from durable assistant/user history so explicit resume preserves exhaustion and new input or another finish resets it.
- Settle unfinished tool input without executing it, preserve completed effects and V1 admission custody, and publish exhaustion through the existing V1 output-length error and settled status path.
- Exercise recovery, exhaustion, reset, tool input, failure/interruption, and affected queue delivery through the existing production prompt-loop fixture. Demonstrate bounded behavior-removal reds for changed assertions.
- Run focused prompt tests and changed-package typechecks, update `CHANGESET.md`, and deliver the single draft PR to `swxtch` for firstmate review.
