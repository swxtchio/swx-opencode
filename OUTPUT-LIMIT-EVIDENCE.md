# V1 output-limit port evidence

Validation snapshot: 2026-10-21, branch `fm/opencode-304-port1`, published through [swxtchio/swx-opencode#325](https://github.com/swxtchio/swx-opencode/pull/325). Source and test history are retained by that PR's head ref. Refs swxtchio/swx-opencode#304, output-limit sub-scope only.

## Source mapping

Read `git show a3f7f42ac96800aec06fb26365cd482024a52cd1` from the object database; `upstream` resolves to `https://github.com/anomalyco/opencode.git`. Upstream's `SessionRunner.continueOutputLimit` queries the most recent user/assistant records including the current response and fails on three consecutive length-stopped assistants. This defines two automatic continuations. Its synthetic continuation guidance does not reset that streak. The behavioral examples are `continues text/reasoning after an output token limit` and `caps output token limit continuations and resets for new input` in `packages/core/test/session-runner.test.ts` at the named immutable commit.

The port keeps V1 history/admission selection in `SessionPrompt.run`, settlement in `SessionProcessor.cleanup`, and exhaustion in `SessionProcessor.finalizeFailure` using the existing `SessionV1.OutputLengthError`. The AI SDK adapter retains the shared `LLMEvent` contract and leaves malformed arguments uncalled so settlement can use the finish reason. No public schema, API, or generated client changed.

## Focused green command

Run from `packages/opencode`:

```sh
bun test test/session/prompt.test.ts -t 'output-limit|delivers queued machine mail after a content-filter terminal|records aborted errors when prompt is cancelled mid-stream|a provider error parks pending prompts until the next admission|an individual queued prompt retains its agent and session model variant'
bun typecheck
```

Observed: 17 tests passed, no failures; package typecheck exited zero. The focused tests cover partial text/reasoning and completed answers, exact exhaustion, its persisted classification/error event/idle settlement, explicit resume, new-input and other-finish resets, incomplete arguments and bounded recovery excerpts, completed effects, provider failures, interruption, content filtering, and queued receipt parking/delivery. Existing cancellation, error parking, filtering and recipient-owned model selection assertions remain green.

`bun run lint:correctness` exited zero with no errors. `bin/fm-lint.sh` is absent, so the ship policy's shell wrapper does not apply. `fm-ensure-agents-md.sh .` reported the existing AGENTS/CLAUDE linkage unchanged.

The provider fixture supplies an extra stop response beyond each expected cap, and assertions count captured requests after the real loop settles. A removed cap therefore produces an observable extra request rather than a waiting provider. Failure/interruption tests race recovery-request receipt against early loop settlement so a removed continuation fails immediately. All asserted history, tool outcomes, events and queue receipts come from the production producer paths.

## Behavior-removal red proofs

Each row was applied temporarily to production code, run against the focused regression, observed to exit nonzero at the indicated behavioral assertion, and restored. Prefix every filter below with `bun test test/session/prompt.test.ts -t` from `packages/opencode`.

| Removed behavior | Filter | Observed red assertion |
| --- | --- | --- |
| Treat length as a continuing finish at the loop's terminal boundary | `'output-limit continues partial|output-limit resets|output-limit recovery settles|delivers queued machine mail after bounded output-limit recovery'` | Both partial cases made one request instead of two; reset made one instead of six; all three recovery-failure cases reported early settlement; queue request contained held mail instead of recovery guidance. Seven failures, no hangs. |
| Enforce exhaustion (`recent.length === 3` temporarily changed to `4`) | `'output-limit caps'` | All three tool modes made four requests instead of three. |
| Classify exhaustion (`OutputLengthError` temporarily replaced by `Unknown`) | `'output-limit caps'` | All three tool modes persisted `UnknownError` instead of `MessageOutputLengthError`. |
| Reset on new input (user records temporarily excluded from the streak) | `'output-limit caps'` | Initial exhaustion and resume assertions passed; each fresh-input phase made one request instead of three. |
| Reset on other finishes (non-length assistants temporarily excluded) | `'output-limit resets the streak on a different finish'` | The tool-boundary sequence made four requests instead of six. |
| Preserve exhausted streak on explicit resume (retry-hidden assistants temporarily excluded from the durable streak) | `'output-limit caps.*none'` | Resume made two requests instead of one. |
| Settle unexecuted input as recoverable (`inputFailure` temporarily disabled) | `'output-limit repairs incomplete input'` | Both length and tool-calls cases persisted `Tool execution aborted` instead of the corresponding recovery guidance. |
| Preserve the original unexecuted call instead of dispatching malformed JSON through `invalid` (repair refusal temporarily disabled) | `'output-limit repairs incomplete input'` | The length case persisted the tool name `invalid` instead of `bash`. |
| Bound queue-intersecting exhaustion (cap temporarily disabled) | `'output-limit exhaustion parks'` | The root returned a successful stop instead of classified exhaustion. |

## Delivery ownership

Authenticated pickup account was `brentyates-swx`; issue #304 had no assignees. Open PR #311 claims fast-suite selection, without overlapping runtime scope. The single draft was opened against `swxtch` before implementation with the required first-comment stamp. `CHANGESET.md` carries the qualified #325 entry. Firstmate owns review, final-suite selection, ready transition, merge, and broader issue closure.
