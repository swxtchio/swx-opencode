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

Authenticated pickup account was `brentyates-swx`; issue #304 had no assignees. At pickup, PR #311 claimed fast-suite selection without overlapping runtime scope. The single draft was opened against `swxtch` before implementation with the required first-comment stamp. `CHANGESET.md` carries the qualified #325 entry. Firstmate owns review, final-suite selection, ready transition, merge, and broader issue closure.

## Subsequent CI failure investigation

On 2026-10-21, [test run 39529648422](https://github.com/swxtchio/swx-opencode/actions/runs/39529648422) on port head `5ceca9be644283c344cf0c2a131b2d1e80e8df09` failed only `opencode serve --questions-command (subprocess) > an answer completes the call while the receiver is still holding its delivery`. Bun reported a test timeout rather than a behavioral assertion. Typecheck and e2e CI passed. The plan-only run's earlier changeset failure was corrected before implementation publication and is a different failure.

The failed case passed ten focused runs on `d1b232e940ef155a3e5311794a2baeaa143af1d8` and ten on the port head, using Bun 1.4.2 on the same local host. Base was a detached task-local worktree with the installed dependency tree reused. Command: `bun test test/cli/serve/serve-questions.test.ts -t 'an answer completes the call while the receiver is still holding its delivery'` from each tree's `packages/opencode`. These isolated runs do not reproduce CI load/concurrency, establish a root cause, or authorize treating CI as green. The affected source is the existing held FIFO receiver, answer/continuation path and scoped subprocess cleanup; no length finish occurs in the case.

[swxtchio/swx-opencode#327](https://github.com/swxtchio/swx-opencode/issues/327) retains the unresolved investigation and required producer-driven evidence. No speculative runtime change, skip, retry or enlarged deadline was applied. The failed unit check was reported to firstmate for scope disposition; it did not supply passing CI evidence.

On 2026-10-21, [test run 39533629596](https://github.com/swxtchio/swx-opencode/actions/runs/39533629596) on evidence-only head `b76d600ea5ce10a20877298816cd816ce962db62` failed instead at `fixture compaction rehearses backup, VACUUM, and restore under an exclusive writer fence`: `compactRestoreFixture` returned `refused` with `restored-source-inode-liveness-unavailable-under-fence`. The held-receiver case was not reported failed. The compaction implementation and test are unchanged from the base; the reason identifies unavailable evidence from `openPath(sourcePath)`, not its cause or permission to weaken the guard. The exact compaction case passed one focused local run with Bun 1.4.2 (`bun test test/cli/db/retention.test.ts -t 'fixture compaction rehearses backup, VACUUM, and restore under an exclusive writer fence'` from `packages/opencode`). [swxtchio/swx-opencode#328](https://github.com/swxtchio/swx-opencode/issues/328) retains this separate unresolved observation for firstmate's scope disposition. Neither failed run is green CI evidence.

## Passing merged-head validation

On 2026-10-21, after merging `swxtch` commit `394afd082f36067387e29c397a11d51b31c1ddb8` into the task branch, head `944b1e57f0e7feda5ff37317773d2834656a616c` passed the focused command above (17 tests), package typecheck and the crew commit-identity gate. [Test run 39536932305](https://github.com/swxtchio/swx-opencode/actions/runs/39536932305) completed successfully with unit and e2e jobs passing; the PR's typecheck check passed as well. This passing run was reported through the PR and review-ready wake. It does not establish or repair the causes of the earlier failures retained in #327/#328. Firstmate still owns review, its final suite gate and merge.
