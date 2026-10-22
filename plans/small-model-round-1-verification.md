# Small-model round-1 evidence

Recorded 2026-10-22 on Linux ARM64 with Bun 1.4.2. The sampled implementation and focused tests are at `1d7aeee58f`, retained in draft PR swxtchio/swx-opencode#342.

Run from `packages/opencode`, with `TMPDIR` pointing to an existing worktree-local scratch directory:

```sh
bun test test/session/prompt.test.ts test/session/session.test.ts test/session/compaction.test.ts -t 'small-model round-1|small-model compaction reports invalid|refuses a title write whose own read' --timeout 20000
```

The implementation passed all 12 selected tests. Removing the TaskTool child-creation `smallModel` field and the fork's copied field, replacing the Session patch snapshot comparison with `true`, and removing the compaction `session.error` publication produced 10 failures and 2 preservation passes. Every failure terminated within the test's own bounded execution, without a harness test timeout or external kill. All mutations were restored afterward.

| Owner | Removal failure observed |
| --- | --- |
| TaskTool child creation | Session and invoking-agent inheritance cases sent compaction requests with the global model instead of the inherited selection. The global and explicit child-agent cases lost the inherited stored assignment; the child's own agent still overrode the inherited choice at the request boundary. |
| Session fork | Fork compaction sent the global model instead of the source session selection. |
| Session patch | Holding each setter immediately after its real Session read, committing the competing setter, then releasing the held setter lost either `smallModel` or the generated title when snapshot validation was removed. With validation, both the stored row and the final durable event retain both fields. |
| Compaction resolution | All three global/agent/session invalid-selection tests failed through their own bounded missing-error checks when publication was removed. With publication, they observe the error and no substitute LLM request or summary assistant. |

The two preservation cases kept passing during removal: a resumed child retained its host-assigned selection, and a title write whose source session was removed still failed through the existing projector refusal.

After restoring the mutations, the broader focused command from `plans/small-model-verification.md`, additionally selecting `refuses a title write whose own read|commits ordinary writes while the Session exists`, passed 61 tests across the same seven files. Package-local OpenCode typechecking also passed. The round changes no public schema or HTTP contract, so the existing generated clients remain applicable; the web changes are documentation only.

Child/fork tests use real parent prompts, TaskTool execution, session creation/forking, provider selection and compaction. The controlled LLM boundary supplies finite events and records the production request; it does not supply the expected model. The patch tests use the existing span-hold fixture to force both race orderings without sleeps deciding success.
