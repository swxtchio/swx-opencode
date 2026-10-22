# Small-model regression evidence

Recorded 2026-10-22 on Linux ARM64 with Bun 1.4.2. The sampled implementation and tests are at `751f9b2e68`, retained in draft PR swxtchio/swx-opencode#342. These are controlled-boundary integration tests: production config loading, provider resolution, session state and orchestration run; the LLM boundary records the model passed by production and supplies finite response events. The HTTP session test sends actual requests through one server to the controlled LLM HTTP endpoint.

## Focused command

Run from `packages/opencode`, with `TMPDIR` set to an existing scratch directory inside the worktree:

```sh
bun test test/provider/provider.test.ts test/config/config.test.ts test/config/v2-compat.test.ts test/session/prompt.test.ts test/session/compaction.test.ts test/session/session.test.ts test/server/httpapi-session.test.ts -t 'small-model|getSmallModel' --timeout 20000
```

The sampled restored implementation passed all 51 selected tests. Unchanged cases cover global title selection, both explicit-global Azure title providers, dedicated auxiliary-agent `model` overrides, unconfigured title inference/plugin/primary fallback, and unconfigured compaction's primary fallback.

## Removal proofs

Every removal run terminated with ordinary assertion failures or the test's own bounded error, without an external kill or harness test timeout. All temporary production mutations were restored afterward.

| Mutation | Selected outcome | Observed behavior |
| --- | --- | --- |
| Apply `small-model-behavior-removal.patch` to the sampled implementation | 26 failed, 25 preserved cases passed | Scoped title and compaction requests selected the global or primary model; compaction's global/Azure cases selected the primary; invalid selections were substituted; agent options leaked `small_model`; plural-agent lowering lost it; session creation/state and HTTP controls lost the new field. |
| In `Provider.getSmallModel`, remove only `input?.model ??` from the configured-selection expression | 10 failed, 0 passed | All four agent/session title and compaction cases plus both dedicated-agent `small_model` cases received a global or primary model despite correctly loaded/persisted scoped inputs. |
| Restore only the catch returning `undefined` around configured `getModel` resolution | 7 failed, 0 passed | Global/agent/session invalid-selection consumer tests and the provider failure test rejected silent substitution. Title tests failed through their own bounded missing-error check; compaction returned success with a substitute request. |
| Replace only `Session.setSmallModel`'s write with `Effect.void` | 2 failed, 0 passed | The session-state and same-server HTTP tests observed the old selection after an update, rather than the requested selection. |

The isolated scope-removal command selected `agent over global and session|session over global|agent only|session only|dedicated agent small_model` in `prompt.test.ts` and `compaction.test.ts`. The substitution command selected `reports invalid .*small_model|reports invalid (global|agent|session) selection` in those consumer files and `provider.test.ts`. The update command selected `small-model` in `session.test.ts` and `httpapi-session.test.ts`.

## Preservation-test boundary

A bounded diagnostic completed `Config.get` and catalog loading but stalled in `Plugin.list` before small-model selection. That provider preservation test has no LLM request or fixture server. Its inference cases now control unrelated plugin initialization and SDK installation while retaining production catalog loading, configured resolution and family inference. The original selected-model assertions and timeout bound remain. The provider-focused command completed with 9 passed and no failures after this change.
