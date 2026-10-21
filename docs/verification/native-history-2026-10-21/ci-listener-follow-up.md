## Observed CI failure — 2026-10-21

Owning investigation: [swxtchio/swx-opencode#335](https://github.com/swxtchio/swx-opencode/issues/335). The task handoff is held for this CI failure; the earlier passing CI snapshot is not reused as green for the failed head.

Run https://github.com/swxtchio/swx-opencode/actions/runs/39576061930 at task head `8aaf31ae31a888f9159a6af234c39c016e4cc152` failed its unit job on one case: `packages/opencode/test/server/httpapi-listen.test.ts`, `HttpApi Server.listen > stop() gracefully closes an idle listener and is repeat-safe`. Typecheck and e2e passed. The assertion expected the post-stop `fetch` to reject, but it resolved. The failure log does not capture the URL/port, response status/body or responder identity.

The victim test and `packages/opencode/src/server/server.ts` are byte-identical to default branch `ac4e395c8d593b10470129647100102ebdc6f8ed`: Git blob IDs are respectively `585c59cb43176241d24939eae5372b0748f388e7` and `f35dde06a6a6c316e331f2c7dd450a5477ae5cba`. The prior task head `fff4119ebb` passed test run https://github.com/swxtchio/swx-opencode/actions/runs/39566491397, and that default-branch head passed https://github.com/swxtchio/swx-opencode/actions/runs/39562970476. These comparisons do not establish the cause of this failure.

## Source trace and confirmed oracle hazard

`startListener` requests `Server.listen({ hostname: "127.0.0.1", port: 0 })`. `server.ts::startWithPortFallback` prefers shared port 4096 before an ephemeral fallback. The test waits for two `stop()` calls, then expects a fetch to the retired URL to reject. It does not establish whether a responding server is the original listener or another listener that acquired the address. Package tests run with `--parallel=2`.

An isolated Bun 1.4.2 arm64 `node:http` dependency probe confirmed the oracle hazard in ten controlled attempts: after the original Node listener's close callback positively acknowledged closure, another listener bound the same selected ephemeral port, and fetching the retired URL resolved with the replacement marker. This is a dependency-level forced interleaving, not a reproduction of `Server.listen`, proof of the actual CI responder, or evidence that the production shutdown implementation is correct. Another unresolved branch is a suppressed shutdown error: the public stop wrapper uses `Effect.runPromiseExit(...).then(() => undefined)` and the scope closer ignores errors.

The [retained diagnostic driver](ci-port-reuse-probe.mjs) and [observed dependency-level results](ci-port-reuse-observations.json) make that limited observation reproducible. They are manual diagnostics, not a new production guard or regression-test replacement. The actual CI failure remains available through `gh-axi run view 39576061930 -R swxtchio/swx-opencode --log-failed`; its exact failing predicate was `expect(fetch(...)).rejects.toThrow()`, with `Received promise that resolved`.

The exact cause remains **unestablished**. This verification-only task has no installed workspace `node_modules`, so the owning source test was not run locally on base/head. No CI retry, skip, wider timeout, production edit or assertion relaxation was used to label the failed run green.

## Required owning-boundary investigation

- Capture the listener URL/port, original listener identity, shutdown exit/callback and any post-stop response identity when reproducing the failure.
- Reproduce the unchanged victim on base and task head with the same load and test packing; determine whether a port-reuse interleaving, a polluter, or shutdown failure is responsible.
- Force the identified interleaving/failure at the actual production listener boundary and fix its owner or test isolation. Preserve graceful, forceable, concurrent and repeat-safe stop behavior.
- Do not weaken the stop assertion, assume that any resolved fetch is the retired listener, or rerun the full CI job until an outside-cause explanation is actually established.
- Any new/rewritten regression must obtain listener behavior through the production producer, terminate on absence of the guarded behavior, and pass after restoring the complete correction.

This source/test investigation is outside the assigned published-release verification work at https://github.com/swxtchio/swx-opencode/pull/331. The published-byte fifty-check battery and ten post-crash join-only observations still passed with their explicit emulation/JIT qualifications; the PR's fresh CI is currently red and needs this owning-boundary decision before a clean handoff.

## Different failure on the automatic follow-up run

On 2026-10-21, automatic run [39582803608](https://github.com/swxtchio/swx-opencode/actions/runs/39582803608) at `7fc5540233bcd384cdb720bc01024b74fc7306b6` also failed its unit job, but it did not establish recurrence of this listener assertion. `@opencode-ai/core#test` reported the V1-to-V2 config property test exceeding its five-second test bound and the crashed-lock-owner fixture timing out before its worker `ready` file appeared. The owning test blobs still match default: `b9615877539b818cb51896694cb7612eaab2b5ab` and `a0f737a998d584828e4010deef77ed294587d28e`. Additional ACP timeout diagnostics and opencode exit 130 were logged while the Turbo run failed; their causal relation is unestablished. Typecheck/e2e passed.

[Owning core timeout investigation #336](https://github.com/swxtchio/swx-opencode/issues/336) records the distinct cases, source trace and needed matched-condition reproduction. The lock readiness timeout happened before crashed-owner recovery was exercised; it is not an observed failure of that recovery algorithm. No infrastructure/load cause is inferred from timeout text alone. Both #335 and #336 remain unresolved; no retries, wider thresholds, skips or source fixes were used to produce a green label.
