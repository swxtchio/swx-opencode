# Webfetch URL-policy verification

Initial backport verification used source tree `ddb907a42f8136e9a066badc39239f773717e3c4`, containing redirect donor `17837a20d69fc514d337940506335afd75693cb3` and canonicalization donor `805a4b8200274b036d241878ac5b12c46771ad3e`. Round-1 follow-up verification on code commit `970be7d35ad050d862c0a1d6631a6e62b6a3a88d` is recorded at the end. Commands ran from package directories; no repository-wide suite was run by this crew.

## Initial backport focused green runs

From `packages/core`:

- `bun typecheck` — exited successfully (`tsgo --noEmit`).
- `bun test test/permission.test.ts -t 'checks for a deny without asking'` — 1 pass, 0 fail, 4 assertions.
- `bun test test/tool-webfetch.test.ts -t 'checks a relative redirect|stops at a redirect target|rejects unsupported redirect targets|retries Cloudflare challenges|matches canonical URL identity|rejects malformed and unsupported inputs|manually follows every supported status|continues an ask-only redirect under the explicit V2 agent|denies a later redirect hop before its live HTTP request|keeps challenge retries on the manual redirect policy'` — 10 pass, 0 fail, 61 assertions.
- `bun test test/tool-webfetch.test.ts -t 'registers and fetches an ordinary hostname HTTP URL without rewriting it|accepts localhost URLs with the same requested-URL permission check|converts HTML to requested markdown and text|rejects declared and streamed oversized bodies|keeps images and files unsupported until typed settlement can carry attachments|times out stalled requests'` — 6 pass, 0 fail, 14 assertions.

From `packages/opencode`:

- `bun typecheck` — exited successfully (`tsgo --noEmit`).
- `bun test test/permission/next.test.ts -t 'assertNotDenied - permits ask-only destinations without opening a request'` — 1 pass, 0 fail, 4 assertions.
- `bun test test/session/tools.test.ts -t 'binds deny-only checks to the executing agent and merged session rules'` — 1 pass, 0 fail, 6 assertions.
- `bun test test/tool/webfetch.test.ts -t 'denies canonical mixed-case|transports an uppercase-scheme|rejects malformed and unsupported input URLs|checks supported relative and absolute redirects|continues an ask-only redirect|checks a later absolute redirect|rejects unsupported redirects and bounds redirect chains|rechecks every redirect after a Cloudflare challenge'` — 8 pass, 0 fail, 58 assertions.
- `bun test test/tool/webfetch.test.ts -t 'returns image responses as file attachments|keeps svg as text output|keeps text responses as text output|extracts text from html without scripts or styles'` — 4 pass, 0 fail, 15 assertions.

## Initial backport changed-behavior absence proofs

Each red run removed or reverted only the listed behavior, then restored it before the focused green runs above. Commands below are run from the named package directory.

| Command | Behavior removed | Observed red result |
| --- | --- | --- |
| Core: `bun test test/permission.test.ts -t 'checks for a deny without asking'` | Queued a request for ask-effect rules in `PermissionV2.assertNotDenied`. | `PermissionV2.Service.list()` contained the pending `read` request instead of remaining empty. |
| OpenCode: `bun test test/permission/next.test.ts -t 'assertNotDenied - permits ask-only destinations without opening a request'` | Implemented `assertNotDenied` by delegating to `Permission.ask`. | The event race observed `permission.asked` and returned `asked` instead of `continued`. |
| OpenCode: `bun test test/session/tools.test.ts -t 'binds deny-only checks to the executing agent and merged session rules'` | Removed Session rules from the ruleset bound by `SessionTools.resolve`. | The target deny was lost and the call fulfilled instead of being rejected. |
| Core: `bun test test/tool-webfetch.test.ts -t 'checks a relative redirect before the server receives it'` | Removed the per-hop `check(next)` call. | The permission-check list was empty instead of containing the relative target. |
| Core: `bun test test/tool-webfetch.test.ts -t 'stops at a redirect target denied by the permission check'` | Removed the per-hop `check(next)` call. | The denied destination check was absent; the assertion requiring that target failed. |
| Core: `bun test test/tool-webfetch.test.ts -t 'denies a later redirect hop before its live HTTP request'` | Removed the per-hop `check(next)` call. | The live server returned the denied endpoint body as text instead of the expected tool error. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'checks a later absolute redirect against deny rules before the server receives it'` | Removed the per-hop `check(next)` call. | The denied hop completed successfully where the test expected failure before transport. |
| Core: `bun test test/tool-webfetch.test.ts -t 'denies a later redirect hop before its live HTTP request'` | Removed `FetchHttpClient.RequestInit`'s manual-redirect setting. | The HTTP client followed the redirect itself; the server returned denied-hop content and the tool produced text. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'checks supported relative and absolute redirects before transport'` | Removed `FetchHttpClient.RequestInit`'s manual-redirect setting. | The live client followed the first redirect without calling the deny-only check; the check list was empty. |
| Core: `bun test test/tool-webfetch.test.ts -t 'manually follows every supported status and an absolute redirect'` | Emptied `redirectStatuses`. | The 301 response became an error instead of returning redirected text. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'checks supported relative and absolute redirects before transport'` | Emptied `redirectStatuses`. | The 301 response failed instead of returning redirected content. |
| Core: `bun test test/tool-webfetch.test.ts -t 'continues an ask-only redirect under the explicit V2 agent'` | Made V2 deny-only evaluation queue ask-effect destinations. | The event race observed a permission ask and failed with `Webfetch opened a redirect permission request`. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'continues an ask-only redirect without creating a permission request'` | Changed the redirect check from `ctx.assertNotDenied` to `ctx.ask`. | The event race observed the new permission request and failed. |
| Core: `bun test test/tool-webfetch.test.ts -t 'rejects unsupported redirect targets and bounds redirect chains'` | Removed HTTP(S) validation for redirect destinations. | The `file:` destination reached the permission checker instead of failing before policy evaluation. |
| Core: `bun test test/tool-webfetch.test.ts -t 'rejects unsupported redirect targets and bounds redirect chains'` | Removed the redirect-count guard while keeping the loop fixture finite. | The recording transport observed 12 requests instead of the expected 11. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'rejects unsupported redirects and bounds redirect chains'` | Removed HTTP(S) validation for redirect destinations. | The unsupported destination completed successfully instead of failing before transport. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'rejects unsupported redirects and bounds redirect chains'` | Removed the redirect-count guard while keeping the loop server finite. | The recording server observed 12 requests instead of the expected 11. |
| Core: `bun test test/tool-webfetch.test.ts -t 'retries Cloudflare challenges with an honest user agent'` | Replaced the Cloudflare retry's deny-only check with a no-op. | The redirect target was missing from the recorded permission checks. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'rechecks every redirect after a Cloudflare challenge retry'` | Replaced the Cloudflare retry's deny-only check with a no-op. | The redirect target was missing from the recorded permission checks. |
| Core: `bun test test/tool-webfetch.test.ts -t 'matches canonical URL identity for the invoking V2 agent'` | Passed raw `input.url` instead of parsed serialization to policy and transport. | Both mixed-case and percent-encoded hosts were fetched under the wildcard allow; the aggregate deny/no-request assertion failed. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'denies canonical mixed-case and percent-encoded hosts before transport'` | Returned raw `params.url` instead of parsed serialization. | Both host variants were authorized and reached the server; the aggregate deny/no-request assertion failed. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'transports an uppercase-scheme URL in its canonical form under an allowing rule'` | Returned raw `params.url` instead of parsed serialization. | The canonical exact allow did not match the raw resource; the result was failure instead of transported content. |
| Core: `bun test test/tool-webfetch.test.ts -t 'rejects non-HTTP schemes before permission or transport'` | Bypassed initial URL parsing and HTTP(S) validation. | The file URL reached the fake permission/transport boundary and returned text instead of a refusal before policy. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'rejects malformed and unsupported input URLs before permission or transport'` | Passed raw input without initial URL parsing and scheme validation. | The malformed `http://` input appeared in the permission patterns instead of failing before permission. |

An initial combined OpenCode uppercase-scheme run reached Bun's default test timeout; the regression now has an effect-level backstop and a longer test ceiling, and its final red run fails on a policy/result assertion rather than the runner timeout.

## Round-1 follow-up verification

Tested code tree: `970be7d35ad050d862c0a1d6631a6e62b6a3a88d`.

From `packages/core`:

- `bun typecheck` — passed (`tsgo --noEmit`).
- `bun test test/tool-webfetch.test.ts` — 21 pass, 0 fail, 97 `expect()` calls.
- `bun test test/tool-webfetch.test.ts -t 'cancels discarded streaming responses across redirects and error exits'` — 1 pass, 0 fail, 15 `expect()` calls.

From `packages/opencode`:

- `bun typecheck` — passed (`tsgo --noEmit`).
- `bun test test/tool/webfetch.test.ts` — 13 pass, 0 fail, 92 `expect()` calls.
- `bun test test/tool/webfetch.test.ts -t 'releases every discarded streaming response body'` — 1 pass, 0 fail, 19 `expect()` calls.

Both live regressions retain the raw Fetch responses, stream an open response body, observe the server's `cancel` callback, and assert each response was used before the executor starts its next request. They cover followed redirects, denied hops, the redirect limit, unsupported targets, rejected 404s, Cloudflare 403 retries, and error exits (V2 unsupported content; V1 declared oversize content).

### Round-1 changed-behavior absence proofs

Each mutation below was restored before the final green runs above.

| Command | Behavior removed | Observed red result |
| --- | --- | --- |
| Core: `bun test test/tool-webfetch.test.ts -t 'cancels discarded streaming responses across redirects and error exits'` | Removed redirect-hop `cancelResponseBody`. | The retained raw 302 response stayed `bodyUsed: false`; the test failed before the next hop. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'releases every discarded streaming response body'` | Removed redirect-hop `cancelResponseBody`. | The retained raw 302 response stayed `bodyUsed: false`; the test failed before the next hop. |
| Core: `bun test test/tool-webfetch.test.ts -t 'cancels discarded streaming responses across redirects and error exits'` | Removed `filterResponse` cleanup for rejected statuses. | The aggregate `bodyUsed` assertion found an unread 404/Cloudflare response and failed. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'releases every discarded streaming response body'` | Removed `filterResponse` cleanup for rejected statuses. | The aggregate `bodyUsed` assertion found an unread 404/Cloudflare response and failed. |
| Core: `bun test test/tool-webfetch.test.ts -t 'cancels discarded streaming responses across redirects and error exits'` | Removed cleanup from V2 final-response error exits. | The unsupported-content stream remained unread; the aggregate `bodyUsed` assertion failed. |
| OpenCode: `bun test test/tool/webfetch.test.ts -t 'releases every discarded streaming response body'` | Removed cleanup from V1 final-response error exits. | The declared-oversize stream remained unread; the aggregate `bodyUsed` assertion failed. |
