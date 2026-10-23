# V2 small-model regression evidence

Recorded 2026-10-23 on Linux ARM64 with Bun 1.4.2, against this branch's implementation commit `db91cf891d` and its
tests. For each test, one production mutation removed the behaviour it guards. The focused test then ran and was
restored afterwards. Every run ended in an ordinary assertion failure inside the test's own bound. None needed an
external kill or a harness timeout.

| Removed behaviour | Mutation | Test that failed |
| --- | --- | --- |
| An exclusive operation waits for the active drain | `run-coordinator.ts`: drop the active-entry wait in `exclusive` | coordinator: runs an exclusive operation after the active drain |
| An exclusive operation releases its key | `run-coordinator.ts`: run the operation inline instead of as the key's execution | coordinator: returns an exclusive operation's failure |
| The runner titles a new session | `runner/llm.ts`: drop the drain's title fork | runner: titles a new Session from its first prompt |
| Titles run on the auxiliary model | `runner/llm.ts`: title on the primary | runner: titles a new Session from its first prompt |
| Title gates (default title, one prompt) | `runner/llm.ts`: drop both gates | runner: titles only a default-titled Session |
| Automatic summaries run on the auxiliary model | `runner/llm.ts`: summarize on the turn's model | runner: summarizes automatic compaction on the auxiliary model |
| Manual compaction exists | `session.ts`: fail `compact` as before | runner: compacts the whole conversation on request |
| Manual compaction summarizes a conversation that fits the keep budget | `compaction.ts`: drop the whole-conversation selection | runner: compacts the whole conversation on request |
| Nothing to compact is success | `session.ts`: treat `skipped` as a failure | runner: completes a manual compaction with nothing to compact |
| Failed and oversized summaries fail by name | `session.ts`: ignore `oversized` | runner: fails a manual compaction whose summary is unavailable or oversized |
| The resolver reads the session small model | `runner/model.ts`: read only the agent model | environment: resolves a session's small model; fails without a substitute |
| The agent's model outranks the session small model | `runner/model.ts`: swap the precedence | environment: prefers the auxiliary agent's own model |
| The resolver falls back to the primary | `runner/model.ts`: fail instead of using the primary | environment: falls back to the primary |
| V2 create records `smallModel` | `server/handlers/session.ts`: drop the payload field | HTTP: runs a V2 session's title and manual compaction on its created and patched small models |
| V2 sessions read the stored small model, including a V1 patch | `session/info.ts`: drop `smallModel` from `fromRow` | HTTP: the same test |
| Compact documents 500, not 503 | `protocol/groups/session.ts`: restore `ServiceUnavailableError` | OpenAPI: documents v2 session compaction failures |

The runner, coordinator and environment tests are in `packages/core/test`. The HTTP and OpenAPI tests are in
`packages/opencode/test/server`.

The HTTP test drives the real server against `TestLLMServer`, which answers every title request itself. The test
therefore asserts the model the title request named and the title the session recorded. It does not assert the title
text it queued.

Existing native HTTP tests that counted every provider call now count only turn calls (`turnCalls`), because a native
session's first prompt also sends a title request. Each test's expected count is unchanged.
