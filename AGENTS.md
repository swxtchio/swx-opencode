- To regenerate the legacy JavaScript SDK, run `./packages/sdk/js/script/build.ts`.
- After changing the public Protocol or Server `HttpApi`, run `bun run generate` from `packages/client`. Do not edit `src/generated` or `src/generated-effect` directly.
- Keep runtime dependencies directed from Schema to Core and Protocol, then from Core and Protocol to Server. Client runtime code may depend on Schema and Protocol but never Core or Server; `sdk-next` composes Client, Core, and Server.
- The default branch in this **fork** (`swxtchio/swx-opencode`) is `swxtch`, not `dev`. `dev` is a read-only mirror
  of `upstream/dev` that only the sync script below advances; never commit to it. `main` is inherited from
  upstream and not maintained here. A local
  `refs/remotes/origin/HEAD` cloned before the fork still points at `origin/dev` and is not to be trusted.
  Resolve it rather than assuming: `gh repo view swxtchio/swx-opencode --json defaultBranchRef`.
- Use `swxtch` or `origin/swxtch` for diffs and as the base for branches and pull requests.
- Sync with upstream by merging, never rebasing: run `bun script/sync-upstream.ts` from a dedicated worktree. It
  fast-forwards `dev` to `upstream/dev` and prepares a `--no-ff` merge onto a new branch off `origin/swxtch`. Land
  that PR with a merge commit, never a squash; the script's header documents its report tokens and conflict handling.
- `CHANGESET.md` at the root records every upstream sync and every change this fork carries. A PR against `swxtch`
  adds its own `- **#<number>**` entry under the matching area in "Fork changes", which CI checks; the sync script
  writes the sync entries, and CI checks those too.

## Branch Names

Use a short branch name of at most three words, separated by hyphens. Do not use slashes or type prefixes such as `feat/` or `fix/`.

Examples: `session-recovery`, `fix-scroll-state`, `regenerate-sdk`.

## Commits and PR Titles

Use conventional commit-style messages and PR titles: `type(scope): summary`.

Valid types are `feat`, `fix`, `docs`, `chore`, `refactor`, and `test`. Scopes are optional; use the affected package or area when helpful, e.g. `core`, `opencode`, `tui`, `app`, `desktop`, `sdk`, or `plugin`.

Examples: `fix(tui): simplify thinking toggle styling`, `docs: update contributing guide`, `chore(sdk): regenerate types`.

## Style Guide

### General Principles

- Keep things in one function unless composable or reusable
- Do not extract single-use helpers preemptively. Inline the logic at the call site unless the helper is reused, hides a genuinely complex boundary, or has a clear independent name that improves the caller.
- Avoid `try`/`catch` where possible
- Avoid using the `any` type
- Use Bun APIs when possible, like `Bun.file()`
- Rely on type inference when possible; avoid explicit type annotations or interfaces unless necessary for exports or clarity
- Prefer functional array methods (flatMap, filter, map) over for loops; use type guards on filter to maintain type inference downstream
- In `src/config`, follow the existing self-export pattern at the top of the file (for example `export * as ConfigAgent from "./agent"`) when adding a new config module.
- In Effect generators, bind services to named variables before calling methods. Do not use nested service yields such as `yield* (yield* Foo.Service).bar()`.

Reduce total variable count by inlining when a value is only used once.

```ts
// Good
const journal = await Bun.file(path.join(dir, "journal.json")).json()

// Bad
const journalPath = path.join(dir, "journal.json")
const journal = await Bun.file(journalPath).json()
```

### Destructuring

Avoid unnecessary destructuring. Use dot notation to preserve context.

```ts
// Good
obj.a
obj.b

// Bad
const { a, b } = obj
```

### Imports

- Never alias imports. Do not use `import { foo as bar } from "..."` or renamed imports like `resolve as pathResolve`.
- Never use star imports. Do not use `import * as Foo from "..."` or `import type * as Foo from "..."`.
- If a namespace-style value is needed, import the module's own exported namespace by name, for example `import { Project } from "@opencode-ai/core/project"`, then reference `Project.ID`.
- Prefer dynamic imports for heavy modules that are only needed in selected code paths, especially in startup-sensitive entrypoints. Destructure dynamic import bindings near the top of the narrowest scope that needs them so they read like normal imports. Avoid inline chains such as `await import("./module").then((mod) => mod.value())` or `(await import("./module")).value()`. Keep branch-specific imports inside the branch that needs them to preserve lazy loading.

### Variables

Prefer `const` over `let`. Use ternaries or early returns instead of reassignment.

```ts
// Good
const foo = condition ? 1 : 2

// Bad
let foo
if (condition) foo = 1
else foo = 2
```

### Control Flow

Avoid `else` statements. Prefer early returns.

```ts
// Good
function foo() {
  if (condition) return 1
  return 2
}

// Bad
function foo() {
  if (condition) return 1
  else return 2
}
```

### Complex Logic

When a function has several validation branches or supporting details, make the main function read as the happy path and move supporting details into small helpers below it.

```ts
// Good
export function loadThing(input: unknown) {
  const config = requireConfig(input)
  const metadata = readMetadata(input)
  return createThing({ config, metadata })
}

function requireConfig(input: unknown) {
  ...
}
```

- Keep helpers close to the code they support, below the main export when that improves readability.
- Do not over-abstract simple expressions into many single-use helpers; extract only when it names a real concept like `requireConfig` or `readMetadata`.
- Do not return `Effect` from helpers unless they actually perform effectful work. Synchronous parsing, validation, and option building should stay synchronous.
- Prefer Effect schema helpers such as `Schema.UnknownFromJsonString` and `Schema.decodeUnknownOption` over manual `JSON.parse` wrapped in `Effect.try` when parsing untrusted JSON strings.
- Add comments for non-obvious constraints and surprising behavior, not for obvious assignments or control flow.

### Schema Definitions (Drizzle)

Use snake_case for field names so column names don't need to be redefined as strings.

```ts
// Good
const table = sqliteTable("session", {
  id: text().primaryKey(),
  project_id: text().notNull(),
  created_at: integer().notNull(),
})

// Bad
const table = sqliteTable("session", {
  id: text("id").primaryKey(),
  projectID: text("project_id").notNull(),
  createdAt: integer("created_at").notNull(),
})
```

## Testing

- Avoid mocks as much as possible, you shouldn't be using globalThis.\* at all unless it's the only option.
- Test actual implementation, do not duplicate logic into tests
- `bun test` and `npm test` refuse at the repo root (guard: `do-not-run-tests-from-root`). Run one package's tests
  from its directory, like `packages/opencode`, or the whole unit suite with `tests/run.sh`, which runs every
  package's `test` through turbo as CI does and passes extra arguments on (`tests/run.sh --filter=@opencode-ai/core`).

## Type Checking

- Always run `bun typecheck` from package directories (e.g., `packages/opencode`), never `tsc` directly.

## V2 Session Core

- Keep durable prompt admission separate from model execution. `SessionV2.prompt(...)` admits one durable `session_input` row before scheduling advisory `SessionExecution.wake(sessionID)` unless `resume: false` requests admit-only behavior. The serialized runner promotes admitted inputs into visible user messages at safe boundaries.
- Reusing a Session ID adopts the existing Session. Reusing a prompt message ID reconciles an exact retry only when Session, prompt, and delivery mode match; conflicting reuse fails. Historical projected prompts lazily synthesize promoted inbox records during exact retry.
- Keep `SessionExecution` process-global and Session-ID based. Its local implementation owns the process-local Session coordinator and discovers placement through `SessionStore` plus `LocationServiceMap.get(session.location)` only when a drain starts; no layer should take a Session ID. V2 interruption targets the active process-local ownership chain for that Session; idle or missing interruption is a no-op.
- Keep `SessionRunner`, model resolution, tool registry, permissions, and filesystem Location-scoped. Omitted `Location.workspaceID` means implicit-local placement; explicit workspace identity remains reserved for future placement semantics.
- Preserve one explicit `llm.stream(request)` call per provider turn and reload projected history before durable continuation. Do not bridge through legacy `SessionPrompt.loop(...)` or delegate orchestration to an in-memory tool loop.
- Keep local Session drains process-local until clustering is implemented. `SessionRunCoordinator` joins explicit same-Session resumes, coalesces prompt wakeups, and allows different Sessions to run concurrently. Advisory wakes drain eligible durable inbox rows only; post-crash continuation recovery requires a separate explicit design before it may retry provider work. A drain has no durable identity or transcript boundary.
- Keep delivery vocabulary explicit. Prompts steer by default and promote at the next safe provider-turn boundary while the current drain requires continuation. An explicit `queue` input remains pending until the Session would otherwise become idle; promote one queued input at that boundary, then reevaluate continuation before promoting another. Promoting any new user input resets the selected agent's provider-turn allowance; a batch of steers resets it once.
- Keep EventV2 replay owner claims separate from clustered Session execution ownership.
- Keep the System Context algebra, registry, and built-ins in `src/system-context`; keep Context Source producers with their observed domains, and keep Session History selection plus Context Epoch persistence Session-owned.

## V1 Prompt Queue (fork)

- Every V1 prompt except `noReply` is admitted to the fork-owned durable queue in `packages/opencode/src/session/queue.ts` before it becomes a user message; `prompt.ts` only hooks into it. Put new queue behaviour in that module, since `prompt.ts` churns upstream and small hooks re-apply cleanly on a sync.
- A `steer` (the default) is promoted at admission so it still reaches the running turn's next step; only an explicit `queue` waits for the would-idle boundary. Holding every steer until the loop top would break senders that expect their message in history mid-run.
- Never name a fork table, route or event after upstream v2's `session_input`, `session_pending` or `session_inbox`; a sync bringing v2 in would collide with it.
- Every queue write goes through `writable()` in `queue.ts`, which opens its transaction with `BEGIN IMMEDIATE`. A deferred transaction that reads before it writes pins a read snapshot, and a later commit by another writer then fails the write with `SQLITE_BUSY_SNAPSHOT`, which the driver does not retry (#162). Add new queue writes inside `writable()`; read-only queue transactions may stay deferred.
- Preserve sender text. Grouped delivery keeps exact original text-part constituent metadata under the captain's batching policy; queue times for receipt/review provenance reach the model through `SessionQueue.annotate` on the request only (#212). A pending prompt runs at the session's model and variant when it is delivered
  (`prepareQueued` in `prompt.ts`), never the ones it was admitted with.
- The TUI review view holds a session's promotion (`hold`/`release` in `queue.ts`, #212). Both `reserve` and `commit` check `held()`: without the `reserve` check a promotion loop re-reserves forever against a `commit` that refuses. A new promotion path needs the same checks, and `awaitingDrain` must keep ignoring held pending rows or a drain caller re-drains in a loop.
  After its run a waiting caller decides from its own item's `disposition`, observed after any hold ends, never from
  whether a hold is live then: the run it joined ended on the turn before (#215).
- A sender that names a `requestID` confirms by receipt, not text (#212). Receipts ride on the item that carries them
  (`settleReceipts` in `queue.ts`), so any new path that deletes, merges or promotes a pending row must settle them, or
  the sender sees its request pending forever. Rules that hide a sender's text (`admission`) apply only when every item
  involved names a request.
- A queued prompt is admitted as the first step of `SessionPrompt.prompt`, before even its config read (#249). Its row
  is `time_preparing` until the sender calls `SessionQueue.prepared`: listed and withdrawable, but never promoted or
  merged, so pre-delivery work (classification, revert cleanup, model selection) runs outside the queue lock. A failed
  preparation calls `unprepared`, which drops the row and its receipt unless it was withdrawn meanwhile (a withdrawn one
  keeps its `withdrawn` receipt, and cannot be restored once its sender is gone). Admission order is the ascending item
  id, never `seq`, which a survivor inherits as a delivery slot: a row merges only with rows admitted before it and
  after the newest still-preparing row before it, and when a preparation ends the rows up to the next one are readmitted
  in id order, so completion order never outranks admission order. Put new pre-delivery work in that preparation, never
  ahead of admission, and keep `promotable` on any new promotion or merge query. The preparing admission runs #244's
  identity reconciliation in the same locked section, classified by built-in markers only and without comparing
  delivery; a retry it matches waits for a still-preparing original (`awaitPrepared`), then reconciles again with its
  configured classification, or is sent anew if the original was dropped.
- The TUI never copies a pending prompt into the composer: UP recall takes custody only through
  `session.queue.withdraw`, holds its lock through any restore, and restores the item once if the composer cannot take
  it (#249). UP does nothing at all while a recall is outstanding, since history could otherwise copy the prompt. It
  then reports only what the list or the receipt confirms, and stashes the text only when the receipt says the item is
  still `withdrawn`, so the queue and the user never both hold it. A prompt the TUI is still sending shows from
  `session_sending` in `context/sync.tsx`; every list of the queue it reads goes through `sync.queue.listed`.

- At eligible V1 delivery boundaries, deliver validated peers before automation, at most ten complete numbered constituents per batch. Coalesce validated automation by type and latest admitted text for an identified episode, explicitly as historical provenance rather than a current measurement, preserving distinct duties and original text-part constituent metadata. Keep native/unknown requests individual, critical steers immediate, and model/variant selection recipient-owned at committed promotion. See `docs/v1-queue-dispatch.md`.

## V1 Tool Execution (fork)

- A local V1 tool runs only after `SessionProcessor.admitToolCall` has durably marked its owned part running (#234).
  Every execute wrapper in `session/tools.ts` admits before its plugin hook or body, and writes metadata and its outcome
  only through the `Admission` that returns; a new wrapper must too. The AI SDK dispatches without waiting for the
  processor, so admission waits for the consumer's registration, never creates the part, and is bound to its stream
  attempt. A retry that reuses a call ID takes the call over: an execution an earlier attempt admitted can no longer
  write or settle it, and the retry's admission records its own input. A running part records the latest admission,
  not that the effect happened.
- Tool part read-modify-writes in `processor.ts` run in one immediate transaction (`withToolCall`), never under an
  in-memory lock: a lock held across `Session.updatePart` also waits on event listeners, so cancellation cleanup would
  wait on another fiber's publication.

## Session Retention (fork)

- `opencode db retention` (`packages/opencode/src/cli/cmd/db-retention.ts`) redacts inactive Session trees only when every gate in `candidateFailures` passes, re-checked inside each batch's write lock.
- Its cross-process liveness relies on `packages/core/src/event/writer.ts`: the Database layer registers each process, and `EventV2` records the writing process in the same transaction as every durable append. Keep Session writes on that path, and classify any new Session-owned table in the retention tool, or it refuses every tree.

## Supervised Asks (fork)

- `--questions-command` (#286, `docs/supervised-questions.md`) is set only from `serve` argv into `AskEmit`
  (`packages/opencode/src/ask/emit.ts`); never read it from config. With it off, `Question` and `Permission` keep
  their stock in-memory paths and write no `session_ask` row.
- Every emitted ask is settled once through the ledger (`packages/opencode/src/ask/ledger.ts`, immediate
  transaction). A new resolver path (route, cascade, cancel) must settle there and act only on a won settlement, or
  the ledger and the waiting tool disagree. A live settlement and its delivery to the waiter are one uninterruptible
  step, and an ask's every exit cancels its still-pending row. An answer with no live waiter is applied only when
  the owning `event_writer_process` is gone, by `AskRecovery` (`packages/opencode/src/ask/recovery.ts`), which
  settles the row and writes that call's outcome in one transaction. Every emitted ask needs a tool call origin.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
