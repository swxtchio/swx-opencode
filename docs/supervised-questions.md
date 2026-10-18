# Supervised questions (`--questions-command`)

Fork feature, swxtchio/swx-opencode#286. A supervisor such as firstmate runs OpenCode where no human watches the
pane. With `--questions-command`, every ask a model raises is delivered to a command the operator chooses, stays
pending, and is settled by the first answer that arrives, from the supervisor or from a human.

An ask is one of:

- a **question** from the `question` tool: one or more questions, each with choices, optional multiple selection,
  and free text unless the question sets `custom: false`;
- a **plan acceptance** from the `plan_exit` tool: a question whose origin tool is `plan_exit`, header
  `Build Agent`, choices `Yes` and `No`;
- a **permission request** that the permission rules leave as `ask`. An operation the rules allow or deny is
  decided by the rules and never emitted.

Every emitted ask names its origin: the message and tool call that raised it, which is what an answer after a
restart is applied to. A GitLab Duo workflow's tool approval is emitted like any other permission request; it has no
tool call of its own, so its origin is its message and its request ID. An answer to it after a restart has no call to
apply to, so it is refused with `409 call-gone` and the ask stays pending. An ask with no origin at all is refused,
with an error in the server log; no producer raises one today.

Without the flag nothing changes: no ask is recorded or emitted.

## Launching

```sh
OPENCODE_SERVER_PASSWORD=... opencode serve --port 4096 \
  --questions-command /usr/local/bin/opencode-ask-receiver
```

- Setting `--questions-command` turns emission on; without it nothing is emitted.
- The command must be an executable path or a name on `PATH`. It is checked at startup.
- Only argv sets the option. Project or global config cannot enable emission or choose its command.

## The receiver

For each ask the server runs the command once:

- directly, without a shell, with no arguments, inheriting the server's environment and working directory;
- with one JSON document followed by a newline on stdin. Nothing the model wrote is ever an argument or a shell
  string; question and permission text are data inside the JSON.

The command's exit status is only logged (`ask emitted` or `ask emit failed`). It never answers, refuses, or delays
the ask: a failing receiver leaves the ask pending and answerable, and a receiver that never exits does not block
the answer. Keep the receiver short-lived; a held one keeps running.

A minimal receiver that appends each ask to a log:

```sh
#!/bin/sh
cat >> /var/log/opencode-asks.jsonl
```

### Payload

```json
{
  "type": "opencode.ask",
  "version": 1,
  "kind": "question",
  "id": "que_...",
  "sessionID": "ses_...",
  "parentSessionID": "ses_...",
  "directory": "/work/repo",
  "workspaceID": "wrk_...",
  "server": "http://127.0.0.1:4096",
  "origin": { "messageID": "msg_...", "callID": "call_...", "tool": "question" },
  "question": {
    "id": "que_...",
    "sessionID": "ses_...",
    "questions": [
      {
        "question": "Which colors?",
        "header": "Colors",
        "multiple": true,
        "options": [{ "label": "Red", "description": "Warm" }]
      }
    ],
    "tool": { "messageID": "msg_...", "callID": "call_..." }
  },
  "answer": {
    "method": "POST",
    "path": "/question/que_.../answer",
    "query": { "directory": "/work/repo" }
  }
}
```

- `kind` is `question` (including plan acceptance) or `permission`. A permission ask carries `permission` instead
  of `question`: the permission name, the `patterns` it asks for, `metadata`, and the `always` patterns an `always`
  reply grants.
- `sessionID` is the Session that raised the ask, which can be a subagent's child Session; `parentSessionID` is
  present for a child.
- `origin.tool` names the producing tool for a question (`question` or `plan_exit`).
- `server` is the server's listening URL. `workspaceID`, and `answer.query.workspace`, are present only for an ask
  raised in a workspace.
- `answer` is the route that settles this ask: send `answer.method` to `server + answer.path` with `answer.query` as
  query parameters.

## Answering

Use the server's normal authorization: HTTP basic auth with `OPENCODE_SERVER_USERNAME` (default `opencode`) and
`OPENCODE_SERVER_PASSWORD` when a password is set.

```sh
curl -u "opencode:$OPENCODE_SERVER_PASSWORD" -H 'content-type: application/json' \
  "$SERVER$PATH?directory=$DIRECTORY" -d '{"answers": [["Red", "Blue"], ["Ship it on Friday"]]}'
```

| Ask                | Body                                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| Question           | `{"answers": [[...], ...]}`: one array per question, in order. Each holds the selected labels (several when `multiple`) or free text. |
| Question, declined | `{"reject": true}`                                                                                                                    |
| Plan acceptance    | `{"answers": [["Yes"]]}` switches to the build agent; `{"answers": [["No"]]}` or `{"reject": true}` declines.                         |
| Permission         | `{"reply": "once"}`, `{"reply": "always"}`, or `{"reply": "reject", "message": "optional feedback"}`                                  |

A permission reply keeps its usual effect: `once` lets this call run, `always` also grants the ask's `always`
patterns for the rest of the server's life and settles the Session's other waiting requests they cover, and
`reject` refuses this call and the Session's other waiting permission requests (a `message` is returned to the
model as feedback).

### Results

- `200` with `{"requestID", "sessionID", "disposition", "decision", "recovered"}`:
  - `disposition: "settled"`: this answer settled the ask, and `decision` is it.
  - `disposition: "duplicate"`: an earlier answer, from a human or another resolver, already settled the ask, and
    `decision` is the winner's. A decision `{"type": "cancelled"}` means the tool stopped waiting before anyone
    answered: its turn was aborted, the ask failed while it was being raised, or its instance was disposed.
  - `recovered: true`: the server that waited on the ask was gone, and recovery wrote the decision into the stored
    call (see [After a restart](#after-a-restart)). A decision the live waiting call received reports
    `recovered: false`.
- `404` `QuestionNotFoundError` or `PermissionNotFoundError`, naming `requestID`: no such ask in this directory. An
  ask raised in another directory is unknown here; check `answer.query.directory`.
- `400`: malformed ID or body. `401`: missing or wrong credentials. Neither changes the ask.
- `503` `AskUnavailableError`, naming `requestID` and a `reason`. Nothing was settled:
  - `store-unavailable`: the ask's state could not be read or written, or, after a restart, the decision could not
    be written to the stored call. Retry.
  - `owned-by-live-process`: another live server process, on the same database, waits on the ask. Answer on that
    server.
- `409` `AskNotAppliedError`, naming `requestID` and a `reason`. The answer was not applied:
  - `apply-failed`: the producer's effect for the answer failed, for example a plan `Yes` whose switch to the build
    agent could not be written. Nothing was settled and the ask is still pending: answer it again once the cause is
    fixed, or answer differently.
  - `call-gone`: after a restart, the call the ask belongs to is gone, or its Session went on without it. Nothing
    was settled and the ask is still pending; every answer to it, a rejection included, is refused the same way.

`GET /question?directory=...` and `GET /permission?directory=...` list the outstanding asks this server can settle.

## Humans

The human routes keep working: the TUI and the SDK's `question.reply`, `question.reject` and
`permission.reply` settle the same ask as the answer route, and whichever is first wins. They return `true` when
their call settled the ask and their usual not-found error otherwise, including when the ask was already settled.
Every settlement publishes the usual `question.replied`, `question.rejected` or `permission.replied` event, so an
attached TUI drops an ask the supervisor answered.

A TUI in auto-accept mode (started with `--auto`, or toggled on) answers permission requests itself as they arrive,
and so wins against the supervisor. Leave it off in a pane the supervisor decides for.

A TUI attached after an ask was raised does not show that ask (swxtchio/swx-opencode#289); answer it on the route or
list it with `GET /question`.

## After a restart

The asks are kept in the server's database (`session_ask`, beside the Sessions), so they survive a restart that
reuses the same data directory or `OPENCODE_DB`. After the restart the outstanding asks are listed again, and the
answer route settles them as usual. When it writes the answer into the asking call, it reports `recovered: true`:

- **Question**: the stored call completes with the answers, as if the tool had returned them, and the Session
  continues on its own.
- **Plan acceptance**: `Yes` switches the Session to the build agent, once, and it continues; `No` declines as
  before.
- **Permission**: the guarded call never ran and is not run now. `once` or `always` reports the call as not run, so
  the model calls the tool again (an `always` grant is restored, so that retry is not asked again); `reject`
  refuses it as before.

Settling the ask and writing the call's outcome, including an accepted plan's build-agent message, are one database
transaction: an answer either does both or neither, with the ask still pending for a retry (`503 store-unavailable`
when the call cannot be written, `409 apply-failed` when the plan switch fails).

A plan accepted live works the same way: the switch to the build agent (its build message and instruction) is written
in the transaction that settles the `Yes`. If the switch fails, the answer gets `409 apply-failed`, nothing is
settled, no build message is left behind, and the plan call keeps waiting for another answer.

When the asking call is gone, or the Session went on before the answer arrived (for example because it was prompted
again after the restart), the answer gets `409 call-gone`: nothing is applied or settled, and the answer is not
consumed. The ask stays pending and listed, and `opencode db prune` and `opencode db retention` keep refusing its
Session; deleting the Session removes it.
When the call already ended without an answer, because its ask was aborted, the ask is settled as `cancelled`.

A refusal ends the turn as it would have live: a question rejection, a plan `No`, or a permission `reject` without a
message leaves the Session idle unless `experimental.continue_loop_on_deny` is set. Nothing else is replayed: no
provider request or other tool call is retried.

A repeated answer after a restart is a `duplicate` as usual. A decision the live waiting call received is never
reapplied, since that call acted on it (a granted command may have run). The one thing a repeated answer repairs
is a recovered call whose server died before its Session continued: that Session is continued now.

### When an ask can be recovered

The ledger records which server process waits on each ask. An answer reaching another server is applied only when
that process is gone; while it runs, or when its state cannot be proven, the answer is refused with
`503 owned-by-live-process`.

- The check uses Linux procfs: the recorded process ID, its start time and the machine's boot ID. Recovery
  therefore needs Linux, and every server on a database must run on the same machine and in the same PID
  namespace. Do not share a database across machines, or across containers that do not share a PID namespace: a
  process ID read in another namespace can name an unrelated process or none.
- Stopping the server by a signal or a crash leaves its asks recoverable. Disposing an instance while the server
  keeps running (for example on a configuration reload) ends its waiting asks, which are then reported as
  `cancelled`.
- Aborting a Session ends its waiting ask, which is cancelled in the ledger. If that write fails it is retried;
  while it still cannot be recorded the ask is no longer listed and the server refuses answers to it
  (`503 store-unavailable`), and after a restart the aborted call's own record settles the ask as `cancelled`.
- An abort can race a plan `Yes` that is being settled. When the switch commits first, the ask is settled with the
  answer and the build-agent message is written, but the aborted plan call ends without continuing the turn: the
  Session's next prompt runs as the build agent.
- A server started without `--questions-command` neither lists nor settles recorded asks, and `opencode db prune`
  and `opencode db retention` keep refusing their Sessions. To settle them, restart the server with
  `--questions-command` on the same database and answer, or reject, each one it lists.
- An ask raised in a subagent's child Session is recovered in that child: its call gets the answer and the child
  continues to its end. The parent's `task` call waited in the old process and is not recovered, so the parent
  Session does not receive the child's result; prompt the parent again to go on.

## Maintenance

`opencode db prune` and `opencode db retention` refuse a Session with an outstanding ask
(`supervised-ask-pending`). A settled ask is deleted with its Session by prune, and removed by retention.
