# Supervised questions (`--questions emit`)

Fork feature, swxtchio/swx-opencode#286. A supervisor such as firstmate runs OpenCode where no human watches the
pane. With `--questions emit`, every ask a model raises is delivered to a command the operator chooses, stays
pending, and is settled by the first answer that arrives, from the supervisor or from a human.

An ask is one of:

- a **question** from the `question` tool: one or more questions, each with choices, optional multiple selection,
  and free text unless the question sets `custom: false`;
- a **plan acceptance** from the `plan_exit` tool: a question whose origin tool is `plan_exit`, header
  `Build Agent`, choices `Yes` and `No`;
- a **permission request** that the permission rules leave as `ask`. An operation the rules allow or deny is
  decided by the rules and never emitted.

Without the flag nothing changes: no ask is recorded or emitted, and `opencode run` keeps refusing questions, plan
exits and permission requests as before.

## Launching

```sh
OPENCODE_SERVER_PASSWORD=... opencode serve --port 4096 \
  --questions emit --questions-command /usr/local/bin/opencode-ask-receiver
```

- `--questions emit` requires `--questions-command`, and `--questions-command` requires `--questions emit`.
- The command must be an executable path or a name on `PATH`. It is checked at startup.
- Only argv sets either option. Project or global config cannot enable emission or choose its command.

`opencode run` takes the same options:

- `opencode run --questions emit --questions-command <cmd> "<message>"` runs locally and listens on
  `127.0.0.1` so the answer route exists. Pass `--port` to fix the port. It prints
  `questions are emitted; answer them on <url>` to stderr. For each ask it prints a `question asked:` or
  `permission requested:` line with the request ID, then waits for the answer.
- `opencode run --attach <url> --questions emit "<message>"` leaves asks to the server it attaches to. That server
  must run with `--questions emit`, and `--questions-command` is refused here because the transport belongs to the
  server.
- `--auto` (and its aliases) is refused with `--questions emit`, which emits every permission request instead of
  approving it. `--questions emit` is refused for the interactive split-footer mode.

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
  - `disposition: "duplicate"`: an earlier answer, from a human or another resolver, already settled the ask.
    Nothing changed, and `decision` is the winner's. A decision `{"type": "cancelled"}` means the tool stopped
    waiting before anyone answered (its turn was aborted or its instance disposed).
  - `recovered: true`: the server that waited on the ask was gone, and the answer was applied to the stored call
    (see [After a restart](#after-a-restart)).
- `404` `QuestionNotFoundError` or `PermissionNotFoundError`, naming `requestID`: no such ask in this directory. An
  ask raised in another directory is unknown here; check `answer.query.directory`.
- `400`: malformed ID or body. `401`: missing or wrong credentials. Neither changes the ask.
- `503` `AskUnavailableError`, naming `requestID` and a `reason`. Nothing was settled:
  - `store-unavailable`: the ask's state could not be read or written. Retry.
  - `owned-by-live-process`: another live server process, on the same database, waits on the ask. Answer on that
    server.
  - `waiter-lost`: this process recorded the ask but no longer waits on it.

`GET /question?directory=...` and `GET /permission?directory=...` list the outstanding asks this server can settle.

## Humans

The human routes keep working: the TUI, `opencode run --mini`, and the SDK's `question.reply`, `question.reject` and
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
answer route settles them as usual, now with `recovered: true`:

- **Question**: the stored call completes with the answers, as if the tool had returned them, and the Session
  continues on its own.
- **Plan acceptance**: `Yes` switches the Session to the build agent, once, and it continues; `No` declines as
  before.
- **Permission**: the guarded call never ran and is not run now. `once` or `always` reports the call as not run, so
  the model calls the tool again (an `always` grant is restored, so that retry is not asked again); `reject`
  refuses it as before.

If the Session went on before the answer arrived, for example because it was prompted again after the restart,
the answer is still recorded (`recovered: true`), but the old call is left as it is and nothing is continued.

A refusal ends the turn as it would have live: a question rejection, a plan `No`, or a permission `reject` without a
message leaves the Session idle unless `experimental.continue_loop_on_deny` is set. Nothing else is replayed: no
provider request or other tool call is retried. A repeated answer after a restart is a `duplicate` as usual.

## Maintenance

`opencode db prune` and `opencode db retention` refuse a Session with an outstanding ask
(`supervised-ask-pending`). A settled ask is deleted with its Session by prune, and removed by retention.
