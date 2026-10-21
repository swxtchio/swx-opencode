# Native x64 hardware reproduction procedure

This checklist was prepared while native x64 execution was blocked. The later [manual driver](probe-commands.py) completed the qualified full-system guest run recorded in [the final recommendation](README.md); this original source-owned battery was not executed by this task. Retain this procedure for a stock native-x64-hardware reproduction in an authorized disposable worktree/root. Keep every database, config, provider fixture, scratch directory and evidence output there. Preserve the selected release bytes and packaged sidecar; a source-built executable cannot replace them.

## Existing release battery

The published source `d1b232e940ef155a3e5311794a2baeaa143af1d8` owns `packages/opencode/script/release-probe.ts`, `release-probe-requests.ts` and `release-probe-process.ts`. These files were unchanged between the selected source and the task's initial plan commit. The battery configures a local controlled provider, starts the extracted release binary, creates its own absolute `session.sqlite`, and holds a local Bash tool and required final provider continuation until positive producer wait-registration observations are recorded. It also exercises idle, admit-only pending, terminal-tool failed and explicit interrupted outcomes, database selection and completed-history restart.

From the approved worktree, with native x64 Bun already available:

```sh
uname -m
gh-axi release view v1.18.32-swxtch.d1b232e9 -R swxtchio/swx-opencode --full
ls .native-history
mkdir -p .native-history/downloads .native-history/extracted
gh-axi release download v1.18.32-swxtch.d1b232e9 -R swxtchio/swx-opencode --pattern 'opencode-linux-x64.tar.gz*' --dir .native-history/downloads
```

Run `sha256sum -c opencode-linux-x64.tar.gz.sha256` in the downloads directory, list the archive, extract into the sibling `extracted` directory, and record archive/executable/sidecar digests as in `artifact-observations.txt`. Retain absolute executable and sidecar paths and version output. Then, from the worktree:

```sh
OPENCODE_VERSION=1.18.32-swxtch.d1b232e9 bun packages/opencode/script/release-probe.ts x64 "$PWD/.native-history/extracted/opencode" "$PWD/.native-history/native-run"
```

The battery's `native-run/evidence.json`, `opencode-server.log`, native DB and generated provider/permission config are reproduction leads and evidence outputs. Copy raw observations into the committed evidence directory after reviewing them for unrelated/environment-secret content. Retain process stop/reap confirmation; use readiness and returned state rather than elapsed time as success signals.

## Observation checklist beyond the original release battery

The existing battery alone does not cover the complete task. For reproduction, include the observations below, using the producer-owned fixtures at `packages/opencode/test/server/httpapi-session.test.ts` (held wait and durable cursor cases), `packages/core/test/session-history.test.ts`, and `packages/core/test/message-history-order.test.ts` as design leads. The qualified guest run's raw observations are now retained in `release-run.json`; source-test passes cannot replace execution of the published bytes.

| Observation | Request/record boundary | Required comparison |
| --- | --- | --- |
| Explicit prompt identity and exact retry | Create through `POST /api/session`; send `POST /api/session/{sessionID}/prompt` with top-level `id`, `prompt`, and explicit `delivery: "steer"`; repeat the identical identity/prompt/delivery. | Returned Session/message/admitted sequence remain the original admission, and native rows/history show no duplicate user turn. Keep the original full request, not a fabricated receipt. |
| Initial provider hold | Start an independently controlled provider request and register `POST /api/session/{sessionID}/wait` while it remains held. | Producer `session.wait.registered` binds the same Session/admission/wait ID; wait has not returned; releasing the provider permits progress. |
| Tool and required continuation holds | Existing battery's controlled Bash tool and final provider continuation. | Each held phase is positively active, wait remains registered/unresolved, the tool settles before final continuation, and final release yields completed with matching admitted sequence and readable terminal assistant. |
| Independent terminal outcomes and controls | Completed, unavailable-tool failed, active interrupted, fresh idle and admit-only pending Sessions. | Raw admission and wait responses agree with native assistant/tool/terminal event facts. Idle and pending are not completion of new admitted work. |
| Native rows | Read-only SQLite queries for `session_input`, `session_message`, `event`, plus database path/inode observations. | Correlate HTTP IDs and sequences with producer-written rows. Do not INSERT, seed or manufacture projections/events. |
| Pre-restart finite history | Read `GET /api/session/{sessionID}/history` with a small page limit; append returned public events and advance `after` from the last returned `durable.seq` until `hasMore` is false. | Full public history, across nonempty page boundaries, contains strictly increasing unique sequences and IDs; public sequence gaps remain valid. Record every page request/response. |
| Restart identity and ordering | Positively stop the server; restart the same executable on exactly the same absolute `OPENCODE_DB`; read Session and prior user/assistant/tool projections through V2 APIs. | IDs and native durable message order match before restart; SQLite physical store and row identities confirm the same store. Do not sort by lexical IDs or timestamps. |
| Exclusive saved-cursor suffix | Read finite history after an observed event cursor before and after restart, plus after the final observed cursor. | Anchor is excluded, suffix matches the complete history filtered strictly after that sequence, and exhausted cursor returns no events with `hasMore: false`. |
| Event-stream disconnect/reconnect | Read `GET /api/session/{sessionID}/event`, disconnect at an observed durable cursor, reconnect with `after` set to that cursor and collect through a positively known final native event. | Replayed `[event ID, durable.seq]` suffix equals finite history, excluding the anchor; prefix plus suffix reconstruct the public history with neither loss nor duplicate events. A deadline bounds failure only. |

Read-only queries on the **observed** absolute DB can start with these commands once real Session IDs exist:

```sql
SELECT id, session_id, delivery, admitted_seq, promoted_seq, prompt
FROM session_input WHERE session_id = :session_id ORDER BY admitted_seq;
SELECT id, session_id, type, seq, data
FROM session_message WHERE session_id = :session_id ORDER BY seq;
SELECT id, aggregate_id, seq, type, data
FROM event WHERE aggregate_id = :session_id ORDER BY seq;
```

`event` includes private/internal history, while finite Session history filters through the public durable manifest. Match returned public IDs to native rows instead of assuming that all DB events must be public or that adjacent public sequences must be consecutive. The release source's `EventV2.readAggregate` owns filtering-before-pagination and exclusive `after`; `V2Session.messages` owns durable message order. `V2Session.wait` joins current execution and must not start post-crash recovery of ambiguous promoted/provider-dispatched work.

## Publication

Update the existing task PR with this run's admissions, terminal assistants/tools, wait IDs/responses, full pre/post-restart histories, page concatenation and reconnect comparisons. Only passing executed release observations can satisfy the producer dependency. Keep the consumer source trace at its recorded revision and link [swxtchio/swx-abbe#1556](https://github.com/swxtchio/swx-abbe/issues/1556) for the real V2 serving-path replacement/refusal proof gap. Persistent-cohort V1 inventory and any ADR-owner-approved migration remain independent rollout gates.
