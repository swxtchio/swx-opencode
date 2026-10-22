# Explicit post-crash join-only control — 2026-10-21

This supplements the completed-history restart battery; it does not change the accepted recovery design. The same published linux-x64 release executable and packaged sidecar were read back with their recorded digests before execution. The environment remained a task-owned full-system x86_64 Linux guest under QEMU TCG, with DFG/FTL JIT disabled and base JIT/FFI enabled. Stock native-hardware deployment and Abbe consumer/cohort cutover remain separately unestablished.

[Raw observations](join-only-run.json) began at `2026-10-21T09:04:32.534180Z`; all ten supplemental checks passed. [`join-only-commands.py`](join-only-commands.py) is the exact executed driver. It created its own absolute `OPENCODE_DB`, config/data/cache/state/tmp roots and fixture Git workspace under `.native-history/join-only-run-1` inside the guest task-worktree replica. It admitted through the actual V2 HTTP contract and read producer-written native rows; it inserted no message/event substitutes.

## Real in-flight work, then a crash

Session `ses_ea247dc4cffe7jcj4a8D2th5uG` admitted top-level user ID `msg_crash_control_20261021` at sequence 1. The provider fixture positively received the actual tool-capable model request and held its response. The release reported that Session active and returned the promoted user projection by the same ID. There was no completed terminal assistant. The owned release process PID 1485 was then killed with SIGKILL and reaped with exit -9; this was a process crash, not explicit native interruption or a successful turn.

Before restarting, the driver read the stopped native input/message/event rows. The restart used the same absolute SQLite file, with device/inode `64769` / `85857` both before and after. The native Session and prior user message remained readable by their original IDs.

## Wait observes uncertainty without recovering it

`POST /api/session/ses_ea247dc4cffe7jcj4a8D2th5uG/wait` returned HTTP 503:

```json
{"_tag":"ServiceUnavailableError","message":"Session wait outcome is not observable","service":"session.wait"}
```

This is the producer's typed unavailable result for promoted work lacking established terminal facts and a current execution owner. It was not treated as idle, completed, failed, or interrupted. Both process-local active reads around the post-restart waits were empty; the provider fixture still had exactly its original one work request, and stopped-versus-after-wait native input/message/event rows were identical. No explicit resume, recovery wake or prompt retry was submitted.

## Admit-only control remains pending

The same pre-crash server also admitted `msg_join_pending_20261021` into Session `ses_ea247724bffeWpRtzzVXwxEdHy` with `resume: false`, at sequence 1. After restart, its wait returned HTTP 200:

```json
{"data":{"type":"pending","admittedSeq":1,"messageID":"msg_join_pending_20261021"}}
```

Its native admission remained unpromoted and its history unchanged. Wait did not start execution of that durable pending input. The replacement server PID 1555 was positively reaped with SIGTERM exit -15 after capture. [Before-crash](join-only-before.log) and [after-restart](join-only-after.log) producer logs are retained alongside the raw responses and native rows.

After transferring the evidence, the task-owned guest PID 247230 was positively reaped with exit 0; its stopped receipt matched the current launch. The guest image and supplemental native store remain isolated scratch inside the task worktree.

## Reproduction and interpretation

Run `python3 join-only-commands.py <absolute-extracted-opencode> <fresh-absolute-proof-root>` in the documented x64 environment. Raw run SHA-256 is `051726febd68be36c8e22efa2be6879c8e5ae23a141f74c58b073192990deb96`; driver SHA-256 is `b2a5d0eca8f09fd9f28e08619e1d854cd913ff55019325392b6b35be76d8e9ea`. The fixture's readiness and provider-hold conditions are positive responses/signals; deadlines only bound failure.

The functional producer verdict remains passing with the material execution qualification. This control supplies direct runtime evidence for the join-only boundary in `specs/v2/session.md` and `V2Session.wait`: completed history is readable after restart, but ambiguous in-flight provider dispatch is not implicitly recovered by wait. It authorizes no consumer completion, blind retry, live migration or weakening of ADR0031's uncertain-turn recovery obligations. No production behavior or production regression tests changed, and no changed-production-behavior absence proof is claimed.
