# Small-model review round 1

Authority: the captain's round-1 order for swxtchio/swx-opencode#342 at `0885b96167`.

- Carry the originating agent/session/global configured selection into new TaskTool children; preserve a child's own agent override and inherit stored session selection when forking.
- Compare each Session patch's read snapshot with the current row inside the existing immediate durable-write transaction; retry a stale patch before it can replace another setter's fields, preserving the projector's missing-Session refusal.
- Publish the compaction model-resolution error through `session.error` before failing, with no substitute request.
- Add production-boundary child/fork, forced-interleaving patch and compaction-error regressions. Demonstrate bounded failures with each fix removed, restore the code, run focused tests and affected typechecks, and update the same draft PR.
