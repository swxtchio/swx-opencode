# Small-model review round 1

Authority: the captain's round-1 order for swxtchio/swx-opencode#342 at `0885b96167`.

- Carry the originating agent/session/global configured selection into new TaskTool children; preserve a child's own agent override and inherit stored session selection when forking.
- Serialize the Session patch read and durable update through the existing immediate database transaction boundary, including event projection, so overlapping setters cannot replace each other's fields.
- Publish the compaction model-resolution error through `session.error` before failing, with no substitute request.
- Add production-boundary child/fork, forced-interleaving patch and compaction-error regressions. Demonstrate bounded failures with each fix removed, restore the code, run focused tests and affected typechecks, and update the same draft PR.
