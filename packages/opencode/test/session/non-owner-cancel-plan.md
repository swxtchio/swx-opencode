# Plan: non-owner cancellation

- Preserve the existing status/event and `/session/status` contracts; a no-runner cancel may signal idle only when this process has a non-idle status entry.
- Add a production-driven regression around a held provider-backed owner turn and a separate process-local cancel stack sharing an isolated SQLite database.
- Observe both owner and non-owner event/status state, verify the assistant stays non-terminal through non-owner cancellation, release the owner, and assert its real completion.
- Include an active-local cancel control that observes both idle event forms; establish a bounded red result before the fix and a green focused test after it.
