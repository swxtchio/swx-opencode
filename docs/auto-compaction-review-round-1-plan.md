# Auto-compaction review round 1 plan

1. Keep the setting project-scoped; make Config.Service.update persist through the loaded project opencode.json(c) source, then replace the stand-in PATCH fixture with a real server-backed persistence and execution regression.
2. Keep soft-threshold compaction deferred during an active drain, including steers; allow an immediate threshold compaction only at the first provider boundary of a newly idle runner.
3. Preserve capacity-triggered maintenance when structured output completes, while settling the structured answer before maintenance.
4. Resume automatically after automatic compaction: use an admitted pending prompt when present, otherwise continue the compacted work; retain capacity and reasoning-token coverage.
5. Prove each changed behavior with bounded producer-driven red/green tests, then run focused checks, commit, and push to the existing draft PR.
