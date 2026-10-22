# Published native V2 verification plan

Task: [swxtchio/swx-abbe#1227](https://github.com/swxtchio/swx-abbe/issues/1227). Evidence and recommendation only; consumer implementation and cohort migration are outside this task.

1. Bind the downloaded `v1.18.32-swxtch.d1b232e9` linux-x64 archive, checksum companion, executable and packaged sidecar to the release provenance and selected source. Inspect the database selector and V2 contracts at that source.
2. Execute real V2 HTTP admissions against that executable with a controlled local provider and an isolated absolute native database. Record idempotent retries, readable native projections/events and truthful waits across held provider/tool/continuation, completion, terminal failure, interruption, idle and admit-only controls.
3. Restart on the same database. Compare message identities and native order, page finite history to exhaustion using exclusive durable cursors, and compare event-stream reconnect replay with finite history.
4. Trace the actual Abbe SessionPool caller at a recorded consumer revision; preserve ADR0031's pipeline-snapshot amendment and distinguish tested producer capability from consumer/replacement/cohort evidence. Publish a dated recommendation in this draft PR.
5. Retain runnable commands, raw observations and evaluated results; complete the crew mechanical gates and push the existing draft for firstmate review.

Acceptance observations must come from the published executable. Prior release results and source fixtures are reproduction leads. This evidence-only change claims no changed-production-behavior regression absence proof.
