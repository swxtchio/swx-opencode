# Fix-round 2 plan

- Replace client-only held-wait claims with producer-observed status/body receipts before holds release; exercise suppressed wait dispatch and require a bounded red result.
- Give every spawned process and fetch request explicit timeout cleanup: terminate process groups, reap children, drain or abort pipes, and abort in-flight requests before the probe exits.
- Resolve release tag/source commit/version from `workflow_dispatch` inputs or a `repository_dispatch` payload, defaulting to the published tag; verify the resolved identity and the selected source's Bun version instead of trusting committed pins or the triggering PR's package metadata.
- Add focused absence-red/restored-green coverage for each repair, rerun the full native ARM and x64 archive proof battery, update PR #282, and leave the already-published release unchanged.
