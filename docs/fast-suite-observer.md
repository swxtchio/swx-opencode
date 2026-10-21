# Fast-suite observer hardening

## Plan

Scope: [swxtchio/swx-opencode#324](https://github.com/swxtchio/swx-opencode/issues/324), including its user-facing documentation advisory. The full suite remains the release gate; fast mode is a merge-gate speedup, not a security boundary.

- Drive absolute Bun, `bun run --bun`, and `node --test` launches through executable fixture packages and the real `tests/run.sh` boundary.
- Close each silent-success escape by observing extra tests or refusing unsupported launches, preserving the existing Turbo and direct-package positive controls.
- Pin non-build prerequisite admission and a producer-derived non-empty wrong-owner Turbo plan with pre-execution markers.
- Demonstrate each behavioral pin with a behavior-removing mutation, its failed assertion, and restored-green focused verification.
- Qualify the help, manifest header, and existing changeset claim with the observation scope and issue reference; add this PR's fork changeset entry.
- Run focused and fork-maintenance checks, then the applicable lint and commit-identity gates. Publish the single draft PR and hand it to firstmate for review.
