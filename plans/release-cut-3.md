# Source-pinned release production plan: 774d90c7

Refs swxtchio/swx-opencode#338. This release carries swxtchio/swx-opencode#347, the V2 env-supplied config and
egress default-model fix, which Abbe B-16.10 (swxtchio/swx-abbe#1951) adopts to restore DEV chat
(swxtchio/swx-abbe#2030). Firstmate owns review of this evidence branch; the binary source is fixed separately.

## Pickup identity

On 2026-10-23 at about 16:03Z, `git fetch origin swxtch` selected immutable source
`774d90c7fb02b9f0962620971630a4f09c1a886b`. The fork default branch read from GitHub is `swxtch`. The isolated
task worktree was clean at pickup. `packages/opencode/package.json` is `1.18.32`, giving version
`1.18.32-swxtch.774d90c7` and tag `v1.18.32-swxtch.774d90c7`; `git ls-remote origin` returned no such tag. The
root `package.json` pins `bun@1.4.2`, and `sha256sum bun.lock` is
`2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

`git merge-base --is-ancestor <commit> 774d90c7fb02b9f0962620971630a4f09c1a886b` exited successfully for each:

| Required landed change | Commit |
| --- | --- |
| V2 env-supplied config and default model, #347 | `774d90c7fb02b9f0962620971630a4f09c1a886b` |
| Independent small models, #342 | `bce37c276f4070e3c9d343baa61ce5f35ca38dc9` |
| Fast-suite observer hardening, #329 | `9340c4d4a88b3c9f1f5274e69e72bd8c631a5053` |
| Output-limit continuation, #325 | `05a4d4194ca4306ae9f11758f66e56d8fb1cf295` |
| Published-native V2 history evidence, #331 | `cbfcad62a6fe04475dae23545c6296976f53fa5d` |
| Existing release-probe V2-wait requirement | `4b87478ddf` |

## Execution plan

1. Open this branch's single draft evidence PR against `swxtch` with its session stamp, and record its number in
   `CHANGESET.md`.
2. Native x64: the existing `release-candidate` job, dispatched from `swxtch` at the source commit with explicit
   `source_commit`, `release_tag`, `version` and `candidate=true` (run 39983517672).
3. Native arm64: under firstmate's grant `vm14-001:/home/byates/vm14-builds/opencode-release-774d90c7`
   (`data/opencode-release-cut-3/firstmate-decisions.md`), acquire the single `ACTIVE` lease as task
   `opencode-release-cut-3`, heartbeat, build the unmodified source with pinned `OPENCODE_VERSION` and
   `MODELS_DEV_API_JSON`, run the unchanged `release-probe.ts` on the extracted package, reap task processes and
   release with `release-lease.sh`.
4. Rerun the focused small-model command from `plans/small-model-verification.md` and #347's core config tests.
5. Publish both archive/companion pairs at the source with the prior release's body structure, then verify
   Latest, tag, body and downloaded bytes against the native-tested candidates.
6. Append the verified notice to the revalidated Abbe and SaaS task wake logs, retain read-back receipts, and
   finish evidence and the changeset entry on this draft.

No production or release/probe tooling change is planned.
