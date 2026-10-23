# Source-pinned release production plan: cd3f0ce2

Refs swxtchio/swx-opencode#338. This release carries swxtchio/swx-opencode#350, the V2 boot-gate fix that runs a
V2 turn only after its location's plugins boot, on top of #347's V2 env-supplied config and egress default-model
fix. Abbe B-16.10 (swxtchio/swx-abbe#1951) adopts it to restore DEV chat (swxtchio/swx-abbe#2030). The previous
release, `v1.18.32-swxtch.774d90c7`, failed Abbe's jail V2 egress rig; that rig is now a publication gate.
Firstmate owns review of this evidence branch; the binary source is fixed separately.

## Pickup identity

On 2026-10-23, `git fetch origin swxtch` selected immutable source `cd3f0ce2d5e55944df41fcf43a167f77e40c8483`.
The fork default branch read from GitHub is `swxtch`. The isolated task worktree was clean at pickup.
`packages/opencode/package.json` is `1.18.32`, giving version `1.18.32-swxtch.cd3f0ce2` and tag
`v1.18.32-swxtch.cd3f0ce2`; `git ls-remote origin` returned no such tag. The root `package.json` pins
`bun@1.4.2`, and `sha256sum bun.lock` is `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

`git merge-base --is-ancestor <commit> cd3f0ce2d5e55944df41fcf43a167f77e40c8483` exited successfully for each:

| Required landed change | Commit |
| --- | --- |
| V2 turns wait for location plugin boot, #350 | `cd3f0ce2d5e55944df41fcf43a167f77e40c8483` |
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
   `source_commit`, `release_tag`, `version` and `candidate=true`.
3. Native arm64: under firstmate's grant `vm14-001:/home/byates/vm14-builds/opencode-release-cd3f0ce2`
   (`data/opencode-release-cut-4/brief.md`, "Firstmate grant"), acquire the single `ACTIVE` lease as task
   `opencode-release-cut-4`, heartbeat, build the unmodified source with pinned `OPENCODE_VERSION` and
   `MODELS_DEV_API_JSON`, run the unchanged `release-probe.ts` on the extracted package, reap task processes and
   release with `release-lease.sh`.
4. Rerun the focused small-model command from `plans/small-model-verification.md`, #347's core config tests and
   #350's own tests.
5. Jail gate: build Abbe's jail image from the unpublished, native-tested archive (the rig doc's Dockerfile with
   the download replaced by the local candidate, same digest and version checks), then run
   `jail_v2_egress_probe.py v1-control v2 v2-named`. Publication requires every mode to pass.
6. Publish both archive/companion pairs at the source with the prior release's body structure, then verify
   Latest, tag, body and downloaded bytes against the native-tested candidates.
7. Append the verified notice to the revalidated Abbe and SaaS task wake logs, retain read-back receipts, and
   finish evidence and the changeset entry on this draft.

No production or release/probe tooling change is planned.
