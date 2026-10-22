# Source-pinned release production plan

Refs swxtchio/swx-opencode#338, resolved by swxtchio/swx-opencode#342. This task produces the binary release for downstream adoption; firstmate owns review of this evidence branch.

## Pickup identity

On 2026-10-22, `git fetch origin swxtch` selected immutable source `bce37c276f4070e3c9d343baa61ce5f35ca38dc9`. The isolated task worktree was clean at pickup. The selected package version is `1.18.32`, yielding version `1.18.32-swxtch.bce37c27` and tag `v1.18.32-swxtch.bce37c27`; the selected workspace pins Bun `1.4.2`. The source lockfile SHA-256 observed with `sha256sum bun.lock` is `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

`git merge-base --is-ancestor <commit> bce37c276f4070e3c9d343baa61ce5f35ca38dc9` exited successfully for each required commit:

| Required landed change | Commit |
| --- | --- |
| Independent small models, #342 | `bce37c276f4070e3c9d343baa61ce5f35ca38dc9` |
| Fast-suite observer hardening, #329 | `9340c4d4a88b3c9f1f5274e69e72bd8c631a5053` |
| Output-limit continuation, #325 | `05a4d4194ca4306ae9f11758f66e56d8fb1cf295` |
| Published-native V2 history evidence, #331 | `cbfcad62a6fe04475dae23545c6296976f53fa5d` |
| Existing release-probe V2-wait requirement | `4b87478ddf4a14ffb42cdffde4af48712a9b03b6` |

## Execution plan

1. Open this branch's single draft evidence PR against `swxtch`, verify its issue reference and session stamp, and record its actual number in `CHANGESET.md`.
2. Establish a current task-specific VM14 host/root/purpose grant and current lease-capacity rule before ARM staging or load. Acquire the atomic lease, retain ownership and heartbeat receipts, and confirm task-owned process cleanup before owner-matched release.
3. Rerun the source-owned focused small-model command in `plans/small-model-verification.md`. Build native ARM64 on VM14 and native x64 with the existing Actions `release-candidate` job, both from the unmodified captured source. Retain the actual catalog inputs, lockfile and runtime fingerprints, build logs, source-cleanliness observations, and separate harness identity.
4. Execute `packages/opencode/script/release-probe.ts` against each extracted packaged candidate. Retain native version/FFF, wait outcomes and producer identities, completed-history restart/replay, and physical database-selector evidence. Stop for firstmate if a required production repair would change the fixed binary source.
5. Prepare the complete prior-release body structure with this release's observations and all four assets. Validate transported checksums, publish at the captured source, verify Latest/tag/body/asset read-back, and download and compare all uploaded archive, executable and sidecar digests.
6. Revalidate Abbe and SaaS task recipients, append the verified release notice to their authorized task wake logs, and retain read-back and supplemental peer-delivery receipts. Finish the evidence and changeset on this same draft, run required mechanical gates, verify pushed-head equality, and report ready to firstmate.

## Current execution boundary

The new task-specific VM14 grant was read before staging and workload admission. Both native candidates passed the unchanged packaged acceptance battery, and the carried small-model focused run passed 59 tests with zero failures. The release is published and its Latest/tag/body/assets/downloaded bytes are verified. SaaS's required wake-log append and supplemental delivery are confirmed; Abbe delivery is ROUTED-VIA-FIRSTMATE under the supervisor's completion disposition and round-1 fix order. `RELEASE-CUT-2-EVIDENCE.md` records the route and the committed raw evidence location.
