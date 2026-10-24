# Source-pinned release production plan: d7f78881

This release carries [#354](https://github.com/swxtchio/swx-opencode/pull/354), V2 small-model parity for session
create, runner titles and compaction (swxtchio/swx-abbe#1953), on top of everything `v1.18.32-swxtch.cd3f0ce2` shipped
(#350's V2 plugin-boot gate and #347's V2 env config). Abbe needs it to restore DEV customer chat
(swxtchio/swx-abbe#2030). It supersedes `v1.18.32-swxtch.cd3f0ce2`. Firstmate owns review of this evidence branch;
the binary source is fixed separately.

## Pickup identity

On 2026-10-24, `git fetch origin swxtch` selected immutable source `d7f78881560aa83e9390f0231562fa79639eacd9`, the
#354 merge and the tip of `swxtch`. The fork default branch read from GitHub is `swxtch`. The isolated task worktree
was clean at pickup. `packages/opencode/package.json` is `1.18.32`. `local-release.sh` names a release
`<package.json version>-swxtch.<source sha8>`, so the version is `1.18.32-swxtch.d7f78881` and the tag is
`v1.18.32-swxtch.d7f78881`; `git ls-remote origin` returned no such tag. The root `package.json` pins `bun@1.4.2`,
and `sha256sum bun.lock` is `2c6631f22941c4367a66410ae866c07fa8d4d141798dfba728e46d4c3c400151`.

No file holds a version to bump: the version comes from the source commit, as in every earlier cut. This branch
changes only evidence, notes, the plan and its `CHANGESET.md` entry.

| Required landed change | Commit |
| --- | --- |
| V2 small-model parity, #354 | `d7f78881560aa83e9390f0231562fa79639eacd9` |
| V2 turns wait for location plugin boot, #350 | `cd3f0ce2d5e55944df41fcf43a167f77e40c8483` |
| V2 env-supplied config and default model, #347 | `774d90c7fb02b9f0962620971630a4f09c1a886b` |
| Independent small models, #342 | `bce37c276f4070e3c9d343baa61ce5f35ca38dc9` |
| Fast-suite observer hardening, #329 | `9340c4d4a88b3c9f1f5274e69e72bd8c631a5053` |
| Output-limit continuation, #325 | `05a4d4194ca4306ae9f11758f66e56d8fb1cf295` |
| Published-native V2 history evidence, #331 | `cbfcad62a6fe04475dae23545c6296976f53fa5d` |
| Existing release-probe V2-wait requirement | `4b87478ddf` |

### Brief discrepancy

The brief names swxtchio/swx-opencode#353 as a merged change to carry. #353 is an open issue (online session
reclamation), not a pull request, and nothing on `swxtch` resolves it. This release does not claim it, and
`CHANGESET.md` gets no #353 entry. #354's entry is already present.

## Execution plan

1. Open this branch's single draft evidence PR against `swxtch` with its session stamp, and record its number in
   `CHANGESET.md`.
2. Native x64: the existing `release-candidate` job of `opencode-arm-release-x64.yml`, dispatched from `swxtch` at
   the source commit with explicit `source_commit`, `release_tag`, `version` and `candidate=true`.
3. Native arm64 and the jail rig on `vm14-001`, under one lease (convention v3): acquire the single `ACTIVE` lease
   as task `release-cut-5`, write `lease.json`, heartbeat while work runs, release with `release-lease.sh` after the
   lap, and reap task processes. All writes stay under `vm14-001:/home/byates/vm14-builds/opencode-release-d7f78881`
   (the rig's scratch), plus the rig's Docker image and networks, which are removed after the lap. The arm64 build
   is cut-4's `arm-build.sh` with only root, source, version and ancestry changed: build the unmodified source with
   pinned `OPENCODE_VERSION` and `MODELS_DEV_API_JSON`, then run the unchanged `release-probe.ts` on the extracted
   package.
4. Rerun #354's focused tests and the carried #347/#350 core tests at the source.
5. Jail gate: build Abbe's jail image from the unpublished, native-tested arm64 archive (the rig doc's §1 build
   with the download replaced by the local candidate, keeping the Dockerfile's digest and version checks), then run
   `jail_v2_egress_probe.py v1-control v2 v2-named compact`. Publication requires every mode to pass; `compact` is
   #354's acceptance case. A failure stops the cut with `needs-decision` and nothing is published.
6. The x64 rig has no local Docker host (`vm001` and `vm14-001` are arm64). That absence is a stated caveat in the
   release notes and the evidence, as in cut-4; Abbe rig-tests the published bits on both architectures.
7. Publish both archive/companion pairs at the source as Latest with the prior release's body structure, then
   verify Latest, tag, body and downloaded bytes against the native-tested candidates.
8. Finish evidence under `docs/verification/release-cut-5/`, `RELEASE-CUT-5-EVIDENCE.md`,
   `RELEASE-CUT-5-NOTES.md` and the changeset entry. Delivery to Abbe and SaaS is firstmate's: this brief
   authorizes no write into another home, so the release notice goes on this task's own wake log.

No production or release/probe tooling change is planned.
