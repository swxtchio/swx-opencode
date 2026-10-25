# Source-pinned release production plan: 832df553

This cut publishes the merged V2 small-model work in #358 and project-tool work in #361, which closes #352, for Abbe's MVP acceptance. The binary source is fixed at `832df553518d8bb581952433c72495a472da8c5d`; the older pin in Abbe #1951 predates both changes. The evidence branch may advance without changing that binary source.

## Pickup

- The live fork default is `swxtch`, and the isolated worktree starts at the selected source.
- The release identity follows `local-release.sh` and is validated by `release-probe-target.ts` from the selected package version, source commit, and Bun pin. Candidate inputs will explicitly name the source, derived version and tag, and `candidate=true`.
- This evidence-only branch will carry the plan, release notes, observed verification artifacts, evidence summary, and its own qualified `CHANGESET.md` entry. No product or release-probe source change is planned.
- Open one draft evidence PR against `swxtch` after committing this plan. The assigned release issue is none; the referenced issues and PRs are carried work and consumer context.

## Candidate and consumer gates

1. Verify source ancestry for #358 and #361, and validate the source-derived candidate identity through the existing resolver.
2. Run the existing clean-source x64 `release-candidate` workflow with explicit candidate inputs. Preserve its native host/runtime, Bun pin and actual version, lock and models catalog digests, clean-source observations, build inputs, packaged archive/companion validation, contents, executable/sidecar digests, extracted version, native probe, and provenance. Keep the workflow harness revision distinct from the binary source.
3. Build and probe the arm64 package on `vm14-001` under the task lease, following the current lease instructions and the cut-5 builder path. Preserve the same source, input, package, probe, and provenance evidence.
4. Stage the clean current Abbe consumer source into the authorized scratch and build its jail image from the exact unpublished arm64 candidate, retaining the Dockerfile digest and version checks. Strengthen `_brain_rig` acceptance so location-only and named-provider V2 turns each advertise and execute a valid call to the real staged `propose_netlist` module, persist its successful tool outcome/output, continue through the provider, and complete their V2 wait. Retain the V1 control and manual-compaction transcript/summary checks. A jail result is attributed only to the candidate bytes copied into that image; report x64 jail coverage only if its candidate is run there.
5. Run the focused small-model, auxiliary-model, project-tool, and V1 registry test files named by the task from their package directories, on the selected source. Record exact commands and final summaries.

## Publication and handoff

Before publishing, recheck the remote tag and release identity; a conflict stops publication. Publish both `opencode-linux-arm64.tar.gz` and `opencode-linux-x64.tar.gz` with their `.sha256` companions as the Latest release, using notes that state the carried changes and measured architecture coverage. Read back the tag's peeled commit, release target/status/Latest identity/body/assets, download each archive and companion afresh, validate companions, compare each archive byte-for-byte with its native-tested candidate, and compare extracted executable/sidecar digests. Keep the raw provenance, logs, image-byte comparison, rig request/history/wait evidence, test results, and read-back records under `docs/verification/release-cut-6/`.

Publication does not authorize merging this evidence PR or changing Abbe's pin or deployment. After successful read-back, hand firstmate the release URL, immutable source, tag, full asset digests, native probe results, tool/compaction rig results, architecture coverage, and remaining consumer acceptance.
