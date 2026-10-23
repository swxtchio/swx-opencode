# Bump swxtchio/swx-abbe#1951 (fm/mvp-b16-10) to OpenCode v1.18.32-swxtch.cd3f0ce2 (firstmate, 20:25Z)

The mvp-b16-10 crew is retired. Branch fm/mvp-b16-10 is on origin at 03d7e2344e232467e0dd4ffd89256990ac8c3537,
the head of PR #1951. Work in a fresh worktree on that branch.

Release: v1.18.32-swxtch.cd3f0ce2 (tag -> cd3f0ce2d5e5..., carries swx-opencode#350, the V2 egress fix).
Assets and their .sha256 companions are on the release (fm-llmrouter note: /tmp/opencode/abbe-rel4.md).
fm-llmrouter ran the jail rig on arm64 only (passed). x64 was NOT rig-tested.

Do:
1. Move the jail pin for BOTH arches to v1.18.32-swxtch.cd3f0ce2.
   - Compute each sha256 yourself from the downloaded asset, and check it against the published .sha256
     companion and the GitHub asset digest.
   - Follow the same pattern 03d7e234 used, including evidence section 0 and the plugin
     InstallationSdkVersion check.
2. Run the jail-boundary V2 egress rig (/home/byates/fm-abbe/data/mvp-b16-10/jail-v2-egress-rig.md):
   - x64: on abbe-build-001, which is x86 with docker, under its lease (data/tools/fs.sh lease convention).
     This closes fm-llmrouter's x64 gap.
   - arm64: wherever the rig ran before (vm14-001 is arm64), under its lease.
   - Both must pass v1-control, v2 and v2-named, with the provider receiving the model request.
3. Merge current origin/main into the branch (no rebase). Run focused jail tests. Push only fm/mvp-b16-10,
   verify the PR head equals the branch head, and leave the PR as it is.
4. Report done: with the new head, both shas, and rig results per arch.
   If either rig fails, report needs-decision: with the outputs.
Heavy runs go on the build boxes, never this host.
