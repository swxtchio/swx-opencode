# Release-cut plan

Authority: the firstmate release-cut task context, reproduced in the task's generated brief. The selected release source is fixed by that context; task-branch evidence and harness commits do not change it.

1. Keep the selected source immutable. Derive its version from the selected package and `local-release.sh`, verify the V2 wait and carried-fix ancestry, and retain native binary identity evidence.
2. Add a prepublication native x64 candidate path alongside the existing published-release proof workflow. Validate explicit source inputs before building, build the clean selected source without overlays, package its FFF sidecar, and run the existing native probe against the extracted candidate. Preserve the resulting archive, checksums, input hashes, native-host observations, and Actions artifact linkage.
3. Cover changed release-input behavior at the existing release-tool test boundary, including bounded failure when that behavior is removed. Run package typecheck and focused tests before handing the harness changes to firstmate's review path.
4. Once the task's VM14 host/root/purpose authorization is established, build ARM64 natively with the selected source's all-target command and run the same acceptance probe against the packaged candidate.
5. Publish only the native-tested candidate bytes, using the designated prior-release body as the provenance structure. Read back the tag, complete body and assets, download and revalidate the published digests, and deliver the release pointers to the established task and peer destinations.

The task brief's external-write authorization and completion-thread questions remain open at pickup. This plan permits worktree-local harness work while preserving those dependent boundaries. The subsequent native-history dispatch remains firstmate-owned.
