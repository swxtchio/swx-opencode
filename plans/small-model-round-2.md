# Small-model round-2 CI repair

Authority: the captain's round-2 order for swxtchio/swx-opencode#342 and failed release-proof run `39723137251` at `fdb4352cc1`.

- The small-model fixture and public Session property are already committed. The failed job overlays the current monolithic HTTP test into older release source `0ff0dfbc`, creating the missing dependency/type mismatch.
- Give the release wait-registration regression a focused test module, and overlay/run that module instead of unrelated current HTTP-session cases. Keep the selected release runtime and build provenance intact.
- Verify the focused wait regression against current and selected-release sources, run the complete current HTTP-session test file and changed-package typechecks before pushing, and reconcile the workflow's existing reviewed digest pins.
- Push the same draft, verify the new remote head and a successful CI run on it, then report done.
