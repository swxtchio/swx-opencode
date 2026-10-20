# Fast-suite review-round 3 plan

1. Keep the package `test` script presence check, but bypass package scripts and Turbo for fast mode; invoke `bun test` directly from the owning package once per exact manifest file.
2. Parse each JUnit report, require its executed file set to be exactly the current target, aggregate those files against the manifest, and preserve a nonzero verdict when any target fails.
3. Make the fixture package scripts contain extra selectors so the current fast dispatch demonstrably runs unlisted files; cover path-shape, missing-script, and ignored/untracked-manifest refusals with targets that pass other guards.
4. Extend the #317 manifest and issue note with the additional packages missing full-suite Turbo tasks.
5. Run the focused mutation proofs and checks, review the diff, and update the existing draft PR.
