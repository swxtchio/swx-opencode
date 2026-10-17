# OpenCode fork release evidence

## Plan

- Pin the fetched `swxtch` commit as the immutable release source; verify the V2 wait implementation, fork version rule, lockfile identity, and clean source tree.
- Classify ARM64 and x64 targets from the actual build configuration and execute each archive in a matching runtime with its required FFF native library.
- Run the focused `SessionV2.wait` and HTTP tests, prove the prior unavailable-wait implementation makes them fail, and exercise wait/restart/history plus database-path selection from the extracted release binary.
- Publish the source tag and both architecture archives with checksums, provenance, and executed version attestations; hand the image-pair work to the separately assigned Abbe task.
