# Fast suite implementation plan

1. Audit file-level candidates for real waits, polling bounds, subprocesses, preloads, and fixture dependencies; keep unsupported timing classes out of the explicit selection and record the evidence in the PR.
2. Add a documented, validated `tests/fast-suite.txt` mode to `tests/run.sh`, refusing dirty or unusable runs with terminal verdicts while preserving the existing bare full-suite Turbo command and its arguments.
3. Add executable runner regressions under `script/` that use committed disposable repositories to observe selected execution, package context, refusals, and fast/full test failures through the real entrypoint.
4. Record the fork changeset and hand off the immutable-head fast gate to firstmate; report the active repository override that still selects the full runner.
