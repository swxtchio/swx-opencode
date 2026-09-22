# Plan: #10 environment-sensitive tests

Root causes established before writing any fix. The three groups have
genuinely different causes and only one of them is a timeout story.

## A. The CLI trio — the harness hides its own timeout

packages/opencode/test/lib/cli-process.ts catches AppProcessError (which
covers BOTH a timeout and a spawn failure) and synthesizes
`{ exitCode: -1, stderr: <message> }`. So a run killed at the deadline fails
as `expected 0, received -1`, indistinguishable from a behavioural break
unless someone reads stderr. That is exactly the cost #10 describes: every
author re-deriving whether their failure is real.

Failing in CI, all within ~400ms of the 30s deadline:
  - unknown stream finish preserves partial output and continues
  - --format json records an unknown stream finish and continuation
  - --format json preserves reasoning, tool, and continuation ordering

Note the same file already fixed one instance of this by demoting elapsed
time to a backstop (run-process.test.ts:65-75). That is the template.

## B. write.test.ts file permissions — umask, not flake

  this box umask 0002 -> Bun.write yields 664
  CI         umask 0022 -> 644
  the test asserts exactly 0o644
  packages/opencode/src/tool/write.ts sets NO mode

So the test passes in CI by coincidence, and its name claims an intent the
code does not implement. Forcing 0644 in the tool would be WRONG - it would
loosen permissions for a user with a strict umask (0077 wants 0600). The
tool is right; the assertion is environment-dependent.

## C. snapshot-tool-race — passes in isolation, fails in a full run

Needs a real synchronisation point or isolation, not a longer deadline.

## Out of scope for this change set, to be filed separately

The two packages/app e2e Playwright specs. Different subsystem, different
tooling, and e2e has now passed three consecutive runs after one failure
that needed three attempts within a single run to reproduce - a different
shape from the unit timeouts and not to be lumped with them.
