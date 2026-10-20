# Fast-suite review-round 6 plan

1. Preserve every supported Bun test option from `scripts.test`, keep exact manifest path selection, and parse leading environment assignments with Bun-compatible double-quote backslash behavior; fail closed on unsupported options.
2. Read the owning package Turbo test task and run its declared `^build` preparation with Turbo before direct tests. Fail closed on unmodeled dependencies or explicit inputs rather than silently skipping them.
3. Propagate pre/post hook failures; always run `posttest` after an attempted pretest/test, including when an earlier hook or test failed. Refuse nonzero targets without JUnit as an attributed failure, not an execution-set mismatch.
4. Add regressions for Turbo preparation/options, hook failure propagation and ordering, and backslash-bearing environment values; fix fixture claims and update help, PR notes, and the scoped refusal documentation.
