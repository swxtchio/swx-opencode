# Fast-suite review-round 5 plan

1. Parse literal leading environment assignments and the Bun test timeout from each package's `scripts.test`, retain exact-file direct execution from the package root, and run optional `pretest`/`posttest` hooks directly.
2. Keep only the independently reachable JUnit execution-set guard: reject any attributed suite outside the selected manifest file. Document that missing attribution/extra-file cardinality and aggregate count/order are structural under one-file sequential dispatch; keep active-test coverage validation.
3. Extend the runner fixture to prove package environment, timeout, hooks, and a reachable unexpected-suite rejection; correct the help, test description, manifest Turbo-task note, and changeset description.
4. Run the focused fork-maintenance tests and script checks, commit and push to the existing draft PR, then verify the live head and append the completion wake.
