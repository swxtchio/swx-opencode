# Value-aware launch option plan

Scope: the round-6 option-arity family for #324 at reviewed head `3e56318c6e`.

- Replace implicit boolean fallback with explicit Bun/Node option tables for required values, booleans, optional values, and eval boundaries. Unknown/missing-value options cannot certify a successful observed run.
- Drive the omitted Bun path-ignore/timings and Node localstorage/coverage options through real lifecycle launches, preserving user-argument, inspector, native worker, and job-control behavior.
- Add real ordinary-code value-option positives and compare parser behavior with the installed runtimes' emitted help surfaces. Red-proof omission handling and conservative fallback, then restore the complete focused matrix.
- Include the shared helper in fork-script typechecking, record evidence in the existing PR, commit/push the same branch, verify its head, and file done.
