# Retirement round-2 correction

Authority: the round-2 order against `bfdbe712f5`.

- Gate the entire directory retirement on the target's own absence, before apply, under the write lock and immediately before deletion. Existing targets return a zero-change no-op; uncertain targets refuse.
- Preserve unresolved session identities and report lexical in-scope uncertainty as retained rather than outside the target.
- Distinguish existing-target no-op from missing-target/no-matching-session no-op without claiming prior retirement history.
- Correct the existing-target/missing-child regression and add both restoration-window and mixed uncertainty proofs. Keep removed-target partial retirement intact.
- Run focused passing/red proofs and package checks, push the same draft, verify head and file done.
