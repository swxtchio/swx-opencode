# Node 24 differential compatibility plan

Scope: round 7, CI run 39716958454 at reviewed head `11f2e487d2`.

- Reproduce the help differential on installed Node 24.16/24.22 under the pinned Bun test host, then classify the current Node 24 additions without weakening unknown-option refusal.
- Read option arity per spelling, preserving genuine aliases and non-consuming optional values rather than assigning one arity to every comma-separated name.
- Keep Bun's pinned help surface strict; for future Node help additions verify either known classification or explicit fail-closed rejection. Preserve the real value-option positives that detect important table omissions.
- Demonstrate version/alias/optional regression reds, restore focused and maintenance checks on Node 24 and Bun, publish the same PR head, verify fresh CI, and file done.
