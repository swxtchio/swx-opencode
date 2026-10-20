# Auto-compaction review round 2 plan

1. When the instance config update includes `compaction.threshold_percent`, update the last-loaded project `.opencode/opencode.json` or `.opencode/opencode.jsonc` source that explicitly owns the value; otherwise preserve the existing loaded root project config target.
2. Extend the live TUI/server regression with an `.opencode` threshold of 80, save 72 through Ctrl+P, and assert the owner file, effective backend config, and real prompt-triggered compaction/resumption all use 72.
3. Bypass the owner selection for a bounded red proof, restore it, then run focused checks and push to the existing draft PR.
