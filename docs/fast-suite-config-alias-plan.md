# Config-file alias arity plan

Scope: round 8 at reviewed head `0a88d13e51`.

- Treat Node's version-dependent bare `--experimental-config-file` as unsupported rather than consuming the next argv element. Preserve the stable attached `=path` form and the default-config Boolean spelling.
- Reproduce the bare alias followed by `--test` under Node 24.21/24.22 with a real default config and isolation disabled, so a child worker cannot supply an incidental refusal.
- Exercise the attached form as a valid ordinary-code control; update the help check to test supported syntax instead of treating help annotations as runtime arity authority.
- Red-proof the old consumption, restore focused/mechanical checks, publish the same branch, verify head and fresh CI, and file done.
