# V2 project tool modules

## Goal

Let V2 turns discover project-local tool modules through the native Location config path, advertise them before the provider request, and execute them through the canonical Core tool registry. Preserve the supported module contract represented by the V1 loader in `packages/opencode/src/tool/registry.ts` and `packages/plugin/src/tool.ts`.

## Plan

- Use V2 `Config.entries()` directory entries as the Location-scoped discovery source; preserve V1 scan paths, supported extensions, export naming, and isolation of broken modules.
- Adapt discovered plugin tool definitions into canonical Core tools and register them through the Location `Tools.Service`, preserving the advertised Zod schema, input validation, invocation/project context, and string or structured output.
- Ensure discovery finishes before `SessionRunner` materializes provider definitions; exercise both discovery and the outgoing request with modules written into temporary project tool directories.
- Cover registry conversion and settlement, Location isolation and recreation behavior, and durable V2 tool outcome/history. Keep application tools, built-ins, projects without local modules, and V1 behavior intact.
- Run changed test files whole, affected package typechecks, focused V1 registry tests, and implementation-removal proofs for each changed-behavior regression. Hand off the release continuation to firstmate after the draft PR is ready for review.
