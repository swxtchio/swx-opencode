# Composer regressions (#285)

- Give persisted composer history an explicit session owner, exclude unowned legacy entries from session recall, and use the same retained source for Ctrl+r while preserving human provenance filtering.
- Make picker selection append original text at the draft end, preserve parts, close the picker, and return editing focus without submission.
- Preserve an omitted enhancement override through config resolution; choose the explicit override, live current model, then the approved Flash fallback at invocation.
- Extend producer-driven composer/history/config tests, demonstrate bounded red failures with each changed behavior removed, and rerun focused files and package typechecks.
- Record the draft PR in CHANGESET.md, publish the tested commits to that draft, and leave review and merge to firstmate.
