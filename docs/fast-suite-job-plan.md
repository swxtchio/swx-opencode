# Job-control observer plan

Scope: the round-4 order for #324 at reviewed head `1141c01f16`, preserving live exec identity and kernel argv capture.

- Attach with PTRACE_SEIZE before releasing a pipe-gated child into its command. Linux ptrace(2)'s PTRACE_SEIZE and Group-stop sections distinguish synthetic child stops from job-control group stops.
- Keep signal injection at delivery stops; use PTRACE_LISTEN for PTRACE_EVENT_STOP with SIGSTOP, SIGTSTP, SIGTTIN, or SIGTTOU, waiting for SIGCONT instead of resuming user code.
- Exercise native waitpid(WUNTRACED), stopped-child pipe state, SIGCONT, and successful reaping through a shared executable fixture and the real runner. Prove the old/resume behavior red, restore green, and retain the existing signal/exec/launch matrix.
- Record evidence in the existing draft PR, run focused/mechanical checks, commit/push the same branch, verify the head, and file done.
