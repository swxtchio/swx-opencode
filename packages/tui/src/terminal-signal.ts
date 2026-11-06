// Keep the app-level handler terminal-only; the OpenTUI renderer has its own exit-signal listener.
export function registerTerminalSighup(handler: () => void) {
  if (!process.stdin.isTTY && !process.stdout.isTTY) return () => {}
  process.on("SIGHUP", handler)
  return () => {
    process.off("SIGHUP", handler)
  }
}
