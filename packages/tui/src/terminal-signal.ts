// A SIGHUP listener replaces nohup's inherited ignore, so only a terminal-owned TUI should claim it.
export function registerTerminalSighup(handler: () => void) {
  if (!process.stdin.isTTY && !process.stdout.isTTY) return () => {}
  process.on("SIGHUP", handler)
  return () => {
    process.off("SIGHUP", handler)
  }
}
