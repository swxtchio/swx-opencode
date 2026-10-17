import process from "node:process"

const terminateGraceMs = 1_000
const cleanupTimeoutMs = 10_000

export type ProcessResult = { exit: number; stdout: string; stderr: string }
export type SpawnInput = { cwd: string; env: Record<string, string | undefined> }
type CapturedStream = { result: Promise<string>; cancel: () => Promise<void> }

export function spawnProcess(command: string[], input: SpawnInput) {
  const child = Bun.spawn(command, {
    cwd: input.cwd,
    env: input.env,
    stdout: "pipe",
    stderr: "pipe",
    detached: process.platform !== "win32",
  })
  const stdout = capture(child.stdout)
  const stderr = capture(child.stderr)
  let stopping: Promise<void> | undefined

  return {
    child,
    stdout: stdout.result,
    stderr: stderr.result,
    stop: (label: string) => (stopping ??= stopProcess(child, [stdout, stderr], label)),
  }
}

export async function runProcess(command: string[], input: SpawnInput & { timeoutMs: number; label: string }) {
  const managed = spawnProcess(command, input)
  const completed = Promise.all([managed.child.exited, managed.stdout, managed.stderr])
  try {
    const [exit, stdout, stderr] = await withTimeout(completed, input.timeoutMs, input.label)
    await managed.stop(input.label)
    return { exit, stdout, stderr } satisfies ProcessResult
  } catch (error) {
    await managed.stop(input.label)
    throw error
  }
}

async function stopProcess(child: Bun.Subprocess, streams: CapturedStream[], label: string) {
  signalProcessGroup(child, "SIGTERM")
  try {
    await waitForGroupExit(child, terminateGraceMs, `${label} SIGTERM`)
  } catch {
    signalProcessGroup(child, "SIGKILL")
    await withTimeout(child.exited, cleanupTimeoutMs, `${label} direct child reap after SIGKILL`)
    await waitForGroupExit(child, cleanupTimeoutMs, `${label} process-group reap after SIGKILL`)
  }

  await withTimeout(child.exited, cleanupTimeoutMs, `${label} direct child reap`)
  try {
    await withTimeout(Promise.all(streams.map((stream) => stream.result)), cleanupTimeoutMs, `${label} output drain`)
  } catch (error) {
    await Promise.all(streams.map((stream) => stream.cancel()))
    await Promise.allSettled(streams.map((stream) => stream.result))
    throw error
  }
}

function signalProcessGroup(child: Bun.Subprocess, signal: NodeJS.Signals) {
  if (process.platform === "win32") {
    child.kill(signal)
    return
  }

  try {
    process.kill(-child.pid, signal)
  } catch (error) {
    if (!isMissingProcess(error)) throw error
  }
}

async function waitForGroupExit(child: Bun.Subprocess, timeoutMs: number, label: string) {
  await until(label, () => (process.platform === "win32" ? child.exitCode !== null : !processGroupExists(child.pid)), timeoutMs)
}

function processGroupExists(pid: number) {
  try {
    process.kill(-pid, 0)
    return true
  } catch (error) {
    if (isMissingProcess(error)) return false
    if (isPermissionDenied(error)) return true
    throw error
  }
}

function isMissingProcess(error: unknown) {
  return error instanceof Error && "code" in error && error.code === "ESRCH"
}

function isPermissionDenied(error: unknown) {
  return error instanceof Error && "code" in error && error.code === "EPERM"
}

function capture(stream: ReadableStream<Uint8Array>): CapturedStream {
  const reader = stream.getReader()
  const result = (async () => {
    const decoder = new TextDecoder()
    let text = ""
    while (true) {
      const chunk = await reader.read()
      if (chunk.done) return text + decoder.decode()
      text += decoder.decode(chunk.value, { stream: true })
    }
  })()
  return { result, cancel: () => reader.cancel().then(() => undefined, () => undefined) }
}

async function until(label: string, condition: () => boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string) {
  let timer: Timer | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}
