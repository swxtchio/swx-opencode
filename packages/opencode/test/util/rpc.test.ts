import { expect, test } from "bun:test"
import { fileURLToPath } from "node:url"
import { Rpc } from "../../src/util/rpc"

type WorkerRpc = {
  ready: (input: undefined) => string
  failSync: (input: string) => never
  failAsync: (input: string) => Promise<never>
  failCause: (input: undefined) => never
  succeed: (input: string) => string
}

type Client = ReturnType<typeof Rpc.client<WorkerRpc>>

test("settles only the failed synchronous request and preserves success and event delivery", async () => {
  await withWorker(async (client) => {
    const notices: string[] = []
    client.on<string>("notice", (value) => notices.push(value))

    const failed = within(client.call("failSync", "synchronous failure"), "missing synchronous RPC result")
    const succeeded = within(client.call("succeed", "still alive"), "successful RPC did not settle")

    await expect(failed).rejects.toThrow("synchronous failure")
    await expect(succeeded).resolves.toBe("success:still alive")
    expect(notices).toEqual(["still alive"])
  })
})

test("rejects a pending call when its worker handler rejects asynchronously", async () => {
  await withWorker(async (client) => {
    await expect(within(client.call("failAsync", "asynchronous failure"), "missing asynchronous RPC result"))
      .rejects.toThrow("asynchronous failure")
  })
})

test("uses nested cause text when the worker error message is empty", async () => {
  await withWorker(async (client) => {
    await expect(within(client.call("failCause", undefined), "missing cause RPC result"))
      .rejects.toThrow("nested cause failure")
  })
})

async function withWorker(run: (client: Client) => Promise<void>) {
  const worker = new Worker(fileURLToPath(new URL("./rpc-worker.ts", import.meta.url)))
  const client = Rpc.client<WorkerRpc>(worker)
  try {
    await expect(within(client.call("ready", undefined), "worker did not acknowledge readiness")).resolves.toBe("ready")
    await run(client)
  } finally {
    await worker.terminate()
  }
}

function within<T>(promise: Promise<T>, message: string) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), 1_000)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}
