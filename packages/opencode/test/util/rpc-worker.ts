import { Rpc } from "../../src/util/rpc"

Rpc.listen({
  ready: (_input: undefined) => "ready",
  failSync: (message: string) => {
    throw new Error(message)
  },
  failAsync: async (message: string) => {
    await Promise.resolve()
    throw new Error(message)
  },
  failCause: (_input: undefined) => {
    throw new Error("", { cause: new Error("nested cause failure") })
  },
  succeed: (value: string) => {
    Rpc.emit("notice", value)
    return `success:${value}`
  },
})
