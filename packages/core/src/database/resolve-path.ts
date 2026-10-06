import { isAbsolute, join } from "path"

export function resolvePath(input: {
  database: string | undefined
  data: string
  channel: string
  disableChannelDb: string | undefined
}) {
  if (input.database) {
    if (input.database === ":memory:" || isAbsolute(input.database)) return input.database
    return join(input.data, input.database)
  }
  if (
    ["latest", "beta", "prod"].includes(input.channel) ||
    input.disableChannelDb === "1" ||
    input.disableChannelDb === "true"
  )
    return join(input.data, "opencode.db")
  return join(input.data, `opencode-${input.channel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}
