import { expect, test } from "bun:test"
import path from "node:path"
import { resolvePath } from "../../src/database/resolve-path"
import { InstallationChannel } from "../../src/installation/version"

const data = path.resolve("opencode-database-path-data")
const absolute = path.resolve("custom.db")
const cases = [
  {
    database: absolute,
    data,
    channel: "local",
    disableChannelDb: undefined,
    expected: absolute,
  },
  {
    database: "custom.db",
    data,
    channel: "local",
    disableChannelDb: undefined,
    expected: path.join(data, "custom.db"),
  },
  {
    database: ":memory:",
    data,
    channel: "local",
    disableChannelDb: undefined,
    expected: ":memory:",
  },
  {
    database: undefined,
    data,
    channel: InstallationChannel,
    disableChannelDb: undefined,
    expected: path.join(data, "opencode-local.db"),
  },
  ...["latest", "beta", "prod"].map((channel) => ({
    database: undefined,
    data,
    channel,
    disableChannelDb: undefined,
    expected: path.join(data, "opencode.db"),
  })),
  {
    database: undefined,
    data,
    channel: "feature/channel",
    disableChannelDb: undefined,
    expected: path.join(data, "opencode-feature-channel.db"),
  },
  ...["1", "true"].map((disableChannelDb) => ({
    database: undefined,
    data,
    channel: "local",
    disableChannelDb,
    expected: path.join(data, "opencode.db"),
  })),
]

test("database path selection preserves explicit, channel, and disabled-channel names", () => {
  expect(cases.map((input) => resolvePath(input))).toEqual(cases.map((input) => input.expected))
})
