import { expect, test } from "bun:test"
import path from "node:path"
import { resolvePath } from "../../src/database/resolve-path"

const data = path.resolve("opencode-database-path-data")
const absolute = path.resolve("custom.db")
const cases = [
  {
    database: absolute,
    data,
    channel: "fixture",
    disableChannelDb: undefined,
    expected: absolute,
  },
  {
    database: "custom.db",
    data,
    channel: "fixture",
    disableChannelDb: undefined,
    expected: path.join(data, "custom.db"),
  },
  {
    database: ":memory:",
    data,
    channel: "fixture",
    disableChannelDb: undefined,
    expected: ":memory:",
  },
  {
    database: undefined,
    data,
    channel: "fixture-channel",
    disableChannelDb: undefined,
    expected: path.join(data, "opencode-fixture-channel.db"),
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
    channel: "feature/channel",
    disableChannelDb,
    expected: path.join(data, "opencode.db"),
  })),
]

test("database path selection preserves explicit, channel, and disabled-channel names", () => {
  expect(cases.map((input) => resolvePath(input))).toEqual(cases.map((input) => input.expected))
})
