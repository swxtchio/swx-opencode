export * as Sqlite from "./sqlite"

import { Context } from "effect"
import type { drizzle } from "drizzle-orm/bun-sqlite"

export type DrizzleClient = ReturnType<typeof drizzle>
export class Native extends Context.Service<Native, unknown>()("@opencode-ai/core/database/SqliteNative") {}
export class Drizzle extends Context.Service<Drizzle, DrizzleClient>()("@opencode-ai/core/database/SqliteDrizzle") {}

export {
  acquirePermitLease,
  isBeginStatement,
  lockWaitMs,
  resetRetryWindow,
  retryWindowStatus,
  withPermit,
  withUnboundedPermitWait,
  withFreshRetryWindow,
  withRetryWindow,
} from "@opencode-ai/effect-sqlite-node/retry"
