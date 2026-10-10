// Fixture-only: counts the statement attempts a SQLite driver makes, for the busy tests in core and opencode. The
// runtime never imports this module. Clients prepare a statement once and retry its execution, so each execution of a
// counted statement is one attempt, and an attempt that throws SQLITE_BUSY is recorded by its native code.
import { Effect } from "effect"

export type StatementAttempts = { count: number; busy: Array<string> }

export const newStatementAttempts = (): StatementAttempts => ({ count: 0, busy: [] })

const counted = new WeakMap<object, StatementAttempts>()

export const countStatement = <S extends object>(statement: S, attempts: StatementAttempts) => {
  if (counted.get(statement) === attempts) return statement
  counted.set(statement, attempts)
  for (const method of ["all", "values"]) {
    const execute: unknown = Reflect.get(statement, method)
    if (typeof execute !== "function") continue
    Object.defineProperty(statement, method, {
      configurable: true,
      writable: true,
      value: (...params: unknown[]) => {
        attempts.count++
        try {
          return execute.apply(statement, params)
        } catch (error) {
          const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined
          if (typeof code === "string" && code.startsWith("SQLITE_BUSY")) attempts.busy.push(code)
          throw error
        }
      },
    })
  }
  return statement
}

// Patches bun:sqlite's query method, so each statement whose SQL matches counts its attempts. The patch is removed
// when the scope closes.
export const patchBunQuery = Effect.fn("patchBunQuery")(function* (
  match: (sql: string) => boolean,
  attempts: StatementAttempts,
) {
  const sqlite = yield* Effect.promise(() => import("bun:sqlite"))
  const prototype = sqlite.Database.prototype
  const descriptor =
    Object.getOwnPropertyDescriptor(prototype, "query") ??
    (yield* Effect.die(new Error("bun:sqlite query method was not found")))
  const query: typeof prototype.query = descriptor.value
  Object.defineProperty(prototype, "query", {
    ...descriptor,
    value: function (this: InstanceType<typeof sqlite.Database>, sql: string) {
      const statement = query.call(this, sql)
      return match(sql) ? countStatement(statement, attempts) : statement
    },
  })
  yield* Effect.addFinalizer(() => Effect.sync(() => Object.defineProperty(prototype, "query", descriptor)))
})
