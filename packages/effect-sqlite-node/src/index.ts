export * as NodeSqliteClient from "./index"

import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import { identity } from "effect/Function"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, LockTimeoutError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import {
  acquirePermitLease,
  isBeginStatement,
  isCommitStatement,
  makeStatementGate,
  retryLocked,
  withPermit,
} from "./retry"

const ATTR_DB_SYSTEM_NAME = "db.system.name"
const nativeBusyTimeoutMs = 5

const statementError = (cause: unknown) => {
  const errcode =
    typeof cause === "object" && cause !== null && "errcode" in cause && typeof cause.errcode === "number"
      ? cause.errcode & 0xff
      : undefined
  const reason =
    errcode === 5 || errcode === 6
      ? new LockTimeoutError({ cause, message: "Failed to execute statement", operation: "execute" })
      : classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" })
  if (reason._tag !== "LockTimeoutError") return new SqlError({ reason })

  const code =
    errcode === 6
      ? "SQLITE_LOCKED"
      : typeof cause === "object" && cause !== null && "code" in cause && typeof cause.code === "string"
        ? cause.code.startsWith("SQLITE_LOCKED")
          ? cause.code
          : "SQLITE_BUSY"
        : "SQLITE_BUSY"
  return new SqlError({
    reason: new LockTimeoutError({
      cause,
      message: `Failed to execute statement: database is locked (${code})`,
      operation: "execute",
    }),
  })
}

export const TypeId: TypeId = "~@opencode-ai/effect-sqlite-node/NodeSqliteClient"
export type TypeId = "~@opencode-ai/effect-sqlite-node/NodeSqliteClient"

export interface SqliteClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: SqliteClientConfig
  readonly loadExtension: (path: string) => Effect.Effect<void, SqlError>
  readonly updateValues: never
}

export const SqliteClient = Context.Service<SqliteClient>("@opencode-ai/effect-sqlite-node/NodeSqliteClient")

export interface SqliteClientConfig {
  readonly filename: string
  readonly readonly?: boolean | undefined
  readonly create?: boolean | undefined
  readonly readwrite?: boolean | undefined
  readonly disableWAL?: boolean | undefined
  readonly timeout?: number | undefined
  readonly allowExtension?: boolean | undefined
  readonly spanAttributes?: Record<string, unknown> | undefined
  readonly transformResultNames?: ((str: string) => string) | undefined
  readonly transformQueryNames?: ((str: string) => string) | undefined
}

interface SqliteConnection extends Connection {
  readonly loadExtension: (path: string) => Effect.Effect<void, SqlError>
}

export const make = (
  options: SqliteClientConfig,
): Effect.Effect<SqliteClient, never, Scope.Scope | Reactivity.Reactivity> =>
  Effect.gen(function* () {
    const compiler = Statement.makeCompilerSqlite(options.transformQueryNames)
    const transformRows = options.transformResultNames
      ? Statement.defaultTransforms(options.transformResultNames).array
      : undefined

    const makeConnection = Effect.gen(function* () {
      const db = new DatabaseSync(options.filename, {
        readOnly: options.readonly,
        timeout: options.timeout ?? nativeBusyTimeoutMs,
        allowExtension: options.allowExtension,
        enableForeignKeyConstraints: true,
        open: true,
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => db.close()))

      if (options.disableWAL !== true && options.readonly !== true) {
        db.exec("PRAGMA journal_mode = WAL;")
      }

      const run = (sql: string, params: ReadonlyArray<unknown> = []) =>
        Effect.withFiber<Array<Record<string, unknown>>, SqlError>((fiber) => {
          const statement = db.prepare(sql)
          statement.setReadBigInts(Context.get(fiber.context, Client.SafeIntegers))
          const execute = Effect.try({
            try: () => statement.all(...(params as SQLInputValue[])) as Array<Record<string, unknown>>,
            catch: statementError,
          })
          return execute
        })

      const runValues = (sql: string, params: ReadonlyArray<unknown> = []) =>
        Effect.withFiber<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>((fiber) => {
          const statement = db.prepare(sql)
          statement.setReadBigInts(Context.get(fiber.context, Client.SafeIntegers))
          statement.setReturnArrays(true)
          const execute = Effect.try({
            try: () =>
              statement.all(...(params as SQLInputValue[])) as unknown as ReadonlyArray<ReadonlyArray<unknown>>,
            catch: statementError,
          })
          return execute
        })

      return identity<SqliteConnection>({
        execute(sql, params, transformRows) {
          return transformRows ? Effect.map(run(sql, params), transformRows) : run(sql, params)
        },
        executeRaw(sql, params) {
          return run(sql, params)
        },
        executeValues(sql, params) {
          return runValues(sql, params)
        },
        executeUnprepared(sql, params, transformRows) {
          return this.execute(sql, params, transformRows)
        },
        executeStream() {
          return Stream.die("executeStream not implemented")
        },
        loadExtension: (path) =>
          Effect.try({
            try: () => db.loadExtension(path),
            catch: (cause) =>
              new SqlError({
                reason: classifySqliteError(cause, { message: "Failed to load extension", operation: "loadExtension" }),
              }),
          }),
      })
    })

    const semaphore = yield* Semaphore.make(1)
    const statementGate = yield* makeStatementGate
    const connection = yield* makeConnection
    const withStatement = <A, E>(
      operation: { sawContention: boolean },
      effect: Effect.Effect<A, E>,
      retryPriority = false,
    ) => statementGate.withStatement(effect, operation, retryPriority)
    const transactionConnection = (
      scope: Scope.Scope,
      initialLease: Scope.Closeable,
      operation: { sawContention: boolean },
    ) => {
      const state: { lease?: Scope.Closeable; transaction: boolean } = { lease: initialLease, transaction: false }
      const acquireLease = () =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const lease = yield* restore(acquirePermitLease(semaphore, 1, operation))
            const registered = yield* Effect.exit(Scope.addFinalizer(scope, Scope.close(lease, Exit.void)))
            if (Exit.isFailure(registered)) {
              yield* Scope.close(lease, registered)
              return yield* Effect.failCause(registered.cause)
            }
            state.lease = lease
          }),
        )
      const retry = <A>(sql: string, effect: Effect.Effect<A, SqlError>) =>
        Effect.suspend(() => {
          if (state.transaction) return retryLocked(effect, sql, isCommitStatement(sql), operation)
          if (!isBeginStatement(sql)) {
            return retryLocked(effect, sql, true, operation, {
              wrapAttempt: (attempt, attemptNumber) =>
                withStatement(
                  operation,
                  Effect.suspend(() =>
                    (state.lease ? Effect.void : acquireLease()).pipe(Effect.andThen(attempt)),
                  ),
                  attemptNumber > 1,
                ),
              beforeRetry: () => {
                if (!state.lease) return Effect.void
                const lease = state.lease
                state.lease = undefined
                return Scope.close(lease, Exit.void)
              },
            })
          }
          return retryLocked(
            Effect.gen(function* () {
              if (state.lease) {
                const lease = state.lease
                state.lease = undefined
                yield* Scope.close(lease, Exit.void)
              }
              return yield* statementGate.withExclusive(
                Effect.uninterruptibleMask((restore) =>
                  Effect.gen(function* () {
                    const connectionLease = yield* restore(acquirePermitLease(semaphore, 1, operation))
                    const registered = yield* Effect.exit(
                      Scope.addFinalizer(scope, Scope.close(connectionLease, Exit.void)),
                    )
                    if (Exit.isFailure(registered)) {
                      yield* Scope.close(connectionLease, registered)
                      return yield* Effect.failCause(registered.cause)
                    }
                    const result = yield* Effect.exit(restore(effect))
                    if (Exit.isFailure(result)) {
                      yield* Scope.close(connectionLease, result)
                      return yield* Effect.failCause(result.cause)
                    }
                    state.lease = connectionLease
                    state.transaction = true
                    return result.value
                  }),
                ),
                operation,
              )
            }),
            sql,
            false,
            operation,
          )
        })

      return identity<SqliteConnection>({
        execute(sql, params, transformRows) {
          const result = retry(sql, connection.execute(sql, params, undefined))
          return transformRows ? Effect.map(result, transformRows) : result
        },
        executeRaw(sql, params) {
          return retry(sql, connection.executeRaw(sql, params))
        },
        executeValues(sql, params) {
          return retry(sql, connection.executeValues(sql, params))
        },
        executeUnprepared(sql, params, transformRows) {
          return this.execute(sql, params, transformRows)
        },
        executeStream() {
          return Stream.die("executeStream not implemented")
        },
        loadExtension(path) {
          return Effect.suspend(() =>
            state.lease
              ? connection.loadExtension(path)
              : withStatement(operation, withPermit(semaphore, connection.loadExtension(path))),
          )
        },
      })
    }
    const runWithPermit = <A>(sql: string, effect: Effect.Effect<A, SqlError>, operation: { sawContention: boolean }) =>
      retryLocked(withPermit(semaphore, effect), sql, true, operation, {
        wrapAttempt: (attempt, attemptNumber) => withStatement(operation, attempt, attemptNumber > 1),
      })
    const lockedConnection = (operation: { sawContention: boolean }) => identity<SqliteConnection>({
      execute(sql, params, transformRows) {
        const result = runWithPermit(sql, connection.execute(sql, params, undefined), operation)
        return transformRows ? Effect.map(result, transformRows) : result
      },
      executeRaw(sql, params) {
        return runWithPermit(sql, connection.executeRaw(sql, params), operation)
      },
      executeValues(sql, params) {
        return runWithPermit(sql, connection.executeValues(sql, params), operation)
      },
      executeUnprepared(sql, params, transformRows) {
        return this.execute(sql, params, transformRows)
      },
      executeStream(sql, params, transformRows) {
        return connection.executeStream(sql, params, transformRows)
      },
      loadExtension(path) {
        return withStatement(operation, withPermit(semaphore, connection.loadExtension(path)))
      },
    })
    const acquirer = Effect.sync(() => lockedConnection({ sawContention: false }))
    // A failed BEGIN owns the connection only for that native attempt. After BEGIN succeeds, its lease stays with the
    // transaction scope so other statements cannot join it.
    const transactionAcquirer = Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const operation = { sawContention: false }
      const connectionLease = yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const lease = yield* restore(
            statementGate.withExclusive(acquirePermitLease(semaphore, 1, operation), operation, true),
          )
          const registered = yield* Effect.exit(Scope.addFinalizer(scope, Scope.close(lease, Exit.void)))
          if (Exit.isFailure(registered)) {
            yield* Scope.close(lease, registered)
            return yield* Effect.failCause(registered.cause)
          }
          return lease
        }),
      )
      return transactionConnection(scope, connectionLease, operation)
    })

    return Object.assign(
      (yield* Client.make({
        acquirer,
        compiler,
        transactionAcquirer,
        spanAttributes: [
          ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
          [ATTR_DB_SYSTEM_NAME, "sqlite"],
        ],
        transformRows,
      })) as SqliteClient,
      {
        [TypeId]: TypeId as TypeId,
        config: options,
        loadExtension: (path: string) => Effect.scoped(Effect.flatMap(acquirer, (_) => _.loadExtension(path))),
      },
    )
  })

export const layer = (config: SqliteClientConfig): Layer.Layer<SqliteClient | Client.SqlClient> =>
  Layer.effectContext(
    Effect.map(make(config), (client) =>
      Context.make(SqliteClient, client).pipe(Context.add(Client.SqlClient, client)),
    ),
  ).pipe(Layer.provide(Reactivity.layer))
