import fs from "fs/promises"
import path from "path"
import { describe, expect, test } from "bun:test"
import { Effect, Option } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { Npm } from "@opencode-ai/core/npm"
import { tmpdir } from "./fixture/tmpdir"

const win = process.platform === "win32"

const writePackage = (dir: string, pkg: Record<string, unknown>) =>
  Bun.write(
    path.join(dir, "package.json"),
    JSON.stringify({
      version: "1.0.0",
      ...pkg,
    }),
  )

const npmLayer = (cache: string) =>
  AppNodeBuilder.build(Npm.node, [[Global.node, Global.layerWith({ cache, state: path.join(cache, "state") })]])

describe("Npm.sanitize", () => {
  test("keeps normal scoped package specs unchanged", () => {
    expect(Npm.sanitize("@opencode/acme")).toBe("@opencode/acme")
    expect(Npm.sanitize("@opencode/acme@1.0.0")).toBe("@opencode/acme@1.0.0")
    expect(Npm.sanitize("prettier")).toBe("prettier")
  })

  test("handles git https specs", () => {
    const spec = "acme@git+https://github.com/opencode/acme.git"
    const expected = win ? "acme@git+https_//github.com/opencode/acme.git" : spec
    expect(Npm.sanitize(spec)).toBe(expected)
  })
})

describe("Npm.add", () => {
  test("reifies when package cache directory exists without the package installed", async () => {
    await using tmp = await tmpdir()
    await fs.mkdir(path.join(tmp.path, "fixture-provider"))
    await writePackage(path.join(tmp.path, "fixture-provider"), {
      name: "fixture-provider",
      main: "index.js",
    })
    await Bun.write(path.join(tmp.path, "fixture-provider", "index.js"), "export const fixture = true\n")

    const spec = `fixture-provider@file:${path.join(tmp.path, "fixture-provider")}`
    await fs.mkdir(path.join(tmp.path, "cache", "packages", Npm.sanitize(spec)), { recursive: true })

    const entry = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return yield* npm.add(spec)
    }).pipe(Effect.scoped, Effect.provide(npmLayer(path.join(tmp.path, "cache"))), Effect.runPromise)

    expect(entry.entrypoint).toBeDefined()
  })
})

describe("Npm.install", () => {
  test("respects omit from project .npmrc", async () => {
    await using tmp = await tmpdir()

    await writePackage(tmp.path, {
      name: "fixture",
      dependencies: {
        "prod-pkg": "file:./prod-pkg",
      },
      devDependencies: {
        "dev-pkg": "file:./dev-pkg",
      },
    })
    await Bun.write(path.join(tmp.path, ".npmrc"), "omit=dev\n")
    await fs.mkdir(path.join(tmp.path, "prod-pkg"))
    await fs.mkdir(path.join(tmp.path, "dev-pkg"))
    await writePackage(path.join(tmp.path, "prod-pkg"), { name: "prod-pkg" })
    await writePackage(path.join(tmp.path, "dev-pkg"), { name: "dev-pkg" })

    await Npm.install(tmp.path)

    await expect(fs.stat(path.join(tmp.path, "node_modules", "prod-pkg"))).resolves.toBeDefined()
    await expect(fs.stat(path.join(tmp.path, "node_modules", "dev-pkg"))).rejects.toThrow()
  })
})

describe("reifyReason", () => {
  const base = {
    declared: ["@opencode-ai/plugin"],
    locked: new Set(["@opencode-ai/plugin"]),
    add: [] as { name: string; version?: string }[],
    installed: () => "1.18.25",
  }

  // GOAL: the #19 defect. The check compared dependency NAMES only, so a
  // directory that already had the package reported clean whatever version was
  // asked for. This is the real case: ~/swx-model-router-saas/.opencode is
  // locked at 1.18.25 while the binary now requests 1.18.31.
  test("reinstalls when the installed version is not the requested one", () => {
    const reason = Npm.reifyReason({ ...base, add: [{ name: "@opencode-ai/plugin", version: "1.18.31" }] })
    expect(reason).toContain("1.18.31")
    expect(reason).toContain("1.18.25")
  })

  // GOAL: and it must not reinstall when the pin is already satisfied, or every
  // start-up would reify.
  test("leaves an install that already matches", () => {
    expect(Npm.reifyReason({ ...base, add: [{ name: "@opencode-ai/plugin", version: "1.18.25" }] })).toBeUndefined()
  })

  // GOAL: the pre-existing behaviour has to survive - a declared dependency
  // missing from the lockfile still triggers a reinstall.
  test("reinstalls when a declared dependency is absent from the lockfile", () => {
    const reason = Npm.reifyReason({ ...base, locked: new Set<string>() })
    expect(reason).toContain("@opencode-ai/plugin")
  })

  // GOAL: a request that cannot be judged from a lockfile must be left alone
  // rather than guessed at. #17's fallback rung asks for `latest`, and turning
  // that into a reinstall on every run would be a regression of its own.
  test.each(["latest", "next", "https://example.com/plugin.tgz", "github:owner/repo"])(
    "does not reinstall for the unjudgeable request %p",
    (version) => {
      expect(Npm.reifyReason({ ...base, add: [{ name: "@opencode-ai/plugin", version }] })).toBeUndefined()
    },
  )

  // GOAL: a request with no version at all is the same case.
  test("does not reinstall when no version is requested", () => {
    expect(Npm.reifyReason({ ...base, add: [{ name: "@opencode-ai/plugin" }] })).toBeUndefined()
  })

  // GOAL: ranges are honoured as ranges, not compared as strings.
  test.each([
    ["^1.18.0", undefined],
    [">=1.18.0", undefined],
    ["^1.19.0", "reinstall"],
    ["1.18.x", undefined],
  ] as [string, string | undefined][])("treats the range %p correctly", (version, expected) => {
    const reason = Npm.reifyReason({ ...base, add: [{ name: "@opencode-ai/plugin", version }] })
    expect(reason === undefined ? undefined : "reinstall").toBe(expected)
  })

  // GOAL: a prerelease build of this fork must satisfy an ordinary range, the
  // same reasoning as #21 - otherwise pinning to a fork build would reify on
  // every single run.
  test("accepts a prerelease installed version against a plain range", () => {
    const reason = Npm.reifyReason({
      ...base,
      installed: () => "1.18.32-swxtch.1",
      add: [{ name: "@opencode-ai/plugin", version: "^1.18.0" }],
    })
    expect(reason).toBeUndefined()
  })

  // GOAL: fail toward installing. If a version was requested but the lockfile
  // records nothing installed, the safe move is to reinstall rather than assume
  // the request is satisfied.
  test("reinstalls when no installed version is recorded", () => {
    const reason = Npm.reifyReason({
      ...base,
      installed: () => undefined,
      add: [{ name: "@opencode-ai/plugin", version: "1.18.31" }],
    })
    expect(reason).toContain("no installed version")
  })
})
