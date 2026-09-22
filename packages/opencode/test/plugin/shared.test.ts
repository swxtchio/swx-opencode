import { describe, expect, test } from "bun:test"
import { checkPluginCompatibility, parsePluginSpecifier } from "../../src/plugin/shared"

describe("parsePluginSpecifier", () => {
  test("parses standard npm package without version", () => {
    expect(parsePluginSpecifier("acme")).toEqual({
      pkg: "acme",
      version: "latest",
    })
  })

  test("parses standard npm package with version", () => {
    expect(parsePluginSpecifier("acme@1.0.0")).toEqual({
      pkg: "acme",
      version: "1.0.0",
    })
  })

  test("parses scoped npm package without version", () => {
    expect(parsePluginSpecifier("@opencode/acme")).toEqual({
      pkg: "@opencode/acme",
      version: "latest",
    })
  })

  test("parses scoped npm package with version", () => {
    expect(parsePluginSpecifier("@opencode/acme@1.0.0")).toEqual({
      pkg: "@opencode/acme",
      version: "1.0.0",
    })
  })

  test("parses package with git+https url", () => {
    expect(parsePluginSpecifier("acme@git+https://github.com/opencode/acme.git")).toEqual({
      pkg: "acme",
      version: "git+https://github.com/opencode/acme.git",
    })
  })

  test("parses scoped package with git+https url", () => {
    expect(parsePluginSpecifier("@opencode/acme@git+https://github.com/opencode/acme.git")).toEqual({
      pkg: "@opencode/acme",
      version: "git+https://github.com/opencode/acme.git",
    })
  })

  test("parses package with git+ssh url containing another @", () => {
    expect(parsePluginSpecifier("acme@git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "acme",
      version: "git+ssh://git@github.com/opencode/acme.git",
    })
  })

  test("parses scoped package with git+ssh url containing another @", () => {
    expect(parsePluginSpecifier("@opencode/acme@git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "@opencode/acme",
      version: "git+ssh://git@github.com/opencode/acme.git",
    })
  })

  test("parses unaliased git+ssh url", () => {
    expect(parsePluginSpecifier("git+ssh://git@github.com/opencode/acme.git")).toEqual({
      pkg: "git+ssh://git@github.com/opencode/acme.git",
      version: "",
    })
  })

  test("parses npm alias using the alias name", () => {
    expect(parsePluginSpecifier("acme@npm:@opencode/acme@1.0.0")).toEqual({
      pkg: "acme",
      version: "npm:@opencode/acme@1.0.0",
    })
  })

  test("parses bare npm protocol specifier using the target package", () => {
    expect(parsePluginSpecifier("npm:@opencode/acme@1.0.0")).toEqual({
      pkg: "@opencode/acme",
      version: "1.0.0",
    })
  })

  test("parses unversioned npm protocol specifier", () => {
    expect(parsePluginSpecifier("npm:@opencode/acme")).toEqual({
      pkg: "@opencode/acme",
      version: "latest",
    })
  })
})

describe("checkPluginCompatibility", () => {
  const pkg = (range: unknown) => ({ dir: "/plugin", json: { engines: { opencode: range } } }) as never

  // GOAL: the #21 defect. A prerelease satisfies no ordinary range under plain
  // semver, so every plugin declaring engines.opencode was rejected on a fork
  // or prerelease build - with a message that blamed the plugin rather than
  // the version string. This fork's builds are versioned 1.18.32-swxtch.N, so
  // it was latent only until some plugin declared engines.
  test.each([">=1.18.0", "^1.18.0", ">=1.18.0 <2.0.0", "1.x"])(
    "accepts a prerelease build against %s",
    async (range) => {
      await checkPluginCompatibility("/plugin", "1.18.32-swxtch.1", pkg(range))
    },
  )

  // GOAL: releases keep working, so the fix is not just "accept everything".
  test("accepts a release build", async () => {
    await checkPluginCompatibility("/plugin", "1.18.31", pkg(">=1.18.0"))
  })

  // GOAL: the check still has teeth. A genuinely incompatible build must be
  // rejected, and the message must name both sides.
  test("still rejects a build below the required range", async () => {
    await expect(checkPluginCompatibility("/plugin", "1.17.0", pkg(">=1.18.0"))).rejects.toThrow(
      /Plugin requires opencode >=1\.18\.0 but running 1\.17\.0/,
    )
  })

  // GOAL: this is the case that decides includePrerelease over semver.coerce,
  // which #21 offered as an alternative. A prerelease of 1.19.0 is NOT 1.19.0,
  // so a plugin requiring the release must still be refused. Coercion would
  // accept it - measured: coerce -> true, includePrerelease -> false.
  test("rejects a prerelease of the very version a plugin requires", async () => {
    await expect(checkPluginCompatibility("/plugin", "1.19.0-beta.1", pkg(">=1.19.0"))).rejects.toThrow(
      /but running 1\.19\.0-beta\.1/,
    )
  })

  // GOAL: the early returns stay - these are "cannot judge", not "compatible".
  test.each([
    ["an invalid version", "not-a-version"],
    ["a major-zero version", "0.5.0"],
  ])("skips the check for %s", async (_name, version) => {
    await checkPluginCompatibility("/plugin", version, pkg(">=99.0.0"))
  })

  test.each([
    ["no engines field", {}],
    ["a non-record engines field", { engines: "nope" }],
    ["a non-string opencode range", { engines: { opencode: 42 } }],
  ])("skips the check when the manifest has %s", async (_name, json) => {
    await checkPluginCompatibility("/plugin", "1.18.32-swxtch.1", { dir: "/plugin", json } as never)
  })
})
