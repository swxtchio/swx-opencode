import { expect, test } from "bun:test"
import { copyFile, mkdtemp, rm } from "node:fs/promises"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import path from "node:path"

const require = createRequire(import.meta.url)

test("photon loads and executes WASM from OpenCode's embedded-runtime path", async () => {
  const modulePath = require.resolve("@silvia-odwyer/photon-node")
  const directory = await mkdtemp(path.join(tmpdir(), "opencode-photon-wasm-"))
  const isolatedModulePath = path.join(directory, "photon_rs.js")
  const wasmPath = path.join(path.dirname(modulePath), "photon_rs_bg.wasm")
  const wasmOverride = globalThis as typeof globalThis & { __OPENCODE_PHOTON_WASM_PATH?: string }
  const previousWasmPath = wasmOverride.__OPENCODE_PHOTON_WASM_PATH

  try {
    await copyFile(modulePath, isolatedModulePath)
    wasmOverride.__OPENCODE_PHOTON_WASM_PATH = wasmPath
    const photon = require(isolatedModulePath) as typeof import("@silvia-odwyer/photon-node")
    const image = new photon.PhotonImage(new Uint8Array([1, 2, 3, 255]), 1, 1)
    try {
      expect([...image.get_raw_pixels()]).toEqual([1, 2, 3, 255])
    } finally {
      image.free()
    }
  } finally {
    if (previousWasmPath === undefined) delete wasmOverride.__OPENCODE_PHOTON_WASM_PATH
    else wasmOverride.__OPENCODE_PHOTON_WASM_PATH = previousWasmPath
    await rm(directory, { recursive: true, force: true })
  }
})
