#!/usr/bin/env bun

import { Script } from "@opencode-ai/script"
import pluginPkg from "../../plugin/package.json"
import path from "path"
import { fileURLToPath } from "url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dir = path.resolve(__dirname, "..")

process.chdir(dir)

const generated = await import("./generate.ts")

await Bun.build({
  target: "node",
  // The watcher resolves its parcel worker as ./parcel-worker.js beside the bundle.
  entrypoints: ["./src/node.ts", "../core/src/filesystem/parcel-worker.ts"],
  outdir: "./dist/node",
  naming: "[name].[ext]",
  format: "esm",
  sourcemap: "linked",
  external: ["jsonc-parser", "@lydell/node-pty"],
  define: {
    OPENCODE_MODELS_DEV: generated.modelsData,
    OPENCODE_VERSION: `'${Script.version}'`,
    // Same define as the standalone build: without it this entrypoint has no
    // SDK version to pin and every project it serves takes the unpinned path,
    // free to drift to an SDK newer than this binary.
    OPENCODE_SDK_VERSION: `'${pluginPkg.version}'`,
    OPENCODE_CHANNEL: `'${Script.channel}'`,
  },
  files: {
    "opencode-web-ui.gen.ts": "",
  },
})

console.log("Build complete")
