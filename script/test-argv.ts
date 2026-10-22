import { writeFileSync } from "node:fs"

const commonValues = ["--env-file", "--require", "--import", "--conditions", "--loader", "-r"]
const bunValues = [
  "--cwd", "--config", "-c", "--preload", "--define", "-d", "-l", "--timeout", "--test-name-pattern", "-t",
  "--rerun-each", "--max-concurrency", "--seed", "--reporter", "--reporter-outfile", "--coverage-reporter", "--coverage-dir",
  "--cpu-prof-name", "--cpu-prof-dir", "--cpu-prof-interval", "--heap-prof-name", "--heap-prof-dir", "--heap-prof-interval",
  "--install", "--port", "--fetch-preconnect", "--max-http-header-size", "--dns-result-order", "--redirect-warnings",
  "--disable-warning", "--title", "--unhandled-rejections", "--console-depth", "--user-agent", "--main-fields",
  "--extension-order", "--tsconfig-override", "--drop", "--feature", "--jsx-factory", "--jsx-fragment", "--jsx-import-source",
  "--jsx-runtime", "--filter", "-F", "--shell", "--elide-lines", "--watch-kill-signal", "--retry",
  "--path-ignore-patterns", "--timings", "--parallel-delay", "--shard", "--cron-title", "--cron-period",
]
const nodeValues = [
  "--env-file-if-exists", "--experimental-loader", "--inspect-port", "--debug-port", "--title", "--redirect-warnings",
  "--diagnostic-dir", "--cpu-prof-dir", "--cpu-prof-name", "--cpu-prof-interval", "--heap-prof-dir", "--heap-prof-name",
  "--heap-prof-interval", "--report-directory", "--report-filename", "--test-name-pattern", "--test-skip-pattern",
  "--test-reporter", "--test-reporter-destination", "--test-concurrency", "--test-timeout", "--test-shard", "--input-type",
  "--experimental-default-type", "--watch-path", "--max-old-space-size", "--stack-trace-limit", "-C",
  "--allow-fs-read", "--allow-fs-write", "--build-snapshot-config", "--disable-proto", "--disable-warning",
  "--dns-result-order", "--experimental-config-file", "--experimental-sea-config", "--experimental-test-isolation",
  "--heapsnapshot-near-heap-limit", "--heapsnapshot-signal", "--icu-data-dir", "--inspect-publish-uid",
  "--localstorage-file", "--max-http-header-size", "--network-family-autoselection-attempt-timeout", "--openssl-config",
  "--report-dir", "--report-signal", "--run", "--secure-heap", "--secure-heap-min", "--snapshot-blob",
  "--test-coverage-branches", "--test-coverage-exclude", "--test-coverage-functions", "--test-coverage-include",
  "--test-coverage-lines", "--tls-cipher-list", "--tls-keylog", "--trace-event-categories", "--trace-event-file-pattern",
  "--trace-require-module", "--unhandled-rejections", "--use-largepages", "--v8-pool-size", "--watch-kill-signal",
  "--experimental-package-map", "--experimental-test-tag-filter", "--max-old-space-size-percentage", "--test-global-setup",
  "--test-isolation", "--test-random-seed", "--test-rerun-failures",
]
const bunBooleans = [
  "--no-orphans", "-u", "--update-snapshots", "--todo", "--only", "--pass-with-no-tests", "--concurrent", "--randomize",
  "--coverage", "--dots", "--only-failures", "--isolate", "--no-isolate", "--test-worker", "--update-timings",
  "--silent", "-v", "--version", "--revision", "-b", "--bun", "--workspaces", "--sequential", "--no-exit-on-error",
  "--watch", "--hot", "--no-clear-screen", "--smol", "--interactive", "--cpu-prof", "--cpu-prof-md", "--heap-prof",
  "--heap-prof-md", "--if-present", "--no-install", "-i", "--prefer-offline", "--prefer-latest",
  "--experimental-http2-fetch", "--experimental-http3-fetch", "--insecure-http-parser", "--experimental-stream-iter",
  "--expose-gc", "--no-deprecation", "--throw-deprecation", "--no-warnings", "--trace-warnings", "--trace-deprecation",
  "--pending-deprecation", "--zero-fill-buffers", "--use-system-ca", "--use-openssl-ca", "--use-bundled-ca",
  "--tls-min-v1.0", "--tls-min-v1.1", "--tls-min-v1.2", "--tls-min-v1.3", "--tls-max-v1.2", "--tls-max-v1.3",
  "--redis-preconnect", "--sql-preconnect", "--no-addons", "--no-ffi-cc", "--preserve-symlinks", "--preserve-symlinks-main",
  "--no-macros", "--jsx-side-effects", "--ignore-dce-annotations", "--no-env-file", "-h", "--help",
]
const nodeBooleans = [
  "--abort-on-uncaught-exception", "--allow-addons", "--allow-child-process", "--allow-wasi", "--allow-worker",
  "--build-snapshot", "-c", "--check", "--completion-bash", "--cpu-prof", "--disable-sigusr1",
  "--disable-wasm-trap-handler", "--disallow-code-generation-from-strings", "--enable-etw-stack-walking", "--enable-fips",
  "--enable-source-maps", "--entry-url", "--experimental-addon-modules", "--experimental-async-context-frame",
  "--experimental-default-config-file", "--experimental-eventsource", "--experimental-import-meta-resolve",
  "--experimental-inspector-network-resource", "--experimental-network-inspection", "--experimental-print-required-tla",
  "--experimental-test-coverage", "--experimental-test-module-mocks", "--experimental-transform-types",
  "--experimental-vm-modules", "--experimental-webstorage", "--experimental-worker-inspection", "--expose-gc",
  "--force-context-aware", "--force-fips", "--force-node-api-uncaught-exceptions-policy", "--frozen-intrinsics",
  "--heap-prof", "-h", "--help", "--huge-max-old-generation-size", "-i", "--interactive",
  "--interpreted-frames-native-stack", "--jitless", "--insecure-http-parser", "--no-addons", "--no-deprecation",
  "--no-experimental-detect-module", "--no-experimental-fetch", "--no-experimental-global-customevent",
  "--no-experimental-global-navigator", "--no-experimental-global-webcrypto", "--no-experimental-repl-await",
  "--no-experimental-require-module", "--no-experimental-sqlite", "--no-experimental-strip-types", "--no-experimental-websocket",
  "--no-extra-info-on-fatal-exception", "--no-force-async-hooks-checks", "--no-global-search-paths",
  "--enable-network-family-autoselection", "--no-network-family-autoselection", "--no-warnings", "--node-memory-debug",
  "--openssl-legacy-provider", "--openssl-shared-config", "--pending-deprecation", "--experimental-permission", "--permission",
  "--preserve-symlinks", "--preserve-symlinks-main", "--prof", "--prof-process", "--report-compact", "--report-exclude-env",
  "--report-exclude-network", "--report-on-fatalerror", "--report-on-signal", "--report-uncaught-exception", "--test",
  "--test-force-exit", "--test-only", "--test-update-snapshots", "--throw-deprecation", "--tls-max-v1.2", "--tls-max-v1.3",
  "--tls-min-v1.0", "--tls-min-v1.1", "--tls-min-v1.2", "--tls-min-v1.3", "--trace-atomics-wait", "--trace-deprecation",
  "--trace-env", "--trace-env-js-stack", "--trace-env-native-stack", "--trace-exit", "--trace-promises", "--trace-sigint",
  "--trace-sync-io", "--trace-tls", "--trace-uncaught", "--trace-warnings", "--track-heap-objects", "--use-bundled-ca",
  "--use-openssl-ca", "--use-system-ca", "--v8-options", "-v", "--version", "--watch", "--watch-preserve-output",
  "--zero-fill-buffers",
  "--allow-inspector", "--allow-openssl-store", "--enable-fips-indicator-events", "--experimental-import-text",
  "--experimental-storage-inspection", "--experimental-stream-iter", "--no-async-context-frame", "--no-require-module",
  "--experimental-strip-types", "--no-strip-types", "--permission-audit", "--test-randomize", "--use-env-proxy",
]
type Arity = "value" | "boolean" | "optional" | "number" | "eval" | "evalOptional"
const tables = {
  bun: new Map<string, Arity>([
    ...[...commonValues, ...bunValues].map((name): [string, Arity] => [name, "value"]),
    ...bunBooleans.map((name): [string, Arity] => [name, "boolean"]),
    ...["--inspect", "--inspect-wait", "--inspect-brk", "--changed"].map((name): [string, Arity] => [name, "optional"]),
    ...["--bail", "--parallel"].map((name): [string, Arity] => [name, "number"]),
    ...["-e", "--eval", "-p", "--print"].map((name): [string, Arity] => [name, "eval"]),
  ]),
  node: new Map<string, Arity>([
    ...[...commonValues, ...nodeValues].map((name): [string, Arity] => [name, "value"]),
    ...nodeBooleans.map((name): [string, Arity] => [name, "boolean"]),
    ...["--inspect", "--inspect-wait", "--inspect-brk", "--force-fips"].map((name): [string, Arity] => [name, "optional"]),
    ...["-e", "--eval"].map((name): [string, Arity] => [name, "eval"]),
    ...["-p", "--print"].map((name): [string, Arity] => [name, "evalOptional"]),
  ]),
}

export function parseLaunch(runtime: "bun" | "node", args: readonly string[]) {
  const options: number[] = []
  let commandIndex = args.length
  let commandSeen = false
  let test = false
  let worker = false
  let delegated = false
  let error: string | undefined
  for (let index = 0; index < args.length; ) {
    const argument = args[index]!
    if (argument === "--" || argument === "-") break
    if (!argument.startsWith("-")) {
      if (commandSeen) break
      commandIndex = index
      commandSeen = true
      if (runtime !== "bun" || !["test", "run", "x"].includes(argument)) break
      test = argument === "test"
      index++
      continue
    }
    options.push(index)
    const name = argument.split("=", 1)[0]!
    const arity = tables[runtime].get(name)
    if (!arity) {
      error = "unclassified " + runtime + " launch option: " + name
      break
    }
    const attached = argument.includes("=")
    if (arity === "boolean" && attached) {
      error = "unexpected value for " + runtime + " boolean option: " + name
      break
    }
    if ((arity === "value" || arity === "eval") && !attached && args[index + 1] === undefined) {
      error = "missing value for " + runtime + " launch option: " + name
      break
    }
    if (runtime === "bun" && name === "--test-worker") worker = true
    if (runtime === "node" && name === "--test") test = true
    if (runtime === "bun" && (name === "--bun" || name === "-b")) delegated = true
    if (arity === "eval" || arity === "evalOptional") break
    index += !attached && (arity === "value" || (arity === "number" && /^\d+$/.test(args[index + 1] ?? ""))) ? 2 : 1
  }
  return { commandIndex, options, test, worker, delegated, error }
}

if (import.meta.main) {
  const parsed = parseLaunch("bun", process.argv.slice(2))
  if (parsed.error) {
    console.error("tests/run.sh: " + parsed.error)
    process.exit(126)
  }
  // Configured preloads can write stdout; keep the shim protocol on its private descriptor.
  writeFileSync(3, [parsed.commandIndex, Number(parsed.worker), Number(parsed.delegated), parsed.options.join(" ")].join("\n"))
}
