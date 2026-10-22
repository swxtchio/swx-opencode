import { writeFileSync } from "node:fs"

const commonValues = ["--env-file", "--require", "--import", "--conditions", "--loader", "-r"]
const bunValues = [
  "--cwd", "--config", "-c", "--preload", "--define", "-d", "-l", "--timeout", "--test-name-pattern", "-t",
  "--rerun-each", "--max-concurrency", "--seed", "--reporter", "--reporter-outfile", "--coverage-reporter", "--coverage-dir",
  "--cpu-prof-name", "--cpu-prof-dir", "--cpu-prof-interval", "--heap-prof-name", "--heap-prof-dir", "--heap-prof-interval",
  "--install", "--port", "--fetch-preconnect", "--max-http-header-size", "--dns-result-order", "--redirect-warnings",
  "--disable-warning", "--title", "--unhandled-rejections", "--console-depth", "--user-agent", "--main-fields",
  "--extension-order", "--tsconfig-override", "--drop", "--feature", "--jsx-factory", "--jsx-fragment", "--jsx-import-source",
  "--jsx-runtime", "--filter", "-F", "--shell", "--elide-lines", "--watch-kill-signal",
]
const nodeValues = [
  "--env-file-if-exists", "--experimental-loader", "--inspect-port", "--debug-port", "--title", "--redirect-warnings",
  "--diagnostic-dir", "--cpu-prof-dir", "--cpu-prof-name", "--cpu-prof-interval", "--heap-prof-dir", "--heap-prof-name",
  "--heap-prof-interval", "--report-directory", "--report-filename", "--test-name-pattern", "--test-skip-pattern",
  "--test-reporter", "--test-reporter-destination", "--test-concurrency", "--test-timeout", "--test-shard", "--input-type",
  "--experimental-default-type", "--watch-path", "--max-old-space-size", "--stack-trace-limit", "-C",
]

export function parseLaunch(runtime: "bun" | "node", args: readonly string[]) {
  const values = new Set([...commonValues, ...(runtime === "bun" ? bunValues : nodeValues)])
  const options: number[] = []
  let commandIndex = args.length
  let commandSeen = false
  let test = false
  let worker = false
  let delegated = false
  for (let index = 0; index < args.length; ) {
    const argument = args[index]
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
    const name = argument.split("=", 1)[0]
    if (runtime === "bun" && name === "--test-worker") worker = true
    if (runtime === "node" && name === "--test") test = true
    if (runtime === "bun" && (name === "--bun" || name === "-b")) delegated = true
    if (["-e", "--eval", "-p", "--print"].includes(name)) break
    index += !argument.includes("=") && values.has(name) ? 2 : 1
  }
  return { commandIndex, options, test, worker, delegated }
}

if (import.meta.main) {
  const parsed = parseLaunch("bun", process.argv.slice(2))
  // Configured preloads can write stdout; keep the shim protocol on its private descriptor.
  writeFileSync(3, [parsed.commandIndex, Number(parsed.worker), Number(parsed.delegated), parsed.options.join(" ")].join("\n"))
}
