import fs from "node:fs/promises"
import path from "node:path"

type EventValues = { releaseTag?: string; sourceCommit?: string; version?: string }
type RecordValue = Record<string, unknown>
export type ReleaseIdentity = { releaseTag: string; sourceCommit: string; version: string; bunVersion: string }

export function releaseInput(eventName: string, payload: unknown, defaultReleaseTag: string): EventValues & { releaseTag: string } {
  const event = record(payload)
  const values =
    eventName === "workflow_dispatch"
      ? record(event.inputs)
      : eventName === "repository_dispatch"
        ? record(event.client_payload)
        : eventName === "pull_request"
          ? {}
          : undefined
  if (values === undefined) throw new Error(`Unsupported release proof event: ${eventName}`)
  return {
    releaseTag: stringValue(values.release_tag) ?? defaultReleaseTag,
    sourceCommit: stringValue(values.source_commit),
    version: stringValue(values.version),
  }
}

export function resolveReleaseIdentity(input: {
  request: EventValues & { releaseTag: string }
  sourceCommit: string
  packageVersion: string
  tagCommit?: string
  allowUnpublishedTag?: boolean
  bunVersion: string
  expectedBunVersion: string
}): ReleaseIdentity {
  const sourceCommit = input.sourceCommit.toLowerCase()
  if (!/^[0-9a-f]{40}$/.test(sourceCommit)) throw new Error(`Invalid checked-out source commit: ${input.sourceCommit}`)
  const expectedVersion = `${input.packageVersion}-swxtch.${sourceCommit.slice(0, 8)}`
  const expectedTag = `v${expectedVersion}`
  if (!/^v\d+\.\d+\.\d+-swxtch\.[0-9a-f]{8}$/.test(input.request.releaseTag))
    throw new Error(`Invalid release tag: ${input.request.releaseTag}`)
  if (input.request.releaseTag !== expectedTag)
    throw new Error(`Release tag ${input.request.releaseTag} does not match checked-out source ${sourceCommit} (${expectedTag})`)
  if (input.request.sourceCommit && input.request.sourceCommit.toLowerCase() !== sourceCommit)
    throw new Error(`Requested source commit ${input.request.sourceCommit} does not match checked-out source ${sourceCommit}`)
  if (input.request.version && input.request.version !== expectedVersion)
    throw new Error(`Requested version ${input.request.version} does not match checked-out source version ${expectedVersion}`)
  if (input.tagCommit === undefined && !input.allowUnpublishedTag)
    throw new Error(`Tag ${input.request.releaseTag} has not been verified`)
  if (input.tagCommit !== undefined && input.tagCommit.toLowerCase() !== sourceCommit)
    throw new Error(`Tag ${input.request.releaseTag} resolves to ${input.tagCommit}, not ${sourceCommit}`)
  if (input.bunVersion !== input.expectedBunVersion)
    throw new Error(`Bun ${input.bunVersion} does not match selected source pin ${input.expectedBunVersion}`)
  return { releaseTag: expectedTag, sourceCommit, version: expectedVersion, bunVersion: input.bunVersion }
}

async function main() {
  const sourceDir = Bun.env.RELEASE_SOURCE_DIR
  const eventPath = Bun.env.GITHUB_EVENT_PATH
  const githubEnv = Bun.env.GITHUB_ENV
  const defaultReleaseTag = Bun.env.RELEASE_PROBE_DEFAULT_TAG
  const eventName = Bun.env.GITHUB_EVENT_NAME
  if (!sourceDir || !eventPath || !githubEnv || !defaultReleaseTag || !eventName)
    throw new Error("Release source directory, event context, default tag, and GITHUB_ENV are required")

  const event = await Bun.file(eventPath).json()
  const request = releaseInput(eventName, event, defaultReleaseTag)
  const candidate = Bun.argv.includes("--candidate")
  if (candidate && !/^[0-9a-f]{40}$/i.test(request.sourceCommit ?? ""))
    throw new Error("A full explicit source_commit is required for a release candidate")
  if (!/^v\d+\.\d+\.\d+-swxtch\.[0-9a-f]{8}$/.test(request.releaseTag))
    throw new Error(`Invalid release tag input: ${request.releaseTag}`)
  const selectedPackage = (await Bun.file(path.join(sourceDir, "packages/opencode/package.json")).json()) as {
    version?: string
  }
  const workspacePackage = (await Bun.file(path.join(sourceDir, "package.json")).json()) as {
    packageManager?: string
  }
  if (!selectedPackage.version) throw new Error("Selected source has no OpenCode package version")
  const expectedBunVersion = workspacePackage.packageManager?.match(/^bun@([^+]+)/)?.[1]
  if (!expectedBunVersion) throw new Error("Selected source has no pinned Bun packageManager version")

  const tagLookup = Bun.spawnSync(
    ["git", "-C", sourceDir, "ls-remote", "--exit-code", "origin", `refs/tags/${request.releaseTag}`],
    { stdout: "pipe", stderr: "pipe" },
  )
  const unpublished = candidate && tagLookup.exitCode === 2
  if (!unpublished) {
    if (tagLookup.exitCode !== 0)
      throw new Error(`Could not resolve release tag ${request.releaseTag}: ${tagLookup.stderr.toString()}`)
    const tagFetch = Bun.spawnSync(
      ["git", "-C", sourceDir, "fetch", "origin", `refs/tags/${request.releaseTag}:refs/tags/${request.releaseTag}`],
      { stdout: "pipe", stderr: "pipe" },
    )
    if (tagFetch.exitCode !== 0) throw new Error(`Could not fetch release tag ${request.releaseTag}: ${tagFetch.stderr.toString()}`)
  }
  const sourceCommit = git(sourceDir, ["rev-parse", "HEAD"])
  const tagCommit = unpublished ? undefined : git(sourceDir, ["rev-parse", "--verify", `${request.releaseTag}^{commit}`])
  const identity = resolveReleaseIdentity({
    request,
    sourceCommit,
    packageVersion: selectedPackage.version,
    tagCommit,
    allowUnpublishedTag: candidate,
    bunVersion: Bun.version,
    expectedBunVersion,
  })
  const waitFix = Bun.spawnSync(
    ["git", "-C", sourceDir, "merge-base", "--is-ancestor", "4b87478ddf", identity.sourceCommit],
    { stdout: "pipe", stderr: "pipe" },
  )
  if (waitFix.exitCode !== 0) throw new Error(`Release source ${identity.sourceCommit} does not contain the V2 wait fix`)

  await fs.appendFile(
    githubEnv,
    `RELEASE_TAG=${identity.releaseTag}\nSOURCE_COMMIT=${identity.sourceCommit}\nOPENCODE_VERSION=${identity.version}\nBUN_VERSION=${identity.bunVersion}\n`,
  )
  console.log(JSON.stringify(identity, null, 2))
}

function git(directory: string, args: string[]) {
  const result = Bun.spawnSync(["git", "-C", directory, ...args], { stdout: "pipe", stderr: "pipe" })
  if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${result.stderr.toString()}`)
  return result.stdout.toString().trim()
}

function record(input: unknown): RecordValue {
  return typeof input === "object" && input !== null && !Array.isArray(input) ? (input as RecordValue) : {}
}

function stringValue(input: unknown) {
  return typeof input === "string" && input.trim() ? input.trim() : undefined
}

if (import.meta.main) await main()
