declare global {
  const OPENCODE_VERSION: string
  const OPENCODE_CHANNEL: string
  const OPENCODE_SDK_VERSION: string
}

export const InstallationVersion = typeof OPENCODE_VERSION === "string" ? OPENCODE_VERSION : "local"
export const InstallationChannel = typeof OPENCODE_CHANNEL === "string" ? OPENCODE_CHANNEL : "local"
export const InstallationLocal = InstallationChannel === "local"

// Version of the @opencode-ai/plugin SDK this build was compiled against,
// injected from the workspace package at build time.
//
// Deliberately NOT InstallationVersion: they are equal only for a published
// upstream release. For any fork or local build InstallationVersion may name a
// version npm has never seen, and pinning the SDK to it yields "No matching
// version found" - leaving every project with NO SDK, which every file under
// `.opencode/{tool,tools}` imports. Undefined when the define is absent (a
// from-source run), which callers read as "do not pin".
export const InstallationSdkVersion = typeof OPENCODE_SDK_VERSION === "string" ? OPENCODE_SDK_VERSION : undefined
