import { randomUUID } from "node:crypto"
import { lstat, mkdir, readdir, realpath, rename, rmdir, symlink, unlink, writeFile } from "node:fs/promises"
import path from "path"

export type UnrelatedPackage = {
  manifest: Record<string, unknown>
  module: string
  afterManifest?: (input: { entry: string; staging: string }) => Promise<void>
}

export async function installWorkspacePluginSdk(input: {
  sandboxRoot: string
  ownerRoot: string
  consumerDirectory: string
  workspace: string
  unrelated?: UnrelatedPackage
}) {
  const sandboxRoot = await realpath(input.sandboxRoot)
  const requestedOwner = path.resolve(input.ownerRoot)
  const requestedConsumer = path.resolve(input.consumerDirectory)
  const workspace = await realpath(input.workspace)
  requireWithin(requestedOwner, requestedConsumer)
  const ownerRoot = await realpath(requestedOwner)
  requireWithin(sandboxRoot, ownerRoot)
  const consumerDirectory = path.join(ownerRoot, path.relative(requestedOwner, requestedConsumer))
  requireWithin(ownerRoot, consumerDirectory)

  await ensureDirectoryPath(ownerRoot, consumerDirectory)
  const parent = path.join(consumerDirectory, "node_modules", "@opencode-ai")
  const entry = path.join(parent, "plugin")
  let staging: string | undefined

  try {
    await ensureDirectoryPath(ownerRoot, parent)
    if (input.unrelated) {
      // Keep fixture writes away from the installed entry until its parents are proven sandbox-owned.
      staging = path.join(ownerRoot, `.plugin-sdk-fixture-${randomUUID()}`)
      await mkdir(staging)
      const manifest = path.join(staging, "package.json")
      await writeFile(manifest, JSON.stringify(input.unrelated.manifest))
      await input.unrelated.afterManifest?.({ entry, staging })
      await writeFile(path.join(staging, "index.js"), input.unrelated.module)
      await ensureDirectoryPath(ownerRoot, parent)
      await removeEntry(ownerRoot, parent, entry)
      await rename(staging, entry)
      staging = undefined
    }

    await ensureDirectoryPath(ownerRoot, parent)
    if (!input.unrelated && (await isWorkspaceLink(entry, workspace))) return { entry, parent, target: workspace }

    await removeEntry(ownerRoot, parent, entry)
    await symlink(input.workspace, entry, "junction")
    const target = await realpath(entry)
    if (target !== workspace) throw new Error(`SDK fixture resolved a different plugin package: ${target}`)
    return { entry, parent, target }
  } finally {
    if (staging) await removeEntry(ownerRoot, ownerRoot, staging)
  }
}

function requireWithin(root: string, target: string) {
  const relative = path.relative(root, target)
  if (path.isAbsolute(relative) || relative === ".." || relative.startsWith(`..${path.sep}`))
    throw new Error(`SDK fixture path escapes its owner: ${target}`)
}

async function ensureDirectoryPath(root: string, target: string) {
  requireWithin(root, target)
  let current = root
  for (const part of path.relative(root, target).split(path.sep).filter(Boolean)) {
    current = path.join(current, part)
    const stat = await lstat(current).catch((error) => {
      if (isNotFound(error)) return undefined
      throw error
    })
    if (!stat) {
      await mkdir(current)
      continue
    }
    if (stat.isSymbolicLink()) {
      await unlink(current)
      await mkdir(current)
      continue
    }
    if (!stat.isDirectory()) throw new Error(`SDK fixture parent is not a directory: ${current}`)
  }
}

async function removeEntry(root: string, parent: string, target: string) {
  await ensureDirectoryPath(root, parent)
  requireWithin(root, target)
  const stat = await lstat(target).catch((error) => {
    if (isNotFound(error)) return undefined
    throw error
  })
  if (!stat) return
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    await unlink(target)
    return
  }
  await removeOwnedTree(root, target)
}

async function removeOwnedTree(root: string, target: string): Promise<void> {
  requireWithin(root, target)
  const stat = await lstat(target)
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    await unlink(target)
    return
  }
  requireWithin(root, await realpath(target))
  // Recurse with lstat so a link inside an owned package is removed as a link, not traversed.
  for (const name of await readdir(target)) await removeOwnedTree(root, path.join(target, name))
  await rmdir(target)
}

async function isWorkspaceLink(entry: string, workspace: string) {
  const stat = await lstat(entry).catch((error) => {
    if (isNotFound(error)) return undefined
    throw error
  })
  if (!stat?.isSymbolicLink()) return false
  return (await realpath(entry)) === workspace
}

function isNotFound(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
}
