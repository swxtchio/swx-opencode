import path from "node:path"

export async function fffLibraryForTarget(
  target: { os: "linux" | "darwin" | "win32"; arch: "arm64" | "x64"; abi?: "musl" },
  resolvePackage: (specifier: string) => string,
) {
  const libc = target.os === "linux" ? `-${target.abi ?? "gnu"}` : ""
  const name = `@ff-labs/fff-bin-${target.os}-${target.arch}${libc}`
  const packageJsonPath = resolvePackage(`${name}/package.json`)
  const library = target.os === "win32" ? "fff_c" : "libfff_c"
  const extension = target.os === "win32" ? "dll" : target.os === "darwin" ? "dylib" : "so"
  const directory = path.dirname(packageJsonPath)
  const sourcePath = path.join(directory, `${library}.${extension}`)
  const digest = new Bun.CryptoHasher("sha256").update(await Bun.file(sourcePath).arrayBuffer()).digest("hex")
  return {
    filename: `${library}-${digest}.${extension}`,
    sourcePath,
  }
}

export function fffBuildDefines(nativeLibrary: { filename: string }) {
  return { FFF_BUN_EXTERNAL_LIBRARY: JSON.stringify(nativeLibrary.filename) }
}
