export async function resolve(specifier, context, nextResolve) {
  if (!specifier.startsWith(".") || /\.[^/]+$/.test(specifier)) return nextResolve(specifier, context)

  for (const suffix of [".ts", ".js", ".mjs", "/index.ts", "/index.js"]) {
    try {
      return await nextResolve(`${specifier}${suffix}`, context)
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ERR_MODULE_NOT_FOUND") throw error
    }
  }

  return nextResolve(specifier, context)
}
