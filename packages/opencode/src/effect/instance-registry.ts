const disposers = new Set<(directory: string) => Promise<void>>()
const instancePromises = new Map<string, Set<Promise<unknown>>>()

export function registerDisposer(disposer: (directory: string) => Promise<void>) {
  disposers.add(disposer)
  return () => {
    disposers.delete(disposer)
  }
}

export async function disposeInstance(directory: string) {
  await Promise.allSettled([...disposers].map((disposer) => disposer(directory)))
}

export function trackInstancePromise<A>(directory: string, promise: Promise<A>) {
  const pending = instancePromises.get(directory) ?? new Set<Promise<unknown>>()
  instancePromises.set(directory, pending)
  pending.add(promise)
  const settled = () => {
    pending.delete(promise)
    if (pending.size === 0) instancePromises.delete(directory)
  }
  void promise.then(settled, settled)
  return promise
}

export function hasInstancePromises(directory: string) {
  return (instancePromises.get(directory)?.size ?? 0) > 0
}

export async function awaitInstancePromises(directory: string) {
  while (true) {
    const pending = instancePromises.get(directory)
    if (!pending?.size) return true
    await Promise.allSettled([...pending])
  }
}
