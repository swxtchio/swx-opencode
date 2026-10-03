type Step = {
  readonly started: Promise<void>
  readonly waiting: Promise<void>
  readonly start: () => void
  readonly release: () => void
  signalled: boolean
}

type Gate = {
  readonly loading: Step
  readonly initializing: Step
  readonly configuring: Step
}

let active: Gate | undefined

function step(): Step {
  let start = () => {}
  let release = () => {}
  return {
    started: new Promise<void>((resolve) => (start = resolve)),
    waiting: new Promise<void>((resolve) => (release = resolve)),
    start: () => start(),
    release: () => release(),
    signalled: false,
  }
}

async function wait(step: Step) {
  if (!step.signalled) {
    step.signalled = true
    step.start()
  }
  await step.waiting
}

export function install() {
  const gate = { loading: step(), initializing: step(), configuring: step() }
  active = gate
  return {
    loadStarted: gate.loading.started,
    initStarted: gate.initializing.started,
    configStarted: gate.configuring.started,
    releaseLoad: () => gate.loading.release(),
    releaseInit: () => gate.initializing.release(),
    releaseConfig: () => gate.configuring.release(),
    reset: () => {
      gate.loading.release()
      gate.initializing.release()
      gate.configuring.release()
      if (active === gate) active = undefined
    },
  }
}

export async function waitForLoad() {
  if (active) await wait(active.loading)
}

export async function waitForInit() {
  if (active) await wait(active.initializing)
}

export async function waitForConfig() {
  if (active) await wait(active.configuring)
}

export * as PluginGate from "./plugin-gate"
