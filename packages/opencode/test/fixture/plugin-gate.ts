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
  readonly triggering: Step
  readonly disposing: Step
  holdDispose: boolean
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
  const gate = {
    loading: step(),
    initializing: step(),
    configuring: step(),
    triggering: step(),
    disposing: step(),
    holdDispose: false,
  }
  active = gate
  return {
    loadStarted: gate.loading.started,
    initStarted: gate.initializing.started,
    configStarted: gate.configuring.started,
    triggerStarted: gate.triggering.started,
    disposeStarted: gate.disposing.started,
    releaseLoad: () => gate.loading.release(),
    releaseInit: () => gate.initializing.release(),
    releaseConfig: () => gate.configuring.release(),
    releaseTrigger: () => gate.triggering.release(),
    releaseDispose: () => gate.disposing.release(),
    holdDispose: () => (gate.holdDispose = true),
    reset: () => {
      gate.loading.release()
      gate.initializing.release()
      gate.configuring.release()
      gate.triggering.release()
      gate.disposing.release()
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

export async function waitForTrigger() {
  if (active) await wait(active.triggering)
}

export async function waitForDispose() {
  if (active?.holdDispose) await wait(active.disposing)
}

export * as PluginGate from "./plugin-gate"
