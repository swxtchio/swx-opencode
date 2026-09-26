import { createSimpleContext } from "./helper"

export interface Args {
  model?: string
  agent?: string
  // --effort: a launch override for the effort of the model current at launch.
  variant?: string
  prompt?: string
  continue?: boolean
  sessionID?: string
  fork?: boolean
  auto?: boolean
}

export const { use: useArgs, provider: ArgsProvider } = createSimpleContext({
  name: "Args",
  init: (props: Args) => props,
})
