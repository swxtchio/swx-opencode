// How `--effort` applies, shared by the TUI and `--mini`. It overrides the saved effort for
// whichever model is current, as long as that model declares it (or it is "default"), until
// the user picks an effort in the app. An unknown one is refused with the choices listed,
// never silently dropped: #29's rule for `run`.

export function launchEffort(requested: string, model: string, available: string[]) {
  if (requested === "default" || available.includes(requested)) return { effort: requested }
  const hint = available.length ? ` Available: ${[...available].sort().join(", ")}` : " This model declares none."
  return { error: `Unknown effort "${requested}" for ${model}.${hint}` }
}

// The effort in force for a model: the launch effort while it still applies and the model
// declares it, otherwise the saved one.
export function effortInForce(launch: string | undefined, available: string[], saved: string | undefined) {
  if (launch !== undefined && (launch === "default" || available.includes(launch))) return launch
  return saved
}
