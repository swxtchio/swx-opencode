import { expect, test } from "bun:test"
import { launchEffort, parseModel, recentModels } from "../../src/context/local"

test("parses model IDs containing slashes", () => {
  expect(parseModel("provider/family/model")).toEqual({
    providerID: "provider",
    modelID: "family/model",
  })
})

test("moves a model to the front, deduplicates, and limits recents", () => {
  const recent = Array.from({ length: 12 }, (_, index) => ({
    providerID: "provider",
    modelID: `model-${index}`,
  }))

  expect(recentModels({ providerID: "provider", modelID: "model-5" }, recent)).toEqual([
    { providerID: "provider", modelID: "model-5" },
    ...recent.slice(0, 5),
    ...recent.slice(6, 10),
  ])
})

// GOAL: --effort at launch follows #29's rule for `run`: a declared effort is used, and an
// unknown one is refused with the choices listed, never silently dropped.
test("accepts a declared launch effort and default", () => {
  expect(launchEffort("high", "p/m", ["low", "high"])).toEqual({ effort: "high" })
  expect(launchEffort("default", "p/m", [])).toEqual({ effort: "default" })
})

test("refuses an unknown launch effort, naming the model and the choices", () => {
  expect(launchEffort("hgih", "p/m", ["low", "high"])).toEqual({
    error: 'Unknown effort "hgih" for p/m. Available: high, low',
  })
  expect(launchEffort("high", "p/m", [])).toEqual({ error: 'Unknown effort "high" for p/m. This model declares none.' })
})
