import { expect, test } from "bun:test"
import { GlobalRegistrator } from "@happy-dom/global-registrator"
import { createContext, useContext } from "solid-js"
import { render } from "solid-js/web"

GlobalRegistrator.register()

const Context = createContext("fallback")

function ContextConsumer() {
  return <span>{useContext(Context)}</span>
}

test("Solid JSX renders a component through its context provider", () => {
  const container = document.createElement("div")
  const dispose = render(() => <Context.Provider value="provided context"><ContextConsumer /></Context.Provider>, container)
  try {
    expect(container.textContent).toBe("provided context")
  } finally {
    dispose()
    container.remove()
  }
})
