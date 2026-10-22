import { createServer } from "node:http"

const observations = []
for (let attempt = 0; attempt < 10; attempt++) {
  const original = createServer((_request, response) => response.end("original"))
  await new Promise((resolve, reject) => {
    original.once("error", reject)
    original.listen(0, "127.0.0.1", resolve)
  })
  const port = original.address().port
  const url = `http://127.0.0.1:${port}/pty/shells`
  await new Promise((resolve, reject) => original.close((error) => error ? reject(error) : resolve()))
  const replacement = createServer((_request, response) => response.end("replacement"))
  await new Promise((resolve, reject) => {
    replacement.once("error", reject)
    replacement.listen(port, "127.0.0.1", resolve)
  })
  try {
    const response = await fetch(url)
    observations.push({ attempt, port, original_listener_closed: original.listening === false, retired_url_fetch_resolved: true, status: response.status, response_body: await response.text() })
  } finally {
    const stopped = new Promise((resolve, reject) => replacement.close((error) => error ? reject(error) : resolve()))
    replacement.closeAllConnections()
    await stopped
  }
}
console.log(JSON.stringify({ runtime: process.versions.bun, architecture: process.arch, evidence_kind: "forced node:http dependency probe, not reproduction of Server.listen or proof of the actual CI responder", observations }, null, 2))
