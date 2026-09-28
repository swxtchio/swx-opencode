# Mid-stream overflow verification

This is a dated run record for the recovery path, not a second behavior specification. On 2026-09-28, I ran the OpenCode CLI from `swxtchio/swx-opencode` commit `6b00206b2d4c8443393ff162234af3c2a0f02b6f` through the unpatched `swxtchio/swx-llmrouter` commit `cc4e5716f9f5d8afe6c5705f304212bebcd80afb` with a local mock backend. The router source is checkable at [`openclaw_router/server.py` in that commit](https://github.com/swxtchio/swx-llmrouter/blob/cc4e5716f9f5d8afe6c5705f304212bebcd80afb/openclaw_router/server.py); its `_call_streaming` path forwards `data:` lines after the upstream has returned HTTP 200.

## Reproduction

Run these commands from the `swx-opencode` repository root. They use ports `18900` and `18901`, and require Bun, Python 3, `uv`, and a local checkout of `swx-llmrouter` containing the recorded commit; set `SWX_LLMROUTER_CHECKOUT` to that checkout's root.

```sh
repro_dir="$PWD/.cache/overflow-e2e-note"
router_checkout="${SWX_LLMROUTER_CHECKOUT:?set SWX_LLMROUTER_CHECKOUT to a local swx-llmrouter checkout}"
router_rev=cc4e5716f9f5d8afe6c5705f304212bebcd80afb
mkdir -p "$repro_dir/router"
git -C "$router_checkout" archive "$router_rev" openclaw_router | tar -x -C "$repro_dir/router"
```

Create the router config:

```sh
cat > "$repro_dir/router-test.yaml" <<'YAML'
serve:
  host: 127.0.0.1
  port: 18900
  show_model_prefix: false
router:
  strategy: random
llms:
  mock:
    provider: openai
    model: mock-model
    base_url: http://127.0.0.1:18901/v1
    provider_type: openai_compatible
    auth_mode: none
    local: true
    max_tokens: 4096
    timeout: 60
YAML
```

Create `$repro_dir/mock.py` with this upstream fixture. Its first response is HTTP 200 and sends a valid partial assistant chunk followed by the unparseable string error chunk; the next two requests return the compaction summary and continuation.

```sh
cat > "$repro_dir/mock.py" <<'PY'
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    call = 0
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        type(self).call += 1
        number = type(self).call
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        def event(data):
            self.wfile.write(("data: " + json.dumps(data) + "\n\n").encode())
            self.wfile.flush()

        def done():
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()

        def chunk(content=None, finish=None):
            delta = {"role": "assistant"}
            if content is not None:
                delta["content"] = content
            return {
                "id": f"chatcmpl-local-{number}",
                "object": "chat.completion.chunk",
                "created": 1790550000,
                "model": body.get("model", "mock-model"),
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }

        if number == 1:
            event(chunk("Partial response before the mock overflow."))
            time.sleep(0.15)
            event({"error": "Your input exceeds the context window of this model. Please reduce the length and retry."})
            return

        content = "Compaction summary from the local mock." if number == 2 else "Continued after automatic compaction."
        event(chunk(content))
        event(chunk(finish="stop"))
        done()

    def log_message(self, format, *args):
        print(format % args, flush=True)

HTTPServer(("127.0.0.1", 18901), Handler).serve_forever()
PY
```

After creating both fixture files in the setup terminal, start the mock in terminal A:

```sh
repro_dir="$PWD/.cache/overflow-e2e-note"
python3 -u "$repro_dir/mock.py"
```

Start the archived router in terminal B:

```sh
repro_dir="$PWD/.cache/overflow-e2e-note"
cd "$repro_dir/router"
UV_CACHE_DIR="$repro_dir/uv" uv run --no-project --with fastapi --with uvicorn --with pyyaml --with httpx --with websockets --with numpy python -m openclaw_router --config ../router-test.yaml
```

Check the router's wire response. This consumes the mock's first response, so stop the mock with Ctrl-C and rerun terminal A's `python3 -u "$repro_dir/mock.py"` command before running OpenCode to reset its request counter.

```sh
curl -sS -N -w '\nstatus=%{http_code}\n' -H 'content-type: application/json' -d '{"model":"auto","messages":[{"role":"user","content":"trigger local context overflow"}],"stream":true}' http://127.0.0.1:18900/v1/chat/completions
```

The response was HTTP 200, with the partial chunk followed by `data: {"error":"Your input exceeds the context window of this model. Please reduce the length and retry."}` and no `[DONE]`.

After restarting the mock, run OpenCode from `packages/opencode` with an isolated config and data directory:

```sh
repro_dir="$(git rev-parse --show-toplevel)/.cache/overflow-e2e-note"
mkdir -p "$repro_dir/project"
OPENCODE_CONFIG_CONTENT='{"model":"llmrouter/auto","provider":{"llmrouter":{"name":"Local llmrouter","npm":"@ai-sdk/openai-compatible","options":{"baseURL":"http://127.0.0.1:18900/v1","apiKey":"mock-key","timeout":30000,"headerTimeout":30000,"chunkTimeout":30000},"models":{"auto":{"name":"auto","limit":{"context":100000,"output":4096}}}}},"compaction":{"auto":true}}' \
XDG_CONFIG_HOME="$repro_dir/config" \
XDG_DATA_HOME="$repro_dir/data" \
XDG_CACHE_HOME="$repro_dir/cache" \
timeout 90s bun run ./src/index.ts run --format json --model llmrouter/auto --title 'Overflow reproduction' --dir "$repro_dir/project" 'Please answer with one short sentence after reading the conversation.' > "$repro_dir/events.jsonl" 2> "$repro_dir/opencode-stderr.log"
opencode_status=$?
rg -n 'ContextOverflow|UnknownError|Type validation|Partial response|Compaction summary|Continued after|compaction_continue' "$repro_dir/events.jsonl"
printf 'opencode_exit_status=%s\n' "$opencode_status"
```

## Observed result

The OpenCode event stream recorded `ContextOverflowError` for the AI SDK's `TypeValidationError`, retained the partial response, sent a second backend request for the compaction summary, emitted the synthetic `compaction_continue` prompt, and sent a third request that completed with `Continued after automatic compaction.` The CLI process returned status 1 because `run.ts` treats any `session.error` event as a failed command even when the session recovers; the session itself completed the continuation. The committed focused regression is rerunnable from `packages/opencode` with `bun test test/session/prompt.test.ts --test-name-pattern 'automatically compacts and continues after an unparseable mid-stream error chunk'`.
