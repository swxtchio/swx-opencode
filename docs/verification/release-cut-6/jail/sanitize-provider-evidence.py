"""Keep the cut-6 request evidence needed for review without exporting other Abbe tools."""

import hashlib
import json
import sys
from pathlib import Path


root = Path(sys.argv[1])
for mode in ("location-only", "named-provider"):
    evidence_path = root / f"{mode}.json"
    provider_log_path = root / f"{mode}.provider.log"
    evidence = json.loads(evidence_path.read_text())
    raw_log = provider_log_path.read_bytes()
    marker = evidence["request_marker"]
    call_id = next(
        request["selected_call_id"]
        for request in evidence["provider_requests"]
        if request["kind"] == "turn"
    )
    requests = []
    for item in evidence["provider_requests"]:
        request = item["request"]
        tools = [
            tool
            for tool in request.get("tools", [])
            if tool.get("function", {}).get("name") == "propose_netlist"
        ]
        messages = []
        for message in request.get("messages", []):
            if message.get("role") == "user":
                messages.append({"role": "user", "marker_present": marker in json.dumps(message)})
            if message.get("role") == "tool" and message.get("tool_call_id") == call_id:
                messages.append({"role": "tool", "tool_call_id": call_id, "content": message.get("content")})
            if message.get("role") == "assistant":
                calls = [call for call in message.get("tool_calls", []) if call.get("id") == call_id]
                if calls:
                    messages.append({"role": "assistant", "tool_calls": calls})
        requests.append({
            "index": item["index"],
            "kind": item["kind"],
            "model": item["model"],
            "marker_present": item["marker_present"],
            "selected_call_id": item["selected_call_id"],
            "tool_result_ids": item["tool_result_ids"],
            "request": {
                "model": request.get("model"),
                "stream": request.get("stream"),
                "messages": messages,
                "tools": tools,
            },
        })
    evidence["provider_requests"] = requests
    evidence["raw_provider_log"] = {
        "path": f"/home/byates/vm14-builds/opencode-release-832df553/evidence/jail/tool/{mode}.provider.log",
        "sha256": hashlib.sha256(raw_log).hexdigest(),
        "public_projection": "request model, turn marker, propose_netlist definition, selected call ID, and matching tool result only",
    }
    evidence.pop("provider_log", None)
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
    provider_log_path.write_text("\n".join(json.dumps(request, separators=(",", ":")) for request in requests) + "\n")
