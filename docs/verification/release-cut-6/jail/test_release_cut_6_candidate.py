"""Candidate acceptance through Abbe's real jail and _brain_rig."""

import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest
import test_opencode_jail_integration as integration

pytest_plugins = ("test_opencode_jail_integration",)


_PROVIDER_CODE = r'''
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os

PROPOSAL = json.loads(__PROPOSAL__)
MARKER = __MARKER__
CALL_ID = __CALL_ID__
IDENTITY = os.environ["PROVIDER_IDENTITY"]


class Handler(BaseHTTPRequestHandler):
    requests = 0

    def log_message(self, *_args):
        return

    def _readiness(self, nonce):
        if (
            self.path != "/v1/chat/completions"
            or self.headers.get("Authorization") != "Bearer fake-vendor-key"
        ):
            self.send_response(401)
            self.end_headers()
            return
        body = json.dumps({
            "readiness": {"provider": IDENTITY, "nonce": nonce},
            "choices": [],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        if "swxtch_readiness_probe" in request:
            self._readiness(request["swxtch_readiness_probe"])
            return
        Handler.requests += 1
        messages = request.get("messages", [])
        serialized = json.dumps(messages)
        title = "Generate a title for this conversation" in serialized or "You are a title generator" in serialized
        tool_messages = [message for message in messages if message.get("role") == "tool"]
        tool_result_ids = [message.get("tool_call_id") for message in tool_messages]
        kind = "title" if title else "continuation" if CALL_ID in tool_result_ids else "turn" if MARKER in serialized else "other"
        tools = request.get("tools", [])
        tool_names = [item.get("function", {}).get("name") for item in tools]
        print("CUT6_PROVIDER " + json.dumps({
            "index": Handler.requests,
            "kind": kind,
            "model": request.get("model"),
            "marker_present": MARKER in serialized,
            "tool_names": tool_names,
            "tool_result_ids": tool_result_ids,
            "selected_call_id": CALL_ID if kind == "turn" else None,
            "request": request,
        }), flush=True)
        if (
            self.path != "/v1/chat/completions"
            or self.headers.get("Authorization") != "Bearer fake-vendor-key"
        ):
            self.send_response(401)
            self.end_headers()
            return

        if kind == "turn":
            delta = {
                "role": "assistant",
                "tool_calls": [{
                    "index": 0,
                    "id": CALL_ID,
                    "type": "function",
                    "function": {
                        "name": "propose_netlist",
                        "arguments": json.dumps(PROPOSAL, separators=(",", ":")),
                    },
                }],
            }
            finish_reason = "tool_calls"
        else:
            text = "Release cut 6 proposal prepared for review." if kind == "title" else "Candidate tool execution continued."
            delta = {"role": "assistant", "content": text}
            finish_reason = "stop"

        first = {
            "id": "chatcmpl-cut6-%d" % Handler.requests,
            "object": "chat.completion.chunk",
            "created": 1,
            "model": request.get("model"),
            "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
            "usage": None,
        }
        final = {
            "id": "chatcmpl-cut6-%d" % Handler.requests,
            "object": "chat.completion.chunk",
            "created": 1,
            "model": request.get("model"),
            "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20},
        }
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for frame in (first, final):
            self.wfile.write(("data: " + json.dumps(frame) + chr(10) + chr(10)).encode())
            self.wfile.flush()
        self.wfile.write(("data: [DONE]" + chr(10) + chr(10)).encode())
        self.wfile.flush()


ThreadingHTTPServer(("0.0.0.0", 18081), Handler).serve_forever()
'''

_CLIENT_CODE = r'''
import json
import sys
import uuid
sys.path.insert(0, "/src/components/orch/services/opencode-variant")
from opencode_client import OpenCodeClient

endpoint, password, mode, marker, call_id = sys.argv[1:]
client = OpenCodeClient(endpoint=endpoint, password=password, directory="/workspace/project", timeout=120)
create_request = {"location": {"directory": "/workspace/project"}}
if mode == "location-only":
    session = client.create_session_v2()
else:
    create_request["model"] = {
        "providerID": "swxtch-egress",
        "id": "accounts/fireworks/models/deepseek-v4p1-flash",
    }
    response = client._request(
        "createSessionV2",
        "/api/session",
        method="POST",
        body=create_request,
        include_directory=False,
    )
    session = json.loads(response.read().decode())["data"]
    response.close()

session_id = session["id"]
message_id = "msg_" + uuid.uuid4().hex
prompt = {
    "text": "Prepare one valid draft CNL proposal by calling propose_netlist exactly once. "
    + "Keep the selected call attached to this user turn. Request marker: " + marker,
}
admission = client.prompt_session_v2(session_id, message_id, prompt)
wait = client.wait_session_v2(admission)
events = []
after_seq = None
while True:
    page = client.get_session_history_v2(session_id, after_seq=after_seq, limit=100)
    events.extend(page["data"])
    if not page["hasMore"]:
        break
    after_seq = page["data"][-1]["durable"]["seq"]

print(json.dumps({
    "mode": mode,
    "session_create_request": create_request,
    "session_create_response": session,
    "session_id": session_id,
    "message_id": message_id,
    "marker": marker,
    "call_id": call_id,
    "admission": admission.payload,
    "wait": wait.payload,
    "history": events,
}))
'''


@pytest.mark.parametrize(
    ("mode", "named_provider"),
    [pytest.param("location-only", False, id="location-only"), pytest.param("named-provider", True, id="named-provider")],
)
def test_candidate_executes_the_staged_tool_and_completes_its_v2_turn(
    internal_network: str,
    mode: str,
    named_provider: bool,
) -> None:
    marker = "RELEASE_CUT_6_" + mode.upper().replace("-", "_")
    call_id = "call_cut6_" + mode.replace("-", "_")
    proposal_json = json.dumps(integration._BRAIN_PROPOSAL, separators=(",", ":"))
    provider_code = (
        _PROVIDER_CODE.replace("__PROPOSAL__", repr(proposal_json))
        .replace("__MARKER__", repr(marker))
        .replace("__CALL_ID__", repr(call_id))
    )
    evidence_dir = Path(os.environ["RELEASE_CUT_6_EVIDENCE"])
    evidence_dir.mkdir(parents=True, exist_ok=True)
    repo_root = Path(__file__).resolve().parents[5]

    with integration._brain_rig(internal_network, provider_code=provider_code) as rig:
        result = subprocess.run(
            [
                "docker", "run", *integration.JAIL_PLATFORM_ARGS, "--rm", "--network", rig["network"],
                "--read-only", "--cap-drop", "ALL", "--tmpfs", "/tmp:rw,size=16m",
                "-e", "PYTHONDONTWRITEBYTECODE=1", "-v", f"{repo_root}:/src:ro",
                "--entrypoint", "python3", integration.JAIL_IMAGE, "-c", _CLIENT_CODE,
                rig["endpoint"], integration.SERVER_PASSWORD, mode, marker, call_id,
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        provider_log = subprocess.run(
            ["docker", "logs", rig["provider"]],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        ).stdout
        daemon_log = subprocess.run(
            ["docker", "logs", rig["name"]],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        ).stdout
        (evidence_dir / f"{mode}.client.stdout.txt").write_text(result.stdout)
        (evidence_dir / f"{mode}.client.stderr.txt").write_text(result.stderr)
        (evidence_dir / f"{mode}.provider.log").write_text(provider_log)
        (evidence_dir / f"{mode}.daemon.log").write_text(daemon_log)
        assert result.returncode == 0, result.stderr
        outcome = json.loads(result.stdout.strip().splitlines()[-1])
        provider_requests = [
            json.loads(line.removeprefix("CUT6_PROVIDER "))
            for line in provider_log.splitlines()
            if line.startswith("CUT6_PROVIDER ")
        ]
        evidence = {
            "mode": mode,
            "release_source_commit": os.environ["RELEASE_CUT_6_SOURCE_COMMIT"],
            "consumer_source_commit": os.environ["RELEASE_CUT_6_CONSUMER_COMMIT"],
            "candidate_archive_sha256": os.environ["RELEASE_CUT_6_ARCHIVE_SHA256"],
            "candidate_image_id": os.environ["RELEASE_CUT_6_IMAGE_ID"],
            "candidate_image": integration.JAIL_IMAGE,
            "session_create_request": outcome["session_create_request"],
            "session_create_response": outcome["session_create_response"],
            "session_id": outcome["session_id"],
            "user_message_id": outcome["message_id"],
            "request_marker": marker,
            "admission": outcome["admission"],
            "wait": outcome["wait"],
            "history": outcome["history"],
            "provider_requests": provider_requests,
            "provider_log": provider_log,
            "daemon_log": daemon_log,
        }
        (evidence_dir / f"{mode}.json").write_text(json.dumps(evidence, indent=2) + "\n")

        assert outcome["admission"]["sessionID"] == outcome["session_id"]
        assert outcome["admission"]["id"] == outcome["message_id"]
        assert outcome["wait"]["type"] == "completed"
        assert outcome["wait"]["admittedSeq"] == outcome["admission"]["admittedSeq"]
        assert marker in json.dumps(outcome["history"])
        assert outcome["message_id"] in json.dumps(outcome["history"])
        if named_provider:
            assert outcome["session_create_request"]["model"] == {
                "providerID": "swxtch-egress",
                "id": "accounts/fireworks/models/deepseek-v4p1-flash",
            }
        else:
            assert "model" not in outcome["session_create_request"]

        advertised = [
            tool_name
            for request in provider_requests
            for tool_name in request["tool_names"]
        ]
        assert "propose_netlist" in advertised
        selected = [request for request in provider_requests if request["kind"] == "turn"]
        assert len(selected) == 1
        assert selected[0]["marker_present"] is True
        assert selected[0]["selected_call_id"] == call_id
        assert selected[0]["request"]["model"] == "accounts/fireworks/models/deepseek-v4p1-flash"
        assert all(
            request["selected_call_id"] is None
            for request in provider_requests
            if request["kind"] == "title"
        )
        continuation = [request for request in provider_requests if request["kind"] == "continuation"]
        assert len(continuation) == 1
        assert call_id in continuation[0]["tool_result_ids"]

        called = [
            event["data"]
            for event in outcome["history"]
            if event["type"] == "session.next.tool.called" and event["data"].get("callID") == call_id
        ]
        assert len(called) == 1
        assert called[0].get("tool") == "propose_netlist"
        succeeded = [
            event["data"]
            for event in outcome["history"]
            if event["type"] == "session.next.tool.success" and event["data"].get("callID") == call_id
        ]
        assert len(succeeded) == 1
        success_output = json.dumps(succeeded[0], sort_keys=True)
        assert "Customer netlist proposal" in success_output
        assert "swxtch-netlist/0.1" in success_output
        assert "create_pipeline" in success_output
        assert not any(
            event["type"] == "session.next.tool.failed" and event["data"].get("callID") == call_id
            for event in outcome["history"]
        )
        assert "Candidate tool execution continued." in json.dumps(outcome["history"])
