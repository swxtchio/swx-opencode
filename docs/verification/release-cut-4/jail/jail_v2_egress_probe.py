#!/usr/bin/env python3
"""Jail-boundary acceptance probe for swxtchio/swx-abbe#1952: does a V2 turn reach swxtch-egress?

Runs the swx-abbe jail integration rig: the real jail daemon started by the image's own entrypoint,
the real egress proxy, and the rig's fake provider. The client is opencode_client.OpenCodeClient
itself, run in a container on the rig's internal network, as the jail integration tests run it.

Usage, from the root of a swx-abbe checkout (or with SWX_ABBE_CHECKOUT set to one):
    python3 jail_v2_egress_probe.py v1-control v2 v2-named

Modes:
    v1-control  a V1 turn (OpenCodeClient.open_turn); shows the rig's egress path works
    v2          the V2 calls exactly as opencode_client.py sends them: create_session_v2(), then
                prompt_session_v2(session, "msg_...", {"text": ...})          <- the acceptance case
    v2-named    the same create with the session's model named (not sent by opencode_client.py today)

Each mode prints RESULT: PASS when the fake provider received a chat-completions request for the
egress model, and RESULT: FAIL otherwise. The exit status is 0 only when every requested mode passed.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("SWX_ABBE_CHECKOUT", ".")).resolve()
VARIANT = ROOT / "components" / "orch" / "services" / "opencode-variant"
sys.path[:0] = [str(VARIANT / "tests"), str(VARIANT)]

import test_opencode_jail_integration as jail  # noqa: E402  the rig

MODEL = "accounts/fireworks/models/deepseek-v4p1-flash"
SIGNAL = f"/v1/chat/completions model={MODEL}"

CLIENT = r'''
import json, sys, time, uuid
sys.path.insert(0, "/src/components/orch/services/opencode-variant")
import opencode_client as oc

endpoint, password, mode = sys.argv[1], sys.argv[2], sys.argv[3]
client = oc.OpenCodeClient(endpoint=endpoint, password=password, directory="/workspace/project", timeout=30)
out = {"mode": mode}
try:
    if mode == "v1-control":
        turn = client.open_turn(session_id="v1-control", message="Please describe an SRT input.")
        list(turn.events())
        response = turn.accumulator.build_response()
        out.update(status=response["status"], model=turn.accumulator.model_id)
    else:
        if mode == "v2":
            # POST /api/session  {"location":{"directory":"/workspace/project"}}
            session = client.create_session_v2()
        else:
            deadline = time.monotonic() + 30
            body = {
                "location": {"directory": client.directory},
                "model": {"providerID": "swxtch-egress", "id": "accounts/fireworks/models/deepseek-v4p1-flash"},
            }
            response = client._request("createSessionV2", "/api/session", method="POST", body=body,
                                       include_directory=False, deadline=deadline)
            session = oc._v2_data_object(oc._read_json(response, "createSessionV2", deadline=deadline),
                                         "createSessionV2")
        sid = session["id"]
        out["session"] = sid
        # POST /api/session/{sid}/prompt  {"id":"msg_...","prompt":{"text":"..."}}
        receipt = client.prompt_session_v2(sid, "msg_" + uuid.uuid4().hex[:16],
                                           {"text": "Please describe an SRT input."})
        out["admitted_seq"] = receipt.admitted_seq
        events = []
        for _ in range(45):
            page = client.get_session_history_v2(sid)
            events = page.get("data") if isinstance(page.get("data"), list) else page.get("events", [])
            types = [event.get("type", "") for event in events]
            if any(t.endswith(("step.failed", "step.ended", "step.completed", ".idle")) for t in types):
                break
            time.sleep(1)
        out["history_types"] = [event.get("type") for event in events]
        out["steps"] = [
            {"type": event.get("type"), "model": (event.get("data") or {}).get("model"),
             "error": (event.get("data") or {}).get("error")}
            for event in events if str(event.get("type", "")).startswith("session.next.step")
        ]
        try:
            outcome = client.wait_session_v2(receipt, deadline=time.monotonic() + 20)
            out["wait"] = repr(outcome)[:300]
        except oc.OpenCodeError as exc:
            out["wait_error"] = str(exc)[:300]
except oc.OpenCodeError as exc:
    out["client_error"] = f"{type(exc).__name__}: {exc}"[:400]
print(json.dumps(out), flush=True)
'''


def run(mode: str) -> bool:
    network = f"jail-v2-egress-{os.getpid()}-{mode}"
    subprocess.run(["docker", "network", "create", "--internal", network], check=True, capture_output=True)
    try:
        with jail._brain_rig(network) as rig:
            client = subprocess.run(
                [
                    "docker", "run", "--rm", "--network", rig["network"],
                    "--read-only", "--cap-drop", "ALL", "--tmpfs", "/tmp:rw,size=16m",
                    "-v", f"{ROOT}:/src:ro", "--entrypoint", "python3", jail.JAIL_IMAGE,
                    "-c", CLIENT, rig["endpoint"], jail.SERVER_PASSWORD, mode,
                ],
                capture_output=True, text=True, timeout=240,
            )
            provider = subprocess.run(["docker", "logs", rig["provider"]], capture_output=True, text=True, timeout=15)
            daemon = subprocess.run(["docker", "logs", rig["name"]], capture_output=True, text=True, timeout=15)
    finally:
        subprocess.run(["docker", "network", "rm", network], capture_output=True)
    requests = [line for line in (provider.stdout + provider.stderr).splitlines() if line.startswith("REQ")]
    errors = [
        line for line in (daemon.stdout + daemon.stderr).splitlines()
        if any(key in line for key in ("Failed to drain", "ModelNotSelected", "ModelUnavailable", "level=ERROR"))
    ]
    passed = any(SIGNAL in line for line in requests)
    print(f"=== {mode}")
    print("client:", (client.stdout.strip().splitlines() or ["<no output>"])[-1])
    if client.returncode != 0:
        print("client stderr:", client.stderr.strip()[-800:])
    print("provider requests:", json.dumps(requests))
    for line in errors[-5:]:
        print("daemon:", line[:400])
    print("RESULT:", "PASS" if passed else "FAIL")
    return passed


if __name__ == "__main__":
    modes = sys.argv[1:] or ["v1-control", "v2", "v2-named"]
    unknown = [mode for mode in modes if mode not in ("v1-control", "v2", "v2-named")]
    if unknown:
        sys.exit(f"unknown mode(s): {' '.join(unknown)}")
    results = [run(mode) for mode in modes]
    sys.exit(0 if all(results) else 1)
