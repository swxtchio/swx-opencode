#!/usr/bin/env python3
"""Record the loaded interpreter and argv before a traced exec can run user code."""

import ctypes
import errno
import json
import os
import signal
import sys


libc = ctypes.CDLL(None, use_errno=True)
libc.ptrace.argtypes = [ctypes.c_uint, ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p]
libc.ptrace.restype = ctypes.c_long
CONT = 7
GETEVENTMSG = 0x4201
SEIZE = 0x4206
LISTEN = 0x4208
TRACEFORK = 1 << 1
TRACEVFORK = 1 << 2
TRACECLONE = 1 << 3
TRACEEXEC = 1 << 4
EXITKILL = 1 << 20
EXEC = 4
EVENT_STOP = 128
STOP_SIGNALS = (signal.SIGSTOP, signal.SIGTSTP, signal.SIGTTIN, signal.SIGTTOU)
WALL = 0x40000000


def ptrace(request, pid, data=0):
    result = libc.ptrace(request, pid, None, ctypes.c_void_p(data))
    if result == -1:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    return result


def observe(output, command):
    with open(output, "w", encoding="utf-8") as report:
        reader, writer = os.pipe()
        root = os.fork()
        if root == 0:
            try:
                os.close(writer)
                if os.read(reader, 1) != b"r":
                    os._exit(125)
                os.close(reader)
                os.execvpe(command[0], command, os.environ)
            except OSError as error:
                print("tests/run.sh: observed dispatch failed: " + str(error), file=sys.stderr)
                os._exit(125)
        os.close(reader)
        children = {root}
        count = 0
        root_status = None
        try:
            # SEIZE enables transparent group-stop handling without an attach SIGSTOP.
            # The pipe gates dispatch until tracing options are acknowledged by the kernel.
            try:
                ptrace(SEIZE, root, TRACEFORK | TRACEVFORK | TRACECLONE | TRACEEXEC | EXITKILL)
                os.write(writer, b"r")
            finally:
                os.close(writer)
            while True:
                try:
                    pid, status = os.waitpid(-1, WALL)
                except ChildProcessError:
                    # Exec can retire sibling threads without a separate death report.
                    # ECHILD, not our bookkeeping set, confirms that tracing is drained.
                    children.clear()
                    break
                if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                    children.discard(pid)
                    if pid == root:
                        root_status = os.WEXITSTATUS(status) if os.WIFEXITED(status) else 128 + os.WTERMSIG(status)
                    continue
                if not os.WIFSTOPPED(status):
                    raise RuntimeError("unrecognized traced process state")
                event = status >> 16
                stop = os.WSTOPSIG(status)
                restart = CONT
                if event == EVENT_STOP:
                    children.add(pid)
                    # Only real group-stops carry a stopping signal here; auto-attached
                    # children and SIGCONT wakeups carry SIGTRAP (ptrace(2), PTRACE_SEIZE).
                    if stop in STOP_SIGNALS:
                        restart = LISTEN
                    stop = 0
                elif event:
                    message = ctypes.c_ulong()
                    try:
                        ptrace(GETEVENTMSG, pid, ctypes.addressof(message))
                    except OSError as error:
                        # A sibling's exec/exit can kill a thread while its event is queued.
                        # It has not been resumed; wait for the kernel's terminal status.
                        if error.errno == errno.ESRCH:
                            continue
                        raise
                    if event in (1, 2, 3):
                        children.add(message.value)
                    if event == EXEC:
                        # A non-leader thread adopts its thread group's PID on exec.
                        if message.value != pid:
                            children.discard(message.value)
                        children.add(pid)
                        executable = os.readlink(f"/proc/{pid}/exe")
                        identity = os.stat(f"/proc/{pid}/exe")
                        with open(f"/proc/{pid}/cmdline", "rb") as arguments:
                            raw = arguments.read()
                        if not raw.endswith(b"\0"):
                            raise RuntimeError("unavailable kernel-expanded argv")
                        record = {
                            "event": "exec",
                            "pid": pid,
                            "executable": executable.removesuffix(" (deleted)"),
                            "device": str(identity.st_dev),
                            "inode": str(identity.st_ino),
                            "argv": [os.fsdecode(value) for value in raw[:-1].split(b"\0")],
                        }
                        report.write(json.dumps(record) + "\n")
                        report.flush()
                        count += 1
                    stop = 0
                try:
                    ptrace(restart, pid, stop)
                except OSError as error:
                    # A thread can disappear when another thread execs or exits the group.
                    # Keep waiting for its terminal wait status, rather than assuming completion.
                    if error.errno != errno.ESRCH:
                        raise
            if root_status is None or count == 0:
                raise RuntimeError("no completed observed dispatch")
            report.write(json.dumps({"event": "complete", "exitCode": root_status, "execCount": count}) + "\n")
            return root_status
        finally:
            for pid in children:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


if __name__ == "__main__":
    try:
        if not sys.platform.startswith("linux") or len(sys.argv) < 3:
            raise RuntimeError("Linux procfs/ptrace and a command are required")
        sys.exit(observe(sys.argv[1], sys.argv[2:]))
    except (OSError, RuntimeError) as error:
        print("tests/run.sh: process-launch observation failed: " + str(error), file=sys.stderr)
        sys.exit(125)
