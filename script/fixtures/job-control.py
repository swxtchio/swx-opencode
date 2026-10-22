"""Check real child stop/continue behavior without a scheduling sleep."""

import os
import select
import signal
import sys


def timeout(signum, frame):
    raise RuntimeError("job-control fixture did not complete")


signal.signal(signal.SIGALRM, timeout)
signal.alarm(10)
stop = getattr(signal, sys.argv[1])
for iteration in range(30):
    reader, writer = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(reader)
        # A distinct group with a live parent in the session is not orphaned:
        # terminal stop signals must retain their normal stopping effect.
        os.setpgid(0, 0)
        os.kill(os.getpid(), stop)
        os.write(writer, b"r")
        os._exit(0)
    os.close(writer)
    reaped = False
    try:
        pid, status = os.waitpid(child, os.WUNTRACED)
        reaped = not os.WIFSTOPPED(status)
        if pid != child or not os.WIFSTOPPED(status) or os.WSTOPSIG(status) != stop:
            raise RuntimeError(f"child ran before SIGCONT: signal={sys.argv[1]} iteration={iteration} status={status}")
        if select.select([reader], [], [], 0)[0]:
            raise RuntimeError(f"child passed its stop before SIGCONT: signal={sys.argv[1]} iteration={iteration}")
        os.kill(child, signal.SIGCONT)
        if os.read(reader, 1) != b"r":
            raise RuntimeError("continued child did not acknowledge resumption")
        pid, status = os.waitpid(child, 0)
        reaped = True
        if pid != child or not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0:
            raise RuntimeError("continued child did not exit successfully")
    finally:
        if not reaped:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child, 0)
        os.close(reader)
signal.alarm(0)
print("job-control-ok:" + sys.argv[1])
