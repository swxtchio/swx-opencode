import os, json, sys
root = sys.argv[1]
skip = {os.getpid(), os.getppid()}
found = []
for pid in os.listdir('/proc'):
    if not pid.isdigit() or int(pid) in skip:
        continue
    try:
        cmd = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\0', b' ').decode(errors='replace')
        cwd = os.readlink(f'/proc/{pid}/cwd')
    except OSError:
        continue
    if root in cmd or cwd.startswith(root):
        found.append({'pid': int(pid), 'cmd': cmd[:200], 'cwd': cwd})
print(json.dumps({'remaining_processes': found}))
