"""Prepare a 15-minute service-key rollover; never restarts services or prints credentials."""
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import time

root = Path(__file__).resolve().parent.parent / '.dev-secrets'
key_file = root / 'body-signing.key'
old = key_file.read_text().strip()
if len(old) != 64 or any(char not in '0123456789abcdef' for char in old):
    raise RuntimeError('invalid service signing key')
deadline = root / 'body-signing.key.previous-until'
if deadline.exists() and int(deadline.read_text()) > int(time.time()):
    raise RuntimeError('previous service-key rollover is still active')

def write(target, value):
    temporary = Path(str(target) + '.new')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(value)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, target)
    directory = os.open(root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)

new = secrets.token_hex(32)
write(root / 'body-signing.key.previous', old)
write(deadline, str(int(time.time()) + 900))
write(key_file, new)
for host, name in [('body-host-01', 'body-host-01.token'), ('body-host-02', 'body-host-02.token'), ('operator:reconcile', 'body-operator.token'), ('gateway:control', 'gateway-control.token')]:
    write(root / name, hmac.new(new.encode(), f'ansts-body-v1:{host}'.encode(), hashlib.sha256).hexdigest())
print('Prepared service-key rollover (values hidden). Deploy accepting control services first; then recreate Gateway and Daemon clients within 15 minutes.')
