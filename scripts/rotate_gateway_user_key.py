"""Rotate only the development Gateway user key, retaining 15-minute read-only grace."""
import os
from pathlib import Path
import secrets
import time

root = Path(__file__).resolve().parent.parent / '.dev-secrets'
key = root / 'user-signing.key'
old = key.read_text().strip()
if len(old) != 64 or any(c not in '0123456789abcdef' for c in old):
    raise RuntimeError('invalid current Gateway user key')
previous_until = Path(str(key) + '.previous-until')
if previous_until.exists() and int(previous_until.read_text()) > int(time.time()):
    raise RuntimeError('rotation grace is still active; refusing to invalidate previous readers')

def atomic_write(target, value):
    temporary = Path(str(target) + '.new')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(value)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, target)

atomic_write(Path(str(key) + '.previous'), old)
atomic_write(previous_until, str(int(time.time()) + 900))
atomic_write(key, secrets.token_hex(32))
print('Rotated Gateway user signing key (values hidden). Recreate both Gateway containers to adopt it.')
