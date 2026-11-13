"""Create stable local-only Body credentials; existing keys are never rotated implicitly."""
import hashlib
import hmac
import os
from pathlib import Path
import secrets

root = Path(__file__).resolve().parent.parent / '.dev-secrets'
root.mkdir(mode=0o700, exist_ok=True)
key_file = root / 'body-signing.key'
if not key_file.exists():
    fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(secrets.token_hex(32))
key = key_file.read_text().strip()
if len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
    raise RuntimeError('invalid existing signing key; refusing to rotate')
for host in ('body-host-01', 'body-host-02'):
    token = hmac.new(key.encode(), f'ansts-body-v1:{host}'.encode(), hashlib.sha256).hexdigest()
    target = root / f'{host}.token'
    target.write_text(token)
    target.chmod(0o600)
print('Prepared host-scoped development credentials (values hidden).')
operator = root / 'body-operator.token'
operator.write_text(hmac.new(key.encode(), b'ansts-body-v1:operator:reconcile', hashlib.sha256).hexdigest())
operator.chmod(0o600)
gateway = root / 'gateway-control.token'
gateway.write_text(hmac.new(key.encode(), b'ansts-body-v1:gateway:control', hashlib.sha256).hexdigest())
gateway.chmod(0o600)
