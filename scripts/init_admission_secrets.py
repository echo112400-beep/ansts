#!/usr/bin/env python3
"""Create missing local admission identities without printing or replacing existing secrets."""
import os
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parent.parent
DIRECTORY = ROOT / '.dev-secrets'
DIRECTORY.mkdir(mode=0o700, exist_ok=True)
values = []
for name in ('admission-service.key', 'admission-signing.key'):
    path = DIRECTORY / name
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(fd, 'w') as output:
            output.write(secrets.token_hex(32) + '\n')
            output.flush()
            os.fsync(output.fileno())
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise RuntimeError('Admission keys require private regular files')
    value = path.read_text().strip()
    if len(value) != 64 or any(character not in '0123456789abcdef' for character in value):
        raise RuntimeError('Existing admission key has invalid format; refusing replacement')
    values.append(value)
for name in ('directory-service.key', 'body-signing.key'):
    path = DIRECTORY / name
    if path.is_file():
        values.append(path.read_text().strip())
if len(values) != len(set(values)):
    raise RuntimeError('Admission identities must be independent; refusing automatic key replacement')
print('Admission secret files ready; existing identities preserved and no values printed.')
