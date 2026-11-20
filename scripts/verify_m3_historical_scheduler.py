#!/usr/bin/env python3
"""Read-only revalidation of the known empty development Scheduler snapshot."""
import hashlib
from pathlib import Path
import subprocess
from urllib.parse import quote
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
OBJECT_ID = '145c35efce31ecfb1342d557a646f9e16e1b1aa02568718c5fae717a723ab2d9'
PREFIX = 'cells/AnstsSchedulerCell:' + OBJECT_ID + '/'
SNAPSHOT = PREFIX + 'ltx/e1/0000/0000000000000001-0000000000000001.ltx'
OWNER_DIGEST = '7b345ed99edc411e5d2ab780667fea97838be0be99e859ded40ed02a7521bfc3'
DIGEST = '762331095104fc04572a87a4ba78fcacadc63710331d532ec877270fefde5244'


def verify():
    credentials = dict(line.split('=', 1) for line in
                       (ROOT / '.dev-secrets/celld-deploy-runtime.env').read_text().splitlines()
                       if line.startswith(('AWS_ACCESS_KEY_ID=', 'AWS_SECRET_ACCESS_KEY=')))
    auth = credentials['AWS_ACCESS_KEY_ID'] + ':' + credentials['AWS_SECRET_ACCESS_KEY']
    if any(ch in auth for ch in ('"', '\\', '\r', '\n')):
        raise RuntimeError('Unexpected development credential encoding')
    base = 'http://127.0.0.1:9000/ansts-fleet'

    def fetch(path):
        return subprocess.check_output(
            ['curl', '--noproxy', '*', '-fsS', '--max-time', '10',
             '--aws-sigv4', 'aws:amz:us-east-1:s3', '--config', '-', base + path],
            input=('user = "' + auth + '"\n').encode())

    def keys():
        root = ET.fromstring(fetch('?list-type=2&prefix=' + quote(PREFIX, safe='')))
        if root.findtext('./{*}IsTruncated') != 'false':
            raise RuntimeError('Could not completely enumerate the historical Scheduler prefix')
        return {item.text for item in root.findall('./{*}Contents/{*}Key')}

    expected = {SNAPSHOT, PREFIX + 'own.json'}
    if keys() != expected:
        raise RuntimeError('Historical Scheduler object set changed')
    if hashlib.sha256(fetch('/' + quote(SNAPSHOT, safe='/:'))).hexdigest() != DIGEST:
        raise RuntimeError('Historical Scheduler snapshot digest changed')
    if hashlib.sha256(fetch('/' + quote(PREFIX + 'own.json', safe='/:'))).hexdigest() != OWNER_DIGEST:
        raise RuntimeError('Historical Scheduler owner changed')
    if keys() != expected:
        raise RuntimeError('Historical Scheduler object set changed during verification')
    return OBJECT_ID


if __name__ == '__main__':
    print(verify())
