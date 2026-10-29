#!/usr/bin/env python3
"""Stage the checked-in CWL tutorial without requiring tar in the runner image."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='calrissian-conformance')
    parser.add_argument('--namespace', default='calrissian-conformance')
    parser.add_argument('--destination', default='/output/docs-tutorial')
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1] / 'docs/tutorials/files'
    files = {p.name: p.read_text() for p in source.iterdir() if p.is_file()}
    code = '''import json, sys
from pathlib import Path
payload = json.load(sys.stdin)
root = Path(payload['destination'])
root.mkdir(parents=True, exist_ok=True)
for name, contents in payload['files'].items():
    (root / name).write_text(contents)
for name in ['tmp', 'tmpout']:
    (root / name).mkdir(exist_ok=True)
print('Staged tutorial at', root)
'''
    subprocess.run([
        'kubectl', '--context', args.context, '-n', args.namespace,
        'exec', '-i', 'deployment/calrissian-session', '-c', 'calrissian',
        '--', 'python', '-c', code,
    ], input=json.dumps({'destination': args.destination, 'files': files}),
        text=True, check=True, timeout=60)


if __name__ == '__main__':
    main()
