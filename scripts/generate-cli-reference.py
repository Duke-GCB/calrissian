"""Refresh the Calrissian-specific option table from the argument parser."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from calrissian.main import add_arguments


def main():
    parser = argparse.ArgumentParser(add_help=False)
    add_arguments(parser)
    rows = [
        '| Option | JSON key | Value / repetition | Parser default | Description |',
        '|---|---|---|---|---|',
    ]
    for action in parser._actions:
        if isinstance(action, argparse._StoreTrueAction):
            kind = 'flag / JSON boolean'
        elif isinstance(action, argparse._AppendAction):
            kind = 'string; repeatable / JSON list'
        else:
            kind = 'string'
            if action.nargs == '?':
                kind += '; parser permits an omitted value'
        default = 'unset' if action.default is None else str(action.default).lower()
        help_text = (action.help or '').replace('|', '\\|')
        rows.append(f'| `{action.option_strings[0]}` | `{action.dest}` | {kind} | {default} | {help_text} |')
    page = ROOT / 'docs/reference/configuration.md'
    before, remaining = page.read_text().split('<!-- generated-options:start -->', 1)
    _, after = remaining.split('<!-- generated-options:end -->', 1)
    page.write_text(before + '<!-- generated-options:start -->\n\n' + '\n'.join(rows)
                    + '\n\n<!-- generated-options:end -->' + after)


if __name__ == '__main__':
    main()
