"""Run both the original slice demonstration and the interval hand-off demo.

The original entry (``demo.py``) is left untouched and still only exercises the
slice business.  This single entry point prints both scenarios so the runtime
manifest keeps one argv for ``demo`` while preserving the original demo.
"""
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import demo
import demo_interval


def main():
    samples = Path(__file__).resolve().parent / 'samples'
    daily = json.loads((samples / 'daily.json').read_text(encoding='utf-8'))
    transfer = json.loads((samples / 'transfer.json').read_text(encoding='utf-8'))
    with TemporaryDirectory() as path:
        demo.run_demo(daily, transfer, Path(path))
    demo_interval.main()


if __name__ == '__main__':
    main()
