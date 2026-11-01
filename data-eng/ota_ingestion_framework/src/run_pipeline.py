# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""One local/Railway job: collect, publish the handover, then exit.

Run: python -m src.run_pipeline --workers 4 --max-seconds 300
"""

import argparse
import fcntl
import json
from datetime import datetime, timezone

from src.batch_pipeline import main as collect
from src.export_batch import build_export
from src.common.source_config import DATA
from src.common.logging import get_logger

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--max-seconds', type=float, default=300)
    parser.add_argument('--timeout', type=float, default=15)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    DATA.mkdir(parents=True, exist_ok=True)
    # A persistent-volume lock prevents two scheduled jobs modifying checkpoints.
    with (DATA/'pipeline.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('A pipeline job already owns this data directory.')
        arguments = ['--workers', str(args.workers), '--max-seconds', str(args.max_seconds), '--timeout', str(args.timeout)]
        if args.recheck:
            arguments.append('--recheck')
        outcome = collect(arguments)
        manifest = build_export()
        (DATA/'job_report.json').write_text(json.dumps(dict(
            finished_at_utc=datetime.now(timezone.utc).isoformat(),
            status='PARTIAL' if outcome else 'FINISHED', batch_exit_code=outcome,
            exported_csvs=manifest['csv_files'], raw_rows=manifest['raw_rows']), indent=2)+'\n')
        logger.info('Job finished. Open %s', DATA/'exports/latest')
        return outcome


if __name__ == '__main__':
    raise SystemExit(main())
