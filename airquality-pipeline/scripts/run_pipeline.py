"""Pipeline entry-point – Raw ingestion CLI (correct version).

Examples
--------
# Single day / state
python scripts/run_pipeline.py aqs --pollutant 88101 --date 2024-03-01 --state 37

# Date range & multi-state
python scripts/run_pipeline.py aqs --pollutant 88101 --start 2000-01-01 --end 2000-01-07 --states 37,41
"""

from __future__ import annotations
import argparse
from datetime import datetime, date
from pathlib import Path
import sys
from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.ingestion import aqs_state  # noqa: E402 pylint: disable=wrong-import-position

# Loguru sink
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger.remove()
logger.add(LOG_DIR / "pipeline.log", rotation="10 MB", retention=7, compression="zip", serialize=True)
logger.add(lambda m: sys.stderr.write(m), level="WARNING", serialize=True)


# CLI parser builder
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Air-Quality ETL – Raw layer")
    sub = p.add_subparsers(dest="cmd", required=True)

    aqs = sub.add_parser("aqs", help="Download AQS sampleData by state/date or range")
    aqs.add_argument("--pollutant", required=True, type=str, help="EPA parameter code(s), e.g. 88101 or 88101,81102")

    g = aqs.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="YYYY-MM-DD single day mode")
    g.add_argument("--start", help="YYYY-MM-DD range start (requires --end & --states)")
    aqs.add_argument("--end", help="YYYY-MM-DD range end")

    aqs.add_argument("--state", help="Single state FIPS (with --date mode)")
    aqs.add_argument("--states", help="Comma list of state FIPS codes (range mode)")

    return p


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def main() -> None:
    ns = _build_parser().parse_args()

    if ns.cmd == "aqs":
        # Support comma-separated list of pollutants
        if "," in ns.pollutant:
            pollutants = [p.strip() for p in ns.pollutant.split(",") if p.strip()]
        else:
            pollutants = [ns.pollutant.strip()]

        if ns.date:
            if not ns.state:
                sys.exit("--state required with --date mode")
            obs_date: date = datetime.strptime(ns.date, "%Y-%m-%d").date()
            for pollutant in pollutants:
                aqs_state.fetch_day(pollutant, obs_date, ns.state)
        else:
            if not (ns.start and ns.end and ns.states):
                sys.exit("--start, --end, --states required in range mode")
            start_d = datetime.strptime(ns.start, "%Y-%m-%d").date()
            end_d = datetime.strptime(ns.end, "%Y-%m-%d").date()
            state_list = [s.strip() for s in ns.states.split(",") if s.strip()]
            if len(pollutants) == 1:
                aqs_state.fetch_range(pollutants[0], start_d, end_d, state_list)
            else:
                aqs_state.fetch_range_multi(pollutants, start_d, end_d, state_list)
    else:
        raise NotImplementedError(ns.cmd)


if __name__ == "__main__":
    main()
