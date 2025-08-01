"""State‑scoped AQS hourly sample downloader (**raw layer v3**).

Client‑confirmed requirements (05‑Jun‑2025)
-------------------------------------------
* Raw files **CSV inside .zip**.
* Path: ``data/raw/aqs/YYYY/MMDD_<state>.csv.zip``
* Supports **date ranges** and **multiple states** in one call.
* Polite delay (default 0.8 s) to stay far under AQS 60‑req/min guideline.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import os
import time
import requests
import pandas as pd
from loguru import logger

ENDPOINT = "https://aqs.epa.gov/data/api/sampleData/byState"
RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "aqs"
RAW_ROOT.mkdir(parents=True, exist_ok=True)


@dataclass(slots=True)
class AQSCreds:
    email: str
    key: str

    @classmethod
    def from_env(cls) -> "AQSCreds":
        email = os.getenv("AQS_API_EMAIL", "").strip()
        key = os.getenv("AQS_API_KEY", "").strip()
        if not email or not key:
            raise EnvironmentError("Set AQS_API_EMAIL and AQS_API_KEY env‑vars")
        return cls(email, key)


# ---------------------------------------------------------------------------
# Low‑level single‑day fetch
# ---------------------------------------------------------------------------

def fetch_day(param_code: int | str, obs_date: date, state_fips: str) -> Path:
    """Download hourly samples for *one state & one day* → zipped CSV path."""

    creds = AQSCreds.from_env()
    yyyymmdd = obs_date.strftime("%Y%m%d")
    params = {
        "email": creds.email,
        "key": creds.key,
        "param": str(param_code),
        "bdate": yyyymmdd,
        "edate": yyyymmdd,
        "state": state_fips.zfill(2),
    }

    outfile = _out_path(param_code, obs_date, state_fips)
    if outfile.exists():
        logger.info("[AQS] Skip existing {}", outfile)
        return outfile

    logger.info("[AQS] GET {} date={} state={}", ENDPOINT, obs_date, state_fips)
    r = requests.get(ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json().get("Data", [])
    if not rows:
        logger.warning("[AQS] No data for param {} date {} state {}", param_code, obs_date, state_fips)
        outfile.touch()
        return outfile

    pd.DataFrame(rows).to_csv(outfile, index=False, compression="zip")
    logger.success("[AQS] Saved {} rows → {}", len(rows), outfile)
    return outfile


# ---------------------------------------------------------------------------
# Convenience range‑fetch
# ---------------------------------------------------------------------------

def fetch_range(
    param_code: int | str,
    start: date,
    end: date,
    states: list[str],
    sleep_sec: float = 0.8,
) -> None:
    """Pull data for *every day* `start…end` (inclusive) for all *states* for a single pollutant.

    Creates files under `data/raw/aqs/<pollutant>/<year>/`. Skips existing files.
    """

    current = start
    while current <= end:
        for st in states:
            fetch_day(param_code, current, st)
            time.sleep(sleep_sec)
        current += timedelta(days=1)


def fetch_range_multi(
    pollutants: list[str],
    start: date,
    end: date,
    states: list[str],
    sleep_sec: float = 0.8,
) -> None:
    """Pull data for every pollutant in `pollutants` for every day `start…end` (inclusive) for all `states`.
    Usage:
        from etl.ingestion.aqs_state import fetch_range_multi
        fetch_range_multi(["88101", "81102"], start, end, ["37", "41"])
    """
    for pollutant in pollutants:
        logger.info("[AQS] Processing pollutant {} for {} to {}", pollutant, start, end)
        fetch_range(pollutant, start, end, states, sleep_sec)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _out_path(param_code: int | str, obs_date: date, state_fips: str) -> Path:
    """Return output path: data/raw/aqs/<pollutant>/<year>/MMDD_<state>.csv.zip"""
    yyyy, mmdd = obs_date.strftime("%Y"), obs_date.strftime("%m%d")
    pollutant_dir = RAW_ROOT / str(param_code) / yyyy
    pollutant_dir.mkdir(parents=True, exist_ok=True)
    return pollutant_dir / f"{mmdd}_{state_fips.zfill(2)}.csv.zip"
