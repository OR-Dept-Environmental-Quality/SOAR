"""Daily AQI calculator (Sprint‑1 Day‑3).

* Reads hourly Parquet files produced by `transform.hourly`.
* Version flag: 'current', 'retired', or 'synchronous'.
* Preserves data source flagging (AQS vs Envista).
* Saves Parquet to `data/transform/daily_aqi/<version>/<param>/<year>.parquet`.

NB: Breakpoints hard‑coded for PM2.5 & O3 as **client will supply full list**.
   DataFrame expected columns (snake_case from hourly cleaner):
   state_code, county_code, site_number, parameter_code,
   date_local, sample_measurement, data_source
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
import pandas as pd
from loguru import logger

TFM_HOURLY = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"
TFM_DAILY  = Path(__file__).resolve().parents[2] / "data" / "transform" / "daily_aqi"

# ---------------------------------------------------------------------------
# Minimal breakpoint tables – will move to YAML once client supplies full list
# Values = (Conc_low, Conc_high, AQI_low, AQI_high)
# ---------------------------------------------------------------------------
CURRENT_PM25 = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),  # tightened in 2024
    (225.5, 325.4, 301, 400),
    (325.5, 425.4, 401, 500),
]

RETIRED_PM25 = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

SWITCH_DATE = date(2024, 5, 6)  # AirNow switch‑over


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calc_year(param_code: str, year: int, version: str = "synchronous") -> Path:
    """Compute daily AQI for given pollutant & year."""

    hourly_dir = TFM_HOURLY / param_code / str(year)
    if not hourly_dir.exists():
        logger.error("[AQI] Hourly dir {} not found", hourly_dir)
        raise FileNotFoundError(hourly_dir)

    dfs = [pd.read_parquet(p) for p in hourly_dir.glob("*.parquet")]
    if not dfs:
        logger.warning("[AQI] No hourly Parquets in {}", hourly_dir)
        raise RuntimeError("No hourly data")

    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date_local"]).dt.date

    # Group by site and date, preserving data source information
    grp_cols = ["state_code", "county_code", "site_number", "date", "data_source"]
    daily = df.groupby(grp_cols, as_index=False)["sample_measurement"].mean()
    daily.rename(columns={"sample_measurement": "conc_avg"}, inplace=True)

    daily["aqi"] = daily["conc_avg"].apply(lambda x: _conc_to_aqi_pm25(x, version))

    out_dir = TFM_DAILY / version / param_code / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "daily_aqi.parquet"
    daily.to_parquet(out_path, index=False)
    logger.success("[AQI] Saved daily AQI → {} ({} rows)", out_path, len(daily))
    return out_path


def batch_calc_year(pollutants: list[str], year: int, version: str = "synchronous") -> dict:
    """Compute daily AQI for a list of pollutants for a given year.
    Returns a dict of pollutant -> output path.
    Usage:
        from etl.transform.daily_aqi import batch_calc_year
        batch_calc_year(["88101", "81102"], 2024)
    """
    results = {}
    for pollutant in pollutants:
        logger.info("[AQI] Processing pollutant {} for year {}", pollutant, year)
        try:
            out_path = calc_year(pollutant, year, version)
            results[pollutant] = out_path
        except Exception as e:
            logger.error("[AQI] Failed for pollutant {}: {}", pollutant, e)
            results[pollutant] = None
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _conc_to_aqi_pm25(conc: float, version: str) -> int:
    """Convert concentration to AQI using chosen breakpoint version."""

    breakpoints = CURRENT_PM25 if version == "current" else RETIRED_PM25
    if version == "synchronous":
        bp = CURRENT_PM25 if date.today() >= SWITCH_DATE else RETIRED_PM25
    else:
        bp = breakpoints

    for Clow, Chigh, Ilow, Ihigh in bp:
        if Clow <= conc <= Chigh:
            return round((Ihigh - Ilow) / (Chigh - Clow) * (conc - Clow) + Ilow)
    return -1  # outside range
