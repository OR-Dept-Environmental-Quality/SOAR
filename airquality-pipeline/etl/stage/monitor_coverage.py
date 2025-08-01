from pathlib import Path
import pandas as pd
from loguru import logger

def write_monitor_coverage(years: list[int]) -> Path:
    """Write monitor_coverage: available site × pollutant × date combinations from transform outputs.
    Output: data/stage/monitor_coverage.csv.zip
    Columns: pollutant, site_number, state_code, county_code, date, available
    """
    hourly_dir = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"
    if not hourly_dir.exists():
        logger.warning("[STG] hourly_dir {} does not exist, skipping monitor_coverage", hourly_dir)
        return None
    records = []
    for pollutant_dir in hourly_dir.iterdir():
        pollutant = pollutant_dir.name
        for year in years:
            year_dir = pollutant_dir / str(year)
            hourly_file = year_dir / "hourly.parquet"
            if not hourly_file.exists():
                continue
            df = pd.read_parquet(hourly_file, columns=["site_number", "state_code", "county_code", "date"])
            df = df.drop_duplicates()
            df["pollutant"] = pollutant
            df["available"] = 1
            records.append(df)
    if not records:
        logger.warning("[STG] No hourly data found for monitor coverage.")
        return None
    out = pd.concat(records, ignore_index=True)
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "monitor_coverage.csv.zip"
    out.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] monitor_coverage → {} rows", len(out))
    return out_path 