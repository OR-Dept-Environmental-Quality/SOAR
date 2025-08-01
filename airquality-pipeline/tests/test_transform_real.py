import os
import pandas as pd
from etl.transform import batch_hourly, daily_aqi

TEST_RAW_ROOT = "data/raw/aqs"
TEST_TRANSFORM_HOURLY = "data/transform/hourly"
TEST_TRANSFORM_DAILY = "data/transform/daily_aqi/synchronous"
POLLUTANTS = ["88101", "81102"]
YEAR = 2024

# Assumes raw files already exist from ingestion test

def test_real_transform():
    # Hourly transform
    batch_hourly.convert_year_multi(POLLUTANTS, YEAR)
    for pollutant in POLLUTANTS:
        hourly_dir = os.path.join(TEST_TRANSFORM_HOURLY, pollutant, str(YEAR))
        assert os.path.exists(hourly_dir), f"Missing hourly dir: {hourly_dir}"
        files = [f for f in os.listdir(hourly_dir) if f.endswith(".parquet")]
        assert files, f"No Parquet files for {pollutant}"
        total_rows = 0
        for f in files:
            df = pd.read_parquet(os.path.join(hourly_dir, f))
            total_rows += len(df)
        print(f"Hourly {pollutant} {YEAR}: {total_rows} rows")
        assert total_rows > 0

    # Daily AQI transform
    daily_aqi.batch_calc_year(POLLUTANTS, YEAR)
    for pollutant in POLLUTANTS:
        daily_path = os.path.join(TEST_TRANSFORM_DAILY, pollutant, str(YEAR), "daily_aqi.parquet")
        assert os.path.exists(daily_path), f"Missing daily AQI: {daily_path}"
        df = pd.read_parquet(daily_path)
        print(f"Daily AQI {pollutant} {YEAR}: {len(df)} rows")
        assert len(df) > 0

    # Optionally clean up transform outputs
    # shutil.rmtree(TEST_TRANSFORM_HOURLY)
    # shutil.rmtree(TEST_TRANSFORM_DAILY) 