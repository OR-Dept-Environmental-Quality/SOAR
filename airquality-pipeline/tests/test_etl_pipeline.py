import os
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from etl.ingestion import aqs_state
from etl.transform import batch_hourly, daily_aqi
from etl.stage import pbi_writer

POLLUTANTS = ["88101", "81102"]
YEAR = 2024
STATES = ["37", "41"]
START = date(2024, 3, 1)
END = date(2024, 3, 3)

# -------------------
# Ingestion Layer
# -------------------
def test_fetch_day_real(tmp_path, monkeypatch):
    email = os.getenv("AQS_API_EMAIL")
    key = os.getenv("AQS_API_KEY")
    assert email and key, "Set AQS_API_EMAIL and AQS_API_KEY in your environment"
    monkeypatch.setenv("AQS_API_EMAIL", email)
    monkeypatch.setenv("AQS_API_KEY", key)
    pollutant = POLLUTANTS[0]
    test_date = START
    state = STATES[0]
    out_path = aqs_state.fetch_day(pollutant, test_date, state)
    assert out_path.exists()
    df = pd.read_csv(out_path, compression="zip")
    print(f"test_fetch_day_real: {len(df)} rows for {pollutant} {test_date} {state}")
    assert len(df) > 0


def test_fetch_range_real(tmp_path, monkeypatch):
    email = os.getenv("AQS_API_EMAIL")
    key = os.getenv("AQS_API_KEY")
    assert email and key, "Set AQS_API_EMAIL and AQS_API_KEY in your environment"
    monkeypatch.setenv("AQS_API_EMAIL", email)
    monkeypatch.setenv("AQS_API_KEY", key)
    pollutant = POLLUTANTS[0]
    aqs_state.fetch_range(pollutant, START, END, STATES, sleep_sec=2)
    for state in STATES:
        current = START
        while current <= END:
            out_path = Path(f"data/raw/aqs/{pollutant}/{YEAR}/{current.strftime('%m%d')}_{state.zfill(2)}.csv.zip")
            assert out_path.exists()
            df = pd.read_csv(out_path, compression="zip")
            print(f"test_fetch_range_real: {len(df)} rows for {pollutant} {current} {state}")
            assert len(df) > 0
            current += timedelta(days=1)


def test_fetch_range_multi_real(tmp_path, monkeypatch):
    email = os.getenv("AQS_API_EMAIL")
    key = os.getenv("AQS_API_KEY")
    assert email and key, "Set AQS_API_EMAIL and AQS_API_KEY in your environment"
    monkeypatch.setenv("AQS_API_EMAIL", email)
    monkeypatch.setenv("AQS_API_KEY", key)
    aqs_state.fetch_range_multi(POLLUTANTS, START, END, STATES, sleep_sec=2)
    for pollutant in POLLUTANTS:
        for state in STATES:
            current = START
            while current <= END:
                out_path = Path(f"data/raw/aqs/{pollutant}/{YEAR}/{current.strftime('%m%d')}_{state.zfill(2)}.csv.zip")
                assert out_path.exists()
                df = pd.read_csv(out_path, compression="zip")
                print(f"test_fetch_range_multi_real: {len(df)} rows for {pollutant} {current} {state}")
                assert len(df) > 0
                current += timedelta(days=1)

# -------------------
# Transform Layer
# -------------------
def test_convert_year_real():
    batch_hourly.convert_year_multi(POLLUTANTS, YEAR)
    for pollutant in POLLUTANTS:
        hourly_dir = Path(f"data/transform/hourly/{pollutant}/{YEAR}")
        assert hourly_dir.exists()
        files = list(hourly_dir.glob("*.parquet"))
        assert files, f"No Parquet files for {pollutant}"
        total_rows = 0
        for f in files:
            df = pd.read_parquet(f)
            total_rows += len(df)
        print(f"test_convert_year_real: Hourly {pollutant} {YEAR}: {total_rows} rows")
        assert total_rows > 0

def test_calc_year_real():
    daily_aqi.batch_calc_year(POLLUTANTS, YEAR)
    for pollutant in POLLUTANTS:
        daily_path = Path(f"data/transform/daily_aqi/synchronous/{pollutant}/{YEAR}/daily_aqi.parquet")
        assert daily_path.exists()
        df = pd.read_parquet(daily_path)
        print(f"test_calc_year_real: Daily AQI {pollutant} {YEAR}: {len(df)} rows")
        assert len(df) > 0

# -------------------
# Stage Layer
# -------------------
def test_write_stage_real():
    for pollutant in POLLUTANTS:
        if pollutant == "88101":
            out_path = pbi_writer.write_pm25_hourly(YEAR)
        elif pollutant == "81102":
            out_path = pbi_writer.write_pm10_hourly(YEAR)
        else:
            continue
        assert Path(out_path).exists()
        df = pd.read_csv(out_path)
        print(f"test_write_stage_real: Stage {pollutant} {YEAR}: {len(df)} rows")
        assert len(df) > 0
        # Daily AQI
        out_aqi = pbi_writer.write_aqi_daily(pollutant, YEAR)
        assert Path(out_aqi).exists()
        df_aqi = pd.read_csv(out_aqi)
        print(f"test_write_stage_real: Stage AQI {pollutant} {YEAR}: {len(df_aqi)} rows")
        assert len(df_aqi) > 0