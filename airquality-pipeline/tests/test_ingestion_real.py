import os
import time
import shutil
import pandas as pd
from datetime import date, timedelta
from etl.ingestion import aqs_state

TEST_OUTPUT_ROOT = "tests/output/raw/aqs"
POLLUTANTS = ["88101", "81102"]
STATES = ["37", "41"]
START = date(2024, 3, 1)
END = date(2024, 3, 3)
SLEEP_SEC = 2  # Slow for API politeness

def test_real_ingestion(monkeypatch):
    # Set up env vars
    email = os.getenv("AQS_API_EMAIL")
    key = os.getenv("AQS_API_KEY")
    assert email and key, "Set AQS_API_EMAIL and AQS_API_KEY in your environment"
    monkeypatch.setenv("AQS_API_EMAIL", email)
    monkeypatch.setenv("AQS_API_KEY", key)

    # Clean output dir
    if os.path.exists(TEST_OUTPUT_ROOT):
        shutil.rmtree(TEST_OUTPUT_ROOT)
    os.makedirs(TEST_OUTPUT_ROOT, exist_ok=True)

    # Run ingestion for each pollutant, state, and day
    for pollutant in POLLUTANTS:
        for state in STATES:
            current = START
            while current <= END:
                out_path = aqs_state.fetch_day(pollutant, current, state)
                assert out_path.exists(), f"Missing file: {out_path}"
                assert out_path.stat().st_size > 100, f"File too small: {out_path}"
                df = pd.read_csv(out_path, compression="zip")
                print(f"{pollutant} {state} {current}: {len(df)} rows")
                assert len(df) > 0, f"No data for {pollutant} {state} {current}"
                time.sleep(SLEEP_SEC)
                current += timedelta(days=1)

    # Clean up after test
    shutil.rmtree(TEST_OUTPUT_ROOT) 