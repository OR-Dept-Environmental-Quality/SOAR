import os
import shutil
import pandas as pd
import subprocess
import sys

POLLUTANTS = ["88101", "81102"]
STATES = ["37", "41"]
YEAR = 2024
START = "2024-03-01"
END = "2024-03-03"
TEST_STAGE_ROOT = "data/stage"
SLEEP_SEC = 2

def test_real_pipeline():
    # Clean up previous outputs
    if os.path.exists(TEST_STAGE_ROOT):
        shutil.rmtree(TEST_STAGE_ROOT)
    # Run orchestrator
    cmd = [
        sys.executable, "-m", "scripts.orchestrate_pipeline",
        "--pollutants", ",".join(POLLUTANTS),
        "--year", str(YEAR),
        "--start", START,
        "--end", END,
        "--states", ",".join(STATES)
    ]
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_root)
    print(result.stdout)
    assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
    # Check stage outputs
    for pollutant in POLLUTANTS:
        if pollutant == "88101":
            stage_file = os.path.join(TEST_STAGE_ROOT, "fctPM25Hourly", f"{YEAR}.csv")
        elif pollutant == "81102":
            stage_file = os.path.join(TEST_STAGE_ROOT, "fctPM10Hourly", f"{YEAR}.csv")
        else:
            continue
        assert os.path.exists(stage_file), f"Missing stage file: {stage_file}"
        df = pd.read_csv(stage_file)
        print(f"Pipeline Stage {pollutant} {YEAR}: {len(df)} rows")
        assert len(df) > 0
        # Daily AQI
        aqi_file = os.path.join(TEST_STAGE_ROOT, "fctAQIDaily", f"{pollutant}_{YEAR}.csv")
        assert os.path.exists(aqi_file), f"Missing AQI file: {aqi_file}"
        df_aqi = pd.read_csv(aqi_file)
        print(f"Pipeline AQI {pollutant} {YEAR}: {len(df_aqi)} rows")
        assert len(df_aqi) > 0
    # Check new stage outputs
    # fctAQICategory
    aqi_cat_file = os.path.join(TEST_STAGE_ROOT, "fctAQICategory", f"{YEAR}.csv.zip")
    assert os.path.exists(aqi_cat_file), f"Missing fctAQICategory: {aqi_cat_file}"
    df_cat = pd.read_csv(aqi_cat_file, compression="zip")
    print(f"Pipeline fctAQICategory {YEAR}: {len(df_cat)} rows")
    assert len(df_cat) > 0
    # fctOtherPollutantsHourly
    other_file = os.path.join(TEST_STAGE_ROOT, "fctOtherPollutantsHourly", f"{YEAR}.csv.zip")
    if os.path.exists(other_file):
        df_other = pd.read_csv(other_file, compression="zip")
        print(f"Pipeline fctOtherPollutantsHourly {YEAR}: {len(df_other)} rows")
        assert len(df_other) > 0
    else:
        print(f"No fctOtherPollutantsHourly file for {YEAR} (OK if only PM2.5/PM10 present)")
    # fctWildfireFlags
    wildfire_file = os.path.join(TEST_STAGE_ROOT, "fctWildfireFlags", f"{YEAR}.csv.zip")
    if os.path.exists(wildfire_file):
        df_wild = pd.read_csv(wildfire_file, compression="zip")
        print(f"Pipeline fctWildfireFlags {YEAR}: {len(df_wild)} rows")
        assert len(df_wild) > 0
    else:
        print(f"No fctWildfireFlags file for {YEAR} (OK if no wildfire flags present)")
    # dimDate
    dim_date_file = os.path.join(TEST_STAGE_ROOT, "dimDate.csv.zip")
    assert os.path.exists(dim_date_file), f"Missing dimDate: {dim_date_file}"
    df_dim = pd.read_csv(dim_date_file, compression="zip")
    print(f"Pipeline dimDate: {len(df_dim)} rows")
    assert len(df_dim) > 0
    # monitor_coverage
    moncov_file = os.path.join(TEST_STAGE_ROOT, "monitor_coverage.csv.zip")
    assert os.path.exists(moncov_file), f"Missing monitor_coverage: {moncov_file}"
    df_cov = pd.read_csv(moncov_file, compression="zip")
    print(f"Pipeline monitor_coverage: {len(df_cov)} rows")
    assert len(df_cov) > 0
    # Optionally clean up
    # shutil.rmtree(TEST_STAGE_ROOT) 