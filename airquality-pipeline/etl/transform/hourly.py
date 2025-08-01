"""Transform layer – Hourly cleaner (Sprint‑1 Day‑2 stub).

* Reads raw `data/raw/aqs/YYYY/MMDD_state.csv.zip` files.
* Normalises column names → snake_case.
* Adds data source flagging (AQS vs Envista).
* Stores Parquet partitioned by pollutant + year →
  `data/transform/hourly/<param>/<year>/MMDD_state.parquet`.
* Designed for **extensible pollutant list** – any `param_code` string works.
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from loguru import logger

RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "aqs"
TFM_ROOT = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal snake_case converter; extend later."""
    df = df.rename(columns={c: c.lower().replace(" ", "_") for c in df.columns})
    return df


def convert_file(raw_zip: Path) -> Path:
    """Convert one raw zip → parquet path."""

    if not raw_zip.suffix == ".zip":
        raise ValueError(raw_zip)

    df = pd.read_csv(raw_zip, compression="zip")
    df = _clean_columns(df)
    
    # Add data source flag (AQS for now, will be extended for Envista)
    df["data_source"] = "AQS"

    param = str(df.iloc[0]["parameter_code"])
    date_loc = df.iloc[0]["date_local"]  # YYYY‑MM‑DD
    year = date_loc.split("-")[0]

    out_dir = TFM_ROOT / param / year
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (raw_zip.stem + ".parquet")

    df.to_parquet(out_path, index=False)
    logger.success("[TX] → {} rows → {}", len(df), out_path)
    return out_path
