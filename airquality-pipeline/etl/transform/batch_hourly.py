"""Transform layer – Batch hourly processor with Envista fallback.

* Reads raw AQS and Envista data from `data/raw/` directories.
* Implements fallback logic: if AQS sample is missing → use Envista value and flag source.
* Normalises column names → snake_case.
* Stores Parquet partitioned by pollutant + year →
  `data/transform/hourly/<param>/<year>/MMDD_state.parquet`.
* Designed for **extensible pollutant list** – any `param_code` string works.
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from loguru import logger
from typing import List, Dict

RAW_AQS_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "aqs"
RAW_ENVISTA_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "envista"
TFM_ROOT = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal snake_case converter; extend later."""
    df = df.rename(columns={c: c.lower().replace(" ", "_") for c in df.columns})
    return df


def _merge_aqs_envista_data(aqs_df: pd.DataFrame, envista_df: pd.DataFrame) -> pd.DataFrame:
    """Merge AQS and Envista data with fallback logic.
    
    For each hour/pollutant: if AQS sample is missing → use Envista value and flag source.
    """
    if aqs_df.empty and envista_df.empty:
        return pd.DataFrame()
    
    if aqs_df.empty:
        return envista_df
    
    if envista_df.empty:
        return aqs_df
    
    # Ensure both DataFrames have required columns
    required_cols = ["state_code", "county_code", "site_number", "date_local", "time_local", "sample_measurement"]
    
    for col in required_cols:
        if col not in aqs_df.columns:
            aqs_df[col] = ""
        if col not in envista_df.columns:
            envista_df[col] = ""
    
    # Create merge key for matching records
    aqs_df["merge_key"] = aqs_df["state_code"] + "_" + aqs_df["county_code"] + "_" + aqs_df["site_number"] + "_" + aqs_df["date_local"] + "_" + aqs_df["time_local"]
    envista_df["merge_key"] = envista_df["state_code"] + "_" + envista_df["county_code"] + "_" + envista_df["site_number"] + "_" + envista_df["date_local"] + "_" + envista_df["time_local"]
    
    # Merge on the key, keeping AQS data when available, falling back to Envista
    merged = pd.merge(aqs_df, envista_df, on="merge_key", how="outer", suffixes=("_aqs", "_envista"))
    
    # Apply fallback logic
    result_rows = []
    
    for _, row in merged.iterrows():
        if pd.notna(row.get("sample_measurement_aqs")):
            # AQS data available - use it
            result_row = {
                "state_code": row.get("state_code_aqs", row.get("state_code_envista", "")),
                "county_code": row.get("county_code_aqs", row.get("county_code_envista", "")),
                "site_number": row.get("site_number_aqs", row.get("site_number_envista", "")),
                "parameter_code": row.get("parameter_code_aqs", row.get("parameter_code_envista", "")),
                "date_local": row.get("date_local_aqs", row.get("date_local_envista", "")),
                "time_local": row.get("time_local_aqs", row.get("time_local_envista", "")),
                "sample_measurement": row["sample_measurement_aqs"],
                "data_source": "AQS"
            }
        elif pd.notna(row.get("sample_measurement_envista")):
            # Only Envista data available - use it as fallback
            result_row = {
                "state_code": row.get("state_code_envista", row.get("state_code_aqs", "")),
                "county_code": row.get("county_code_envista", row.get("county_code_aqs", "")),
                "site_number": row.get("site_number_envista", row.get("site_number_aqs", "")),
                "parameter_code": row.get("parameter_code_envista", row.get("parameter_code_aqs", "")),
                "date_local": row.get("date_local_envista", row.get("date_local_aqs", "")),
                "time_local": row.get("time_local_envista", row.get("time_local_aqs", "")),
                "sample_measurement": row["sample_measurement_envista"],
                "data_source": "Envista"
            }
        else:
            # No data available - skip
            continue
        
        result_rows.append(result_row)
    
    result_df = pd.DataFrame(result_rows)
    
    # Clean up merge key
    if "merge_key" in result_df.columns:
        result_df = result_df.drop("merge_key", axis=1)
    
    return result_df


def convert_year_multi(pollutants: List[str], year: int) -> Dict[str, Path]:
    """Convert multiple pollutants for a given year with Envista fallback.
    
    Args:
        pollutants: List of AQS parameter codes
        year: Year to process
    
    Returns:
        Dictionary mapping pollutant codes to output paths
    """
    results = {}
    
    for pollutant in pollutants:
        logger.info(f"[TX] Processing pollutant {pollutant} for year {year}")
        try:
            out_path = convert_year_single(pollutant, year)
            results[pollutant] = out_path
        except Exception as e:
            logger.error(f"[TX] Failed for pollutant {pollutant}: {e}")
            results[pollutant] = None
    
    return results


def convert_year_single(param_code: str, year: int) -> Path:
    """Convert one pollutant for one year with Envista fallback."""
    
    # Load AQS data - FIXED: Look in year subdirectory
    aqs_year_dir = RAW_AQS_ROOT / param_code / str(year)
    aqs_files = list(aqs_year_dir.glob("*.csv.zip")) if aqs_year_dir.exists() else []
    aqs_dfs = []
    
    for file_path in aqs_files:
        try:
            df = pd.read_csv(file_path, compression="zip")
            # Filter for the target year (should already be correct, but double-check)
            if "date_local" in df.columns:
                df["date_local"] = pd.to_datetime(df["date_local"])
                df = df[df["date_local"].dt.year == year]
                df["date_local"] = df["date_local"].dt.strftime("%Y-%m-%d")
            aqs_dfs.append(df)
        except Exception as e:
            logger.warning(f"[TX] Failed to read AQS file {file_path}: {e}")
    
    aqs_data = pd.concat(aqs_dfs, ignore_index=True) if aqs_dfs else pd.DataFrame()
    
    # Load Envista data - FIXED: Look in year subdirectory
    envista_year_dir = RAW_ENVISTA_ROOT / param_code / str(year)
    envista_files = list(envista_year_dir.glob("*.csv.zip")) if envista_year_dir.exists() else []
    envista_dfs = []
    
    for file_path in envista_files:
        try:
            df = pd.read_csv(file_path, compression="zip")
            # Filter for the target year
            if "date_local" in df.columns:
                df["date_local"] = pd.to_datetime(df["date_local"])
                df = df[df["date_local"].dt.year == year]
                df["date_local"] = df["date_local"].dt.strftime("%Y-%m-%d")
            envista_dfs.append(df)
        except Exception as e:
            logger.warning(f"[TX] Failed to read Envista file {file_path}: {e}")
    
    envista_data = pd.concat(envista_dfs, ignore_index=True) if envista_dfs else pd.DataFrame()
    
    # Clean and standardize data
    if not aqs_data.empty:
        aqs_data = _clean_columns(aqs_data)
        aqs_data["data_source"] = "AQS"
    
    if not envista_data.empty:
        envista_data = _clean_columns(envista_data)
        envista_data["data_source"] = "Envista"
    
    # Merge with fallback logic
    merged_data = _merge_aqs_envista_data(aqs_data, envista_data)
    
    if merged_data.empty:
        logger.warning(f"[TX] No data found for {param_code} {year}")
        raise RuntimeError(f"No data available for {param_code} {year}")
    
    # Group by date and state for output files
    if "date_local" in merged_data.columns and "state_code" in merged_data.columns:
        merged_data["date"] = pd.to_datetime(merged_data["date_local"])
        merged_data["month_day"] = merged_data["date"].dt.strftime("%m%d")
        
        for (month_day, state), group in merged_data.groupby(["month_day", "state_code"]):
            out_dir = TFM_ROOT / param_code / str(year)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{month_day}_{state}.parquet"
            
            # Remove temporary columns
            group = group.drop(["date", "month_day"], axis=1, errors="ignore")
            
            group.to_parquet(out_path, index=False)
            logger.success(f"[TX] {param_code} {year} {month_day}_{state} → {len(group)} rows")
    else:
        # Fallback: save all data to single file
        out_dir = TFM_ROOT / param_code / str(year)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "hourly.parquet"
        
        merged_data.to_parquet(out_path, index=False)
        logger.success(f"[TX] {param_code} {year} → {len(merged_data)} rows")
    
    return out_path


def convert_file(raw_zip: Path) -> Path:
    """Convert one raw zip → parquet path (legacy function for backward compatibility)."""
    
    if not raw_zip.suffix == ".zip":
        raise ValueError(raw_zip)
    
    # Determine if this is AQS or Envista data
    if "envista" in str(raw_zip):
        # Envista data
        df = pd.read_csv(raw_zip, compression="zip")
        df = _clean_columns(df)
        df["data_source"] = "Envista"
    else:
        # AQS data
        df = pd.read_csv(raw_zip, compression="zip")
        df = _clean_columns(df)
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
