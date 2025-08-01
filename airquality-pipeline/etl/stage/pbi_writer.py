"""Stage layer – Power BI CSV writer (hourly + daily tables).

Implemented fact tables
-----------------------
* **fctPM25Hourly** – param *88101*
* **fctPM10Hourly** – param *81102*
* **fctO3Hourly**   – param *44201*
* **fctAQIDaily**   – any pollutant with daily AQI Parquet
* **fctAQICategory** – count of days in each AQI category by city × year
* **fctOtherPollutantsHourly** – all other pollutants except PM2.5, PM10, O3
* **fctWildfireFlags** – wildfire event flags by site and date

Output paths
------------
All tables: data/stage/<table>/<year>.csv.zip
Daily AQI: data/stage/fctAQIDaily/<param>_<year>.csv.zip
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from loguru import logger

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
TFM_HOURLY = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"
TFM_DAILY = Path(__file__).resolve().parents[2] / "data" / "transform" / "daily_aqi" / "synchronous"
STG_ROOT = Path(__file__).resolve().parents[2] / "data" / "stage"
STG_ROOT.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------------

def _write_hourly_generic(param_code: str, table_name: str, year: int) -> Path:
    """Stage hourly Parquet → zipped CSV for Power BI."""

    src_dir = TFM_HOURLY / param_code / str(year)
    if not src_dir.exists():
        logger.error("[STG] hourly dir not found: {}", src_dir)
        raise FileNotFoundError(src_dir)

    dfs = [pd.read_parquet(p) for p in src_dir.glob("*.parquet")]
    if not dfs:
        raise RuntimeError(f"No hourly Parquet files for {param_code} {year}")

    fact = pd.concat(dfs, ignore_index=True)[[
        "state_code", "county_code", "site_number",
        "parameter_code", "date_local", "time_local", "sample_measurement",
        "data_source"  # Add data source flag
    ]]

    out_dir = STG_ROOT / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{year}.csv.zip"
    fact.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] {} {} → {} rows", table_name, year, len(fact))
    return out_path

# ----------------------------------------------------------------------------
# Public wrappers – extend as needed
# ----------------------------------------------------------------------------

def write_pm25_hourly(year: int) -> Path:
    return _write_hourly_generic("88101", "fctPM25Hourly", year)


def write_pm10_hourly(year: int) -> Path:
    return _write_hourly_generic("81102", "fctPM10Hourly", year)


def write_o3_hourly(year: int) -> Path:
    return _write_hourly_generic("44201", "fctO3Hourly", year)

# ----------------------------------------------------------------------------
# Daily AQI staging
# ----------------------------------------------------------------------------

def write_aqi_daily(param_code: str, year: int) -> Path:
    src = TFM_DAILY / param_code / str(year) / "daily_aqi.parquet"
    if not src.exists():
        logger.error("[STG] daily AQI src missing: {}", src)
        raise FileNotFoundError(src)

    fact = pd.read_parquet(src)[[
        "state_code", "county_code", "site_number", "date", "aqi", "conc_avg",
        "data_source"  # Add data source flag
    ]]

    out_dir = STG_ROOT / "fctAQIDaily"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{param_code}_{year}.csv.zip"
    fact.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] fctAQIDaily {} {} → {} rows", param_code, year, len(fact))
    return out_path

def write_aqi_category(year: int) -> Path:
    """Write fctAQICategory: number of days in each AQI category by city, year.
    Reads from daily AQI transform outputs. Writes zipped CSV to data/stage/fctAQICategory/<year>.csv.zip.
    """
    # Standard AQI categories (can be expanded as needed)
    AQI_CATEGORIES = [
        (0, 50, "Good"),
        (51, 100, "Moderate"),
        (101, 150, "Unhealthy for Sensitive Groups"),
        (151, 200, "Unhealthy"),
        (201, 300, "Very Unhealthy"),
        (301, 500, "Hazardous"),
    ]
    def categorize_aqi(aqi):
        for low, high, cat in AQI_CATEGORIES:
            if low <= aqi <= high:
                return cat
        return "Unknown"

    # Read all daily AQI Parquet files for the year
    daily_dir = Path(__file__).resolve().parents[2] / "data" / "transform" / "daily_aqi" / "synchronous"
    dfs = []
    for pollutant_dir in daily_dir.iterdir():
        year_dir = pollutant_dir / str(year)
        daily_file = year_dir / "daily_aqi.parquet"
        if daily_file.exists():
            df = pd.read_parquet(daily_file)
            df["pollutant"] = pollutant_dir.name
            dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No daily AQI data for year {year}")
    df = pd.concat(dfs, ignore_index=True)
    if "aqi" not in df.columns:
        raise RuntimeError("No 'aqi' column in daily AQI data")
    # Add AQI category
    df["aqi_category"] = df["aqi"].apply(categorize_aqi)
    # Use city_name if available, else site_number
    group_fields = ["pollutant", "aqi_category", "state_code", "county_code"]
    if "city_name" in df.columns:
        group_fields.append("city_name")
    else:
        group_fields.append("site_number")
    df["year"] = year
    agg = df.groupby(group_fields + ["year"]).size().reset_index(name="days")
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage" / "fctAQICategory"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{year}.csv.zip"
    agg.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] fctAQICategory {} → {} rows", year, len(agg))
    return out_path


def write_other_pollutants_hourly(year: int) -> Path:
    """Write fctOtherPollutantsHourly: hourly data for all pollutants except PM2.5, PM10, O3.
    Reads from hourly transform outputs. Writes zipped CSV to data/stage/fctOtherPollutantsHourly/<year>.csv.zip.
    """
    EXCLUDE = {"pm25", "pm10", "o3"}
    hourly_dir = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"
    if not hourly_dir.exists():
        logger.warning("[STG] hourly_dir {} does not exist, skipping fctOtherPollutantsHourly", hourly_dir)
        return None
    dfs = []
    for pollutant_dir in hourly_dir.iterdir():
        if pollutant_dir.name.lower() in EXCLUDE:
            continue
        year_dir = pollutant_dir / str(year)
        hourly_file = year_dir / "hourly.parquet"
        if hourly_file.exists():
            df = pd.read_parquet(hourly_file)
            df["pollutant"] = pollutant_dir.name
            dfs.append(df)
    if not dfs:
        logger.warning("[STG] No other pollutant hourly data for year {}", year)
        return None
    df = pd.concat(dfs, ignore_index=True)
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage" / "fctOtherPollutantsHourly"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{year}.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] fctOtherPollutantsHourly {} → {} rows", year, len(df))
    return out_path


def write_bc_hourly(year: int) -> Path:
    """Write fctBCHourly: hourly black carbon data.
    Output: data/stage/fctBCHourly/<year>.csv.zip
    TODO: Implement if black carbon data is available in transform outputs.
    """
    # Look for black carbon parameter codes (e.g., 88305 for BC)
    BC_CODES = ["88305"]  # Add more BC codes as needed
    hourly_dir = Path(__file__).resolve().parents[2] / "data" / "transform" / "hourly"
    dfs = []
    for code in BC_CODES:
        code_dir = hourly_dir / code / str(year)
        hourly_file = code_dir / "hourly.parquet"
        if hourly_file.exists():
            df = pd.read_parquet(hourly_file)
            df["pollutant"] = "bc"
            dfs.append(df)
    if not dfs:
        logger.warning("[STG] No black carbon data for year {}", year)
        return None
    df = pd.concat(dfs, ignore_index=True)
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage" / "fctBCHourly"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{year}.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] fctBCHourly {} → {} rows", year, len(df))
    return out_path


def write_toxics_annual(year: int) -> Path:
    """Write fctToxicsAnnual: annual toxics metrics with TRV exceedances.
    Output: data/stage/fctToxicsAnnual/<year>.csv.zip
    TODO: Requires TRV and toxics data from client/reference.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctToxicsAnnual not yet implemented")


def write_toxics_daily(year: int) -> Path:
    """Write fctToxicsDaily: daily toxics metrics with TRV exceedances.
    Output: data/stage/fctToxicsDaily/<year>.csv.zip
    TODO: Requires TRV and toxics data from client/reference.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctToxicsDaily not yet implemented")


def write_emissions_county(year: int) -> Path:
    """Write fctEmissionsCounty: annual emissions totals by county.
    Output: data/stage/fctEmissionsCounty/<year>.csv.zip
    TODO: Requires emissions data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctEmissionsCounty not yet implemented")


def write_emissions_censustract(year: int) -> Path:
    """Write fctEmissionsCensusTract: annual emissions by census tract.
    Output: data/stage/fctEmissionsCensusTract/<year>.csv.zip
    TODO: Requires emissions data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctEmissionsCensusTract not yet implemented")


def write_emissions_atei(year: int) -> Path:
    """Write fctEmissionsATEI: ATEI point source emissions of toxics pollutants.
    Output: data/stage/fctEmissionsATEI/<year>.csv.zip
    TODO: Requires ATEI data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctEmissionsATEI not yet implemented")


def write_atsrisk_county(year: int) -> Path:
    """Write fctATSRiskCounty: ATS chronic risk exposure by county.
    Output: data/stage/fctATSRiskCounty/<year>.csv.zip
    TODO: Requires ATS risk data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctATSRiskCounty not yet implemented")


def write_atsrisk_tract(year: int) -> Path:
    """Write fctATSRiskTract: ATS chronic risk exposure by census tract.
    Output: data/stage/fctATSRiskTract/<year>.csv.zip
    TODO: Requires ATS risk data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctATSRiskTract not yet implemented")


def write_atsconcentrations_tract(year: int) -> Path:
    """Write fctATSConcentrationsTract: ATS ambient concentrations by census tract.
    Output: data/stage/fctATSConcentrationsTract/<year>.csv.zip
    TODO: Requires ATS concentration data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctATSConcentrationsTract not yet implemented")


def write_noxsat(year: int) -> Path:
    """Write fctNOXSat: NOX enhancements from satellite data.
    Output: data/stage/fctNOXSat/<year>.csv.zip
    TODO: Requires satellite data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctNOXSat not yet implemented")


def write_hourly_pams(year: int) -> Path:
    """Write fctHourlyPAMS: hourly VOCs (SEL).
    Output: data/stage/fctHourlyPAMS/<year>.csv.zip
    TODO: Requires PAMS data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctHourlyPAMS not yet implemented")


def write_eighthour_pams(year: int) -> Path:
    """Write fctEightHourPAMS: 8-hour carbonyls (SEL).
    Output: data/stage/fctEightHourPAMS/<year>.csv.zip
    TODO: Requires PAMS data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctEightHourPAMS not yet implemented")


def write_hourly_met(year: int) -> Path:
    """Write fctHourlyMet: hourly meteorology data.
    Output: data/stage/fctHourlyMet/<year>.csv.zip
    TODO: Requires meteorology data from reference/metadata.
    """
    # TODO: Implement when data available
    raise NotImplementedError("fctHourlyMet not yet implemented")


def write_wildfire_flags(year: int) -> Path:
    """Write fctWildfireFlags: days flagged as wildfire event by site and date (from PM2.5 daily AQI).
    Output: data/stage/fctWildfireFlags/<year>.csv.zip
    """
    daily_pm25 = Path(__file__).resolve().parents[2] / "data" / "transform" / "daily_aqi" / "synchronous" / "pm25" / str(year) / "daily_aqi.parquet"
    if not daily_pm25.exists():
        logger.warning("No PM2.5 daily AQI data for year {}", year)
        return None
    df = pd.read_parquet(daily_pm25)
    if "wildfire_flag" not in df.columns:
        logger.warning("No wildfire_flag column in PM2.5 daily AQI for year {}", year)
        return None
    flagged = df[df["wildfire_flag"] == 1].copy()
    if flagged.empty:
        logger.info("No wildfire-flagged days for year {}", year)
        return None
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage" / "fctWildfireFlags"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{year}.csv.zip"
    flagged.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] fctWildfireFlags {} → {} rows", year, len(flagged))
    return out_path
