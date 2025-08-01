from pathlib import Path
import pandas as pd
from loguru import logger

# -------------------
# Dimension Table Writers
# -------------------

def write_dim_date(years: list[int]) -> Path:
    """Write dimDate: full calendar with flags for year, quarter, season, day of week.
    Output: data/stage/dimDate.csv.zip
    """
    # Generate date range for all years
    min_year, max_year = min(years), max(years)
    start = pd.Timestamp(f"{min_year}-01-01")
    end = pd.Timestamp(f"{max_year}-12-31")
    dates = pd.date_range(start, end, freq="D")
    df = pd.DataFrame({"date": dates})
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    # Simple season logic: DJF=Winter, MAM=Spring, JJA=Summer, SON=Fall
    def get_season(dt):
        m = dt.month
        if m in [12, 1, 2]: 
            return "Winter"
        if m in [3, 4, 5]: 
            return "Spring"
        if m in [6, 7, 8]: 
            return "Summer"
        return "Fall"
    df["season"] = df["date"].apply(get_season)
    df["day_of_week"] = df["date"].dt.day_name()
    df["is_weekend"] = df["date"].dt.weekday >= 5
    df["is_holiday"] = False  # TODO: Add holiday logic if needed
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dimDate.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] dimDate → {} rows", len(df))
    return out_path


def write_dim_sites() -> Path:
    """Write dimSites: monitoring site metadata from AQS monitors API.
    Output: data/stage/dimSites.csv.zip
    """
    # Read from raw AQS data to extract site metadata
    raw_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
    if not raw_dir.exists():
        logger.warning("[STG] raw_dir {} does not exist, skipping dimSites", raw_dir)
        return None
    
    sites_data = []
    for pollutant_dir in raw_dir.iterdir():
        if not pollutant_dir.is_dir():
            continue
        for year_file in pollutant_dir.glob("*.csv.zip"):
            try:
                df = pd.read_csv(year_file, compression="zip")
                # Extract unique site information
                site_cols = ["state_code", "county_code", "site_number", "latitude", "longitude"]
                if all(col in df.columns for col in site_cols):
                    sites = df[site_cols].drop_duplicates()
                    sites["pollutant"] = pollutant_dir.name
                    sites_data.append(sites)
            except Exception as e:
                logger.warning("[STG] Error reading {}: {}", year_file, e)
    
    if not sites_data:
        logger.warning("[STG] No site data found for dimSites")
        return None
    
    df = pd.concat(sites_data, ignore_index=True).drop_duplicates()
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dimSites.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] dimSites → {} rows", len(df))
    return out_path


def write_dim_pollutant() -> Path:
    """Write dimPollutant: pollutant names, codes, crosswalks.
    Output: data/stage/dimPollutant.csv.zip
    """
    # Common AQS parameter codes and names
    pollutants = [
        {"parameter_code": "88101", "parameter_name": "PM2.5 - Local Conditions", "units": "µg/m³"},
        {"parameter_code": "81102", "parameter_name": "PM10 Total 0-10um STP", "units": "µg/m³"},
        {"parameter_code": "44201", "parameter_name": "Ozone", "units": "ppm"},
        {"parameter_code": "42101", "parameter_name": "Carbon monoxide", "units": "ppm"},
        {"parameter_code": "42401", "parameter_name": "Sulfur dioxide", "units": "ppb"},
        {"parameter_code": "42602", "parameter_name": "Nitrogen dioxide (NO2)", "units": "ppb"},
        {"parameter_code": "88305", "parameter_name": "Black Carbon", "units": "µg/m³"},
        {"parameter_code": "88306", "parameter_name": "Elemental Carbon", "units": "µg/m³"},
        {"parameter_code": "88307", "parameter_name": "Organic Carbon", "units": "µg/m³"},
    ]
    
    df = pd.DataFrame(pollutants)
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dimPollutant.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] dimPollutant → {} rows", len(df))
    return out_path


def write_dim_county() -> Path:
    """Write dimCounty: geographic reference for county from AQS data.
    Output: data/stage/dimCounty.csv.zip
    """
    # Extract county information from raw AQS data
    raw_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
    if not raw_dir.exists():
        logger.warning("[STG] raw_dir {} does not exist, skipping dimCounty", raw_dir)
        return None
    
    counties_data = []
    for pollutant_dir in raw_dir.iterdir():
        if not pollutant_dir.is_dir():
            continue
        for year_file in pollutant_dir.glob("*.csv.zip"):
            try:
                df = pd.read_csv(year_file, compression="zip")
                # Extract unique county information
                county_cols = ["state_code", "county_code", "state_name", "county_name"]
                if all(col in df.columns for col in county_cols):
                    counties = df[county_cols].drop_duplicates()
                    counties_data.append(counties)
            except Exception as e:
                logger.warning("[STG] Error reading {}: {}", year_file, e)
    
    if not counties_data:
        logger.warning("[STG] No county data found for dimCounty")
        return None
    
    df = pd.concat(counties_data, ignore_index=True).drop_duplicates()
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dimCounty.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] dimCounty → {} rows", len(df))
    return out_path


def write_dim_censustract() -> Path:
    """Write dimCensusTract: geographic reference for census tract (from census/reference).
    Output: data/stage/dimCensusTract.csv.zip
    TODO: Requires census tract reference data.
    """
    # TODO: Implement when data available
    raise NotImplementedError("dimCensusTract not yet implemented")


def write_dim_trv() -> Path:
    """Write dimTRV: TRV values and thresholds by pollutant.
    Output: data/stage/dimTRV.csv.zip
    TODO: Requires TRV table from client/reference.
    """
    # TODO: Implement when data available
    raise NotImplementedError("dimTRV not yet implemented")


def write_dim_aqi() -> Path:
    """Write dimAQI: EPA AQI breakpoints by pollutant.
    Output: data/stage/dimAQI.csv.zip
    """
    # EPA AQI breakpoints for common pollutants
    aqi_breakpoints = [
        # PM2.5 (24-hour)
        {"pollutant": "88101", "aqi_low": 0, "aqi_high": 50, "conc_low": 0.0, "conc_high": 12.0, "category": "Good", "color": "Green"},
        {"pollutant": "88101", "aqi_low": 51, "aqi_high": 100, "conc_low": 12.1, "conc_high": 35.4, "category": "Moderate", "color": "Yellow"},
        {"pollutant": "88101", "aqi_low": 101, "aqi_high": 150, "conc_low": 35.5, "conc_high": 55.4, "category": "Unhealthy for Sensitive Groups", "color": "Orange"},
        {"pollutant": "88101", "aqi_low": 151, "aqi_high": 200, "conc_low": 55.5, "conc_high": 150.4, "category": "Unhealthy", "color": "Red"},
        {"pollutant": "88101", "aqi_low": 201, "aqi_high": 300, "conc_low": 150.5, "conc_high": 250.4, "category": "Very Unhealthy", "color": "Purple"},
        {"pollutant": "88101", "aqi_low": 301, "aqi_high": 500, "conc_low": 250.5, "conc_high": 500.4, "category": "Hazardous", "color": "Maroon"},
        
        # PM10 (24-hour)
        {"pollutant": "81102", "aqi_low": 0, "aqi_high": 50, "conc_low": 0, "conc_high": 54, "category": "Good", "color": "Green"},
        {"pollutant": "81102", "aqi_low": 51, "aqi_high": 100, "conc_low": 55, "conc_high": 154, "category": "Moderate", "color": "Yellow"},
        {"pollutant": "81102", "aqi_low": 101, "aqi_high": 150, "conc_low": 155, "conc_high": 254, "category": "Unhealthy for Sensitive Groups", "color": "Orange"},
        {"pollutant": "81102", "aqi_low": 151, "aqi_high": 200, "conc_low": 255, "conc_high": 354, "category": "Unhealthy", "color": "Red"},
        {"pollutant": "81102", "aqi_low": 201, "aqi_high": 300, "conc_low": 355, "conc_high": 424, "category": "Very Unhealthy", "color": "Purple"},
        {"pollutant": "81102", "aqi_low": 301, "aqi_high": 500, "conc_low": 425, "conc_high": 604, "category": "Hazardous", "color": "Maroon"},
        
        # Ozone (8-hour)
        {"pollutant": "44201", "aqi_low": 0, "aqi_high": 50, "conc_low": 0.000, "conc_high": 0.054, "category": "Good", "color": "Green"},
        {"pollutant": "44201", "aqi_low": 51, "aqi_high": 100, "conc_low": 0.055, "conc_high": 0.070, "category": "Moderate", "color": "Yellow"},
        {"pollutant": "44201", "aqi_low": 101, "aqi_high": 150, "conc_low": 0.071, "conc_high": 0.085, "category": "Unhealthy for Sensitive Groups", "color": "Orange"},
        {"pollutant": "44201", "aqi_low": 151, "aqi_high": 200, "conc_low": 0.086, "conc_high": 0.105, "category": "Unhealthy", "color": "Red"},
        {"pollutant": "44201", "aqi_low": 201, "aqi_high": 300, "conc_low": 0.106, "conc_high": 0.200, "category": "Very Unhealthy", "color": "Purple"},
        {"pollutant": "44201", "aqi_low": 301, "aqi_high": 500, "conc_low": 0.201, "conc_high": 0.404, "category": "Hazardous", "color": "Maroon"},
    ]
    
    df = pd.DataFrame(aqi_breakpoints)
    out_dir = Path(__file__).resolve().parents[2] / "data" / "stage"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dimAQI.csv.zip"
    df.to_csv(out_path, index=False, compression="zip")
    logger.success("[STG] dimAQI → {} rows", len(df))
    return out_path


def write_dim_sourcecategory() -> Path:
    """Write dimSourceCategory: emission source categories/subcategories.
    Output: data/stage/dimSourceCategory.csv.zip
    TODO: Requires source category data from reference.
    """
    # TODO: Implement when data available
    raise NotImplementedError("dimSourceCategory not yet implemented") 