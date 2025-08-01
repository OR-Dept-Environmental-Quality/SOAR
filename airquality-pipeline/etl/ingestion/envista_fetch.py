"""Envista API ingestion module.

Implements fallback/supplemental data ingestion from Envista API.
- Fallback: Latest O3 & PM readings when AQS has latency
- Supplemental: Additional pollutants absent from AQS
- Easy credential configuration via environment variables

Usage:
    from etl.ingestion.envista_fetch import fetch_hourly_data
    fetch_hourly_data(station_id, channel_id, start_date, end_date)
"""

from __future__ import annotations
import os
import requests
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
from loguru import logger
import time
from typing import Optional, Dict, List, Any

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
ENVISTA_BASE_URL = os.getenv("ENVISTA_BASE_URL", "https://api.envista.com/")
ENVISTA_USERNAME = os.getenv("ENVISTA_USERNAME", "")
ENVISTA_PASSWORD = os.getenv("ENVISTA_PASSWORD", "")
ENVISTA_API_KEY = os.getenv("ENVISTA_API_KEY", "")

# Rate limiting (polite API usage)
REQUEST_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3

# Output paths
RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw" / "envista"
RAW_ROOT.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# API Client
# ----------------------------------------------------------------------------

class EnvistaClient:
    """Envista API client with authentication and rate limiting."""
    
    def __init__(self, base_url: str = None, username: str = None, 
                 password: str = None, api_key: str = None):
        """Initialize Envista client with credentials."""
        self.base_url = base_url or ENVISTA_BASE_URL.rstrip('/')
        self.username = username or ENVISTA_USERNAME
        self.password = password or ENVISTA_PASSWORD
        self.api_key = api_key or ENVISTA_API_KEY
        self.session = requests.Session()
        self.last_request_time = 0
        
        # Configure authentication
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        elif self.username and self.password:
            self.session.auth = (self.username, self.password)
        else:
            logger.warning("[ENV] No credentials provided - using unauthenticated requests")
    
    def _rate_limit(self):
        """Implement polite API usage with rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make authenticated API request with retry logic."""
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"[ENV] Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def get_regions(self) -> List[Dict[str, Any]]:
        """Get list of available regions."""
        return self._make_request("/v1/envista/regions")
    
    def get_stations(self, region_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of stations with optional region filter."""
        params = {"region_id": region_id} if region_id else {}
        return self._make_request("/v1/envista/stations", params)
    
    def get_station_metadata(self, station_id: str) -> Dict[str, Any]:
        """Get detailed station metadata including monitors/channels."""
        return self._make_request(f"/v1/envista/stations/{station_id}")
    
    def get_hourly_data(self, station_id: str, channel_id: str, 
                       start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Get hourly data for a specific station and channel."""
        params = {
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "timebase": 60  # 60 minutes = hourly
        }
        return self._make_request(
            f"/v1/envista/stations/{station_id}/data/{channel_id}", 
            params
        )

# ----------------------------------------------------------------------------
# Data Processing
# ----------------------------------------------------------------------------

def _standardize_envista_data(raw_data: List[Dict[str, Any]], 
                             station_metadata: Dict[str, Any]) -> pd.DataFrame:
    """Convert Envista API response to standardized DataFrame."""
    if not raw_data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    # Standardize column names
    column_mapping = {
        "datetime": "date_local",
        "value": "sample_measurement",
        "status": "status",
        "valid": "valid",
        "description": "description",
        "parameter": "parameter_name",
        "units_of_measure": "units",
        "method_code": "method_code",
        "latitude": "latitude",
        "longitude": "longitude",
        "site": "site_name",
        "qualifier": "qualifier",
        "sample_frequency": "sample_frequency"
    }
    
    df = df.rename(columns=column_mapping)
    
    # Add required AQS-compatible columns
    df["data_source"] = "Envista"
    df["parameter_code"] = _map_envista_to_aqs_parameter(df.get("parameter_name", "").iloc[0] if len(df) > 0 else "")
    
    # Extract station info from metadata
    if station_metadata:
        df["state_code"] = station_metadata.get("state_code", "")
        df["county_code"] = station_metadata.get("county_code", "")
        df["site_number"] = station_metadata.get("site_number", "")
        df["station_id"] = station_metadata.get("station_id", "")
        df["channel_id"] = station_metadata.get("channel_id", "")
    
    # Convert datetime
    if "date_local" in df.columns:
        df["date_local"] = pd.to_datetime(df["date_local"])
        df["time_local"] = df["date_local"].dt.strftime("%H:%M")
        df["date_local"] = df["date_local"].dt.strftime("%Y-%m-%d")
    
    # Ensure numeric measurement
    if "sample_measurement" in df.columns:
        df["sample_measurement"] = pd.to_numeric(df["sample_measurement"], errors="coerce")
    
    return df

def _map_envista_to_aqs_parameter(envista_param: str) -> str:
    """Map Envista parameter names to AQS parameter codes."""
    param_mapping = {
        "PM2.5": "88101",
        "PM10": "81102", 
        "Ozone": "44201",
        "Carbon Monoxide": "42101",
        "Sulfur Dioxide": "42401",
        "Nitrogen Dioxide": "42602",
        "Black Carbon": "88305",
        "Elemental Carbon": "88306",
        "Organic Carbon": "88307"
    }
    
    for envista_name, aqs_code in param_mapping.items():
        if envista_name.lower() in envista_param.lower():
            return aqs_code
    
    # Return original if no mapping found
    return envista_param

# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

def fetch_hourly_data(station_id: str, channel_id: str, 
                     start_date: date, end_date: date,
                     client: Optional[EnvistaClient] = None) -> Path:
    """Fetch hourly data from Envista API and save to raw layer.
    
    Args:
        station_id: Envista station identifier
        channel_id: Envista channel identifier  
        start_date: Start date for data fetch
        end_date: End date for data fetch
        client: Optional pre-configured EnvistaClient
    
    Returns:
        Path to saved CSV file
    """
    if client is None:
        client = EnvistaClient()
    
    logger.info(f"[ENV] Fetching data for station {station_id}, channel {channel_id}")
    logger.info(f"[ENV] Date range: {start_date} to {end_date}")
    
    try:
        # Get station metadata
        station_metadata = client.get_station_metadata(station_id)
        logger.info(f"[ENV] Retrieved metadata for station {station_id}")
        
        # Get hourly data
        raw_data = client.get_hourly_data(station_id, channel_id, start_date, end_date)
        logger.info(f"[ENV] Retrieved {len(raw_data)} data points")
        
        if not raw_data:
            logger.warning(f"[ENV] No data found for {station_id}/{channel_id}")
            return None
        
        # Standardize data
        df = _standardize_envista_data(raw_data, station_metadata)
        
        if df.empty:
            logger.warning("[ENV] No valid data after processing")
            return None
        
        # Determine output path
        param_code = df["parameter_code"].iloc[0]
        year = start_date.year
        
        out_dir = RAW_ROOT / param_code / str(year)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        filename = f"envista_{station_id}_{channel_id}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv.zip"
        out_path = out_dir / filename
        
        # Save to CSV
        df.to_csv(out_path, index=False, compression="zip")
        logger.success(f"[ENV] Saved {len(df)} rows to {out_path}")
        
        return out_path
        
    except Exception as e:
        logger.error(f"[ENV] Failed to fetch data: {e}")
        raise

def fetch_fallback_data(pollutant: str, start_date: date, end_date: date,
                       stations: List[str] = None) -> List[Path]:
    """Fetch fallback data for specific pollutant when AQS is unavailable.
    
    Args:
        pollutant: AQS parameter code (e.g., "88101" for PM2.5)
        start_date: Start date for data fetch
        end_date: End date for data fetch
        stations: Optional list of station IDs to query
    
    Returns:
        List of paths to saved CSV files
    """
    client = EnvistaClient()
    
    # Get available stations if not specified
    if not stations:
        all_stations = client.get_stations()
        stations = [s["station_id"] for s in all_stations]
    
    results = []
    
    for station_id in stations:
        try:
            # Get station metadata to find relevant channels
            station_metadata = client.get_station_metadata(station_id)
            channels = station_metadata.get("channels", [])
            
            # Find channels for the requested pollutant
            for channel in channels:
                if _map_envista_to_aqs_parameter(channel.get("parameter", "")) == pollutant:
                    channel_id = channel["channel_id"]
                    
                    try:
                        result = fetch_hourly_data(station_id, channel_id, start_date, end_date, client)
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.warning(f"[ENV] Failed to fetch {station_id}/{channel_id}: {e}")
                        
        except Exception as e:
            logger.warning(f"[ENV] Failed to get metadata for station {station_id}: {e}")
    
    logger.info(f"[ENV] Fallback fetch complete: {len(results)} files saved")
    return results

def discover_available_data(start_date: date = None, end_date: date = None) -> Dict[str, Any]:
    """Discover available Envista data sources and parameters.
    
    Returns:
        Dictionary with available regions, stations, and parameters
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=30)
    if end_date is None:
        end_date = date.today()
    
    client = EnvistaClient()
    
    try:
        # Get regions
        regions = client.get_regions()
        logger.info(f"[ENV] Found {len(regions)} regions")
        
        # Get stations
        stations = client.get_stations()
        logger.info(f"[ENV] Found {len(stations)} stations")
        
        # Get parameters from recent data
        parameters = set()
        for station in stations[:5]:  # Sample first 5 stations
            try:
                metadata = client.get_station_metadata(station["station_id"])
                for channel in metadata.get("channels", []):
                    param = channel.get("parameter", "")
                    if param:
                        parameters.add(param)
            except Exception as e:
                logger.warning(f"[ENV] Failed to get metadata for station {station['station_id']}: {e}")
        
        return {
            "regions": regions,
            "stations": stations,
            "parameters": list(parameters),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"[ENV] Failed to discover data: {e}")
        return {}

# ----------------------------------------------------------------------------
# CLI Interface
# ----------------------------------------------------------------------------

def main():
    """CLI interface for Envista data fetching."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch data from Envista API")
    parser.add_argument("--station", required=True, help="Station ID")
    parser.add_argument("--channel", required=True, help="Channel ID")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--discover", action="store_true", help="Discover available data sources")
    
    args = parser.parse_args()
    
    if args.discover:
        data = discover_available_data()
        print("Available Envista data sources:")
        print(f"Regions: {len(data.get('regions', []))}")
        print(f"Stations: {len(data.get('stations', []))}")
        print(f"Parameters: {data.get('parameters', [])}")
        return
    
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    
    result = fetch_hourly_data(args.station, args.channel, start_date, end_date)
    if result:
        print(f"Data saved to: {result}")

if __name__ == "__main__":
    main() 