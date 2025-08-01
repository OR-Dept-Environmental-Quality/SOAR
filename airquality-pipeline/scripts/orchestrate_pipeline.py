"""
Orchestrate the full AQS/Envista pipeline: ingestion → transform (hourly, daily AQI) → stage.

Usage:
    python scripts/orchestrate_pipeline.py --pollutants 88101,81102 --year 2024 --start 2024-03-01 --end 2024-03-03 --states 37,41

- Ingests raw data from AQS (primary) and Envista (fallback/supplemental) for all pollutants, date range, and states
- Runs hourly and daily AQI transforms with fallback logic (AQS → Envista)
- Writes stage outputs for all pollutants and dimension tables

Features:
- AQS primary data ingestion with polite API usage
- Envista fallback for missing AQS data (O3 & PM)
- Envista supplemental data for additional pollutants
- Automatic data source flagging (AQS vs Envista)
- Complete Power BI stage layer with all fact and dimension tables
"""

import argparse
from datetime import datetime
from loguru import logger
from etl.ingestion.aqs_state import fetch_range_multi
from etl.ingestion.envista_fetch import fetch_fallback_data, discover_available_data
from etl.transform.batch_hourly import convert_year_multi
from etl.transform.daily_aqi import batch_calc_year
from etl.stage import pbi_writer
from etl.stage import dim_writer
from etl.stage import monitor_coverage


def main():
    parser = argparse.ArgumentParser(description="Orchestrate full AQS/Envista pipeline")
    parser.add_argument("--pollutants", required=True, type=str, help="Comma-separated EPA parameter codes, e.g. 88101,81102")
    parser.add_argument("--year", required=True, type=int, help="Year to process (e.g. 2024)")
    parser.add_argument("--start", required=True, type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--states", required=True, type=str, help="Comma-separated list of state FIPS codes, e.g. 37,41")
    parser.add_argument("--envista-fallback", action="store_true", help="Enable Envista fallback for missing AQS data")
    parser.add_argument("--discover-envista", action="store_true", help="Discover available Envista data sources")
    args = parser.parse_args()

    pollutants = [p.strip() for p in args.pollutants.split(",") if p.strip()]
    year = args.year
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    states = [s.strip() for s in args.states.split(",") if s.strip()]

    # Discover Envista data sources if requested
    if args.discover_envista:
        logger.info("[ORCH] Discovering available Envista data sources...")
        envista_data = discover_available_data(start, end)
        if envista_data:
            logger.info(f"[ORCH] Found {len(envista_data.get('regions', []))} regions")
            logger.info(f"[ORCH] Found {len(envista_data.get('stations', []))} stations")
            logger.info(f"[ORCH] Available parameters: {envista_data.get('parameters', [])}")
        else:
            logger.warning("[ORCH] No Envista data sources discovered")
        return

    # Step 1: AQS Primary Ingestion
    logger.info("[ORCH] Starting AQS primary ingestion for pollutants {}", pollutants)
    try:
        fetch_range_multi(pollutants, start, end, states)
        logger.success("[ORCH] AQS ingestion completed successfully")
    except Exception as e:
        logger.error("[ORCH] AQS ingestion failed: {}", e)
        if not args.envista_fallback:
            raise

    # Step 2: Envista Fallback/Supplemental Ingestion
    if args.envista_fallback:
        logger.info("[ORCH] Starting Envista fallback/supplemental ingestion")
        for pollutant in pollutants:
            try:
                # Fetch fallback data for missing AQS data
                envista_files = fetch_fallback_data(pollutant, start, end)
                if envista_files:
                    logger.success("[ORCH] Envista fallback for {}: {} files", pollutant, len(envista_files))
                else:
                    logger.info("[ORCH] No Envista fallback data for {}", pollutant)
            except Exception as e:
                logger.warning("[ORCH] Envista fallback failed for {}: {}", pollutant, e)

    # Step 3: Transform Layer (with AQS/Envista merge and fallback logic)
    logger.info("[ORCH] Starting hourly transform with fallback logic for year {}", year)
    try:
        transform_results = convert_year_multi(pollutants, year)
        successful_transforms = [p for p in transform_results.values() if p is not None]
        logger.success("[ORCH] Transform completed: {} successful, {} failed", 
                      len(successful_transforms), len(pollutants) - len(successful_transforms))
    except Exception as e:
        logger.error("[ORCH] Transform failed: {}", e)
        raise

    # Step 4: Daily AQI Transform
    logger.info("[ORCH] Starting daily AQI transform for year {}", year)
    try:
        aqi_results = batch_calc_year(pollutants, year)
        successful_aqi = [p for p in aqi_results.values() if p is not None]
        logger.success("[ORCH] Daily AQI completed: {} successful, {} failed", 
                      len(successful_aqi), len(pollutants) - len(successful_aqi))
    except Exception as e:
        logger.error("[ORCH] Daily AQI transform failed: {}", e)
        raise

    # Step 5: Stage Layer - Fact Tables
    logger.info("[ORCH] Starting stage outputs for year {}", year)
    
    # --- Fact Tables ---
    for pollutant in pollutants:
        if pollutant == "88101":
            pbi_writer.write_pm25_hourly(year)
        elif pollutant == "81102":
            pbi_writer.write_pm10_hourly(year)
        elif pollutant == "44201":
            pbi_writer.write_o3_hourly(year)
        # Write daily AQI for all pollutants
        try:
            pbi_writer.write_aqi_daily(pollutant, year)
        except FileNotFoundError as e:
            logger.warning(f"[ORCH] Skipping AQI daily for {pollutant}: {e}")

    # --- Additional Fact Tables ---
    logger.info("[ORCH] Writing fctAQICategory for year {}", year)
    pbi_writer.write_aqi_category(year)
    
    logger.info("[ORCH] Writing fctOtherPollutantsHourly for year {}", year)
    try:
        pbi_writer.write_other_pollutants_hourly(year)
    except Exception as e:
        logger.warning("[ORCH] fctOtherPollutantsHourly failed: {}", e)
    
    logger.info("[ORCH] Writing fctBCHourly for year {}", year)
    try:
        pbi_writer.write_bc_hourly(year)
    except Exception as e:
        logger.warning("[ORCH] fctBCHourly failed: {}", e)
    
    logger.info("[ORCH] Writing fctWildfireFlags for year {}", year)
    try:
        pbi_writer.write_wildfire_flags(year)
    except Exception as e:
        logger.warning("[ORCH] fctWildfireFlags failed: {}", e)

    # Step 6: Stage Layer - Dimension Tables
    logger.info("[ORCH] Writing dimension tables")
    try:
        dim_writer.write_dim_date([year])
    except Exception as e:
        logger.warning("[ORCH] dimDate failed: {}", e)
    
    try:
        dim_writer.write_dim_sites()
    except Exception as e:
        logger.warning("[ORCH] dimSites failed: {}", e)
    
    try:
        dim_writer.write_dim_pollutant()
    except Exception as e:
        logger.warning("[ORCH] dimPollutant failed: {}", e)
    
    try:
        dim_writer.write_dim_county()
    except Exception as e:
        logger.warning("[ORCH] dimCounty failed: {}", e)
    
    try:
        dim_writer.write_dim_aqi()
    except Exception as e:
        logger.warning("[ORCH] dimAQI failed: {}", e)

    # Step 7: Monitor Coverage
    logger.info("[ORCH] Writing monitor_coverage for year {}", year)
    monitor_coverage.write_monitor_coverage([year])

    logger.success("[ORCH] Full pipeline complete for year {} with {} pollutants", year, len(pollutants))
    
    # Summary
    logger.info("[ORCH] Pipeline Summary:")
    logger.info("[ORCH] - AQS primary data: ingested")
    if args.envista_fallback:
        logger.info("[ORCH] - Envista fallback: enabled")
    logger.info("[ORCH] - Transform: {} pollutants processed", len(successful_transforms))
    logger.info("[ORCH] - Daily AQI: {} pollutants processed", len(successful_aqi))
    logger.info("[ORCH] - Stage layer: all fact and dimension tables generated")
    logger.info("[ORCH] - Monitor coverage: generated")


if __name__ == "__main__":
    main() 