"""Test stage layer with real data and file I/O.

This test verifies that the stage layer correctly:
1. Reads from transform outputs
2. Writes zipped CSV files for Power BI
3. Includes data source flagging
4. Handles all implemented fact and dimension tables
"""

import pytest
from pathlib import Path
import pandas as pd
import tempfile
from unittest.mock import patch

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from etl.stage import pbi_writer, dim_writer, monitor_coverage


class TestStageLayerReal:
    """Test stage layer with real data and file I/O."""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary data directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create transform structure
            transform_dir = temp_path / "data" / "transform"
            hourly_dir = transform_dir / "hourly"
            daily_dir = transform_dir / "daily_aqi" / "synchronous"
            
            # Create stage directory
            stage_dir = temp_path / "data" / "stage"
            
            # Create raw directory for dimension tables
            raw_dir = temp_path / "data" / "raw"
            
            for dir_path in [hourly_dir, daily_dir, stage_dir, raw_dir]:
                dir_path.mkdir(parents=True, exist_ok=True)
            
            yield temp_path
    
    def test_write_pm25_hourly_real(self, temp_data_dir):
        """Test PM2.5 hourly fact table with real data."""
        # Create test hourly data
        hourly_dir = temp_data_dir / "data" / "transform" / "hourly" / "88101" / "2024"
        hourly_dir.mkdir(parents=True, exist_ok=True)
        
        test_data = pd.DataFrame({
            "state_code": ["37", "37", "41"],
            "county_code": ["183", "183", "051"],
            "site_number": ["0014", "0014", "0003"],
            "parameter_code": ["88101", "88101", "88101"],
            "date_local": ["2024-03-01", "2024-03-01", "2024-03-01"],
            "time_local": ["01:00", "02:00", "01:00"],
            "sample_measurement": [12.5, 15.2, 8.7],
            "data_source": ["AQS", "AQS", "AQS"]
        })
        
        test_file = hourly_dir / "test.parquet"
        test_data.to_parquet(test_file, index=False)
        
        # Mock the transform path
        with patch.object(pbi_writer, 'TFM_HOURLY', temp_data_dir / "data" / "transform" / "hourly"):
            with patch.object(pbi_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
                result = pbi_writer.write_pm25_hourly(2024)
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) == 3
        assert "data_source" in df.columns
        assert all(df["data_source"] == "AQS")
        assert "sample_measurement" in df.columns
    
    def test_write_aqi_daily_real(self, temp_data_dir):
        """Test daily AQI fact table with real data."""
        # Create test daily AQI data
        daily_dir = temp_data_dir / "data" / "transform" / "daily_aqi" / "synchronous" / "88101" / "2024"
        daily_dir.mkdir(parents=True, exist_ok=True)
        
        test_data = pd.DataFrame({
            "state_code": ["37", "37"],
            "county_code": ["183", "183"],
            "site_number": ["0014", "0014"],
            "date": ["2024-03-01", "2024-03-02"],
            "aqi": [45, 67],
            "conc_avg": [12.5, 18.2],
            "data_source": ["AQS", "AQS"]
        })
        
        test_file = daily_dir / "daily_aqi.parquet"
        test_data.to_parquet(test_file, index=False)
        
        # Mock the transform path
        with patch.object(pbi_writer, 'TFM_DAILY', temp_data_dir / "data" / "transform" / "daily_aqi" / "synchronous"):
            with patch.object(pbi_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
                result = pbi_writer.write_aqi_daily("88101", 2024)
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) == 2
        assert "data_source" in df.columns
        assert "aqi" in df.columns
        assert "conc_avg" in df.columns
    
    def test_write_aqi_category_real(self, temp_data_dir):
        """Test AQI category fact table with real data."""
        # Create test daily AQI data for multiple pollutants
        for pollutant in ["88101", "44201"]:
            daily_dir = temp_data_dir / "data" / "transform" / "daily_aqi" / "synchronous" / pollutant / "2024"
            daily_dir.mkdir(parents=True, exist_ok=True)
            
            test_data = pd.DataFrame({
                "state_code": ["37", "37", "37"],
                "county_code": ["183", "183", "183"],
                "site_number": ["0014", "0014", "0014"],
                "date": ["2024-03-01", "2024-03-02", "2024-03-03"],
                "aqi": [45, 67, 120],  # Good, Moderate, Unhealthy for Sensitive Groups
                "conc_avg": [12.5, 18.2, 35.8],
                "data_source": ["AQS", "AQS", "AQS"]
            })
            
            test_file = daily_dir / "daily_aqi.parquet"
            test_data.to_parquet(test_file, index=False)
        
        # Mock the transform path
        with patch.object(pbi_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = pbi_writer.write_aqi_category(2024)
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) > 0
        assert "aqi_category" in df.columns
        assert "days" in df.columns
        assert "pollutant" in df.columns
        
        # Check that we have different categories
        categories = df["aqi_category"].unique()
        assert len(categories) >= 2  # Should have at least Good and Moderate
    
    def test_write_other_pollutants_hourly_real(self, temp_data_dir):
        """Test other pollutants hourly fact table with real data."""
        # Create test hourly data for non-PM2.5/PM10/O3 pollutants
        for pollutant in ["42101", "42401"]:  # CO, SO2
            hourly_dir = temp_data_dir / "data" / "transform" / "hourly" / pollutant / "2024"
            hourly_dir.mkdir(parents=True, exist_ok=True)
            
            test_data = pd.DataFrame({
                "state_code": ["37"],
                "county_code": ["183"],
                "site_number": ["0014"],
                "parameter_code": [pollutant],
                "date_local": ["2024-03-01"],
                "time_local": ["01:00"],
                "sample_measurement": [0.5],
                "data_source": ["AQS"]
            })
            
            test_file = hourly_dir / "hourly.parquet"
            test_data.to_parquet(test_file, index=False)
        
        # Mock the transform path
        with patch.object(pbi_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = pbi_writer.write_other_pollutants_hourly(2024)
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) == 2  # One row per pollutant
        assert "pollutant" in df.columns
        assert all(df["pollutant"].isin(["42101", "42401"]))
    
    def test_write_dim_date_real(self, temp_data_dir):
        """Test dimension date table with real data."""
        # Mock the stage path
        with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = dim_writer.write_dim_date([2024])
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) == 366  # 2024 is a leap year
        assert "date" in df.columns
        assert "year" in df.columns
        assert "quarter" in df.columns
        assert "season" in df.columns
        assert "day_of_week" in df.columns
        assert "is_weekend" in df.columns
        
        # Check some specific values
        assert all(df["year"] == 2024)
        assert "Winter" in df["season"].values
        assert "Spring" in df["season"].values
        assert "Summer" in df["season"].values
        assert "Fall" in df["season"].values
    
    def test_write_dim_pollutant_real(self, temp_data_dir):
        """Test dimension pollutant table with real data."""
        # Mock the stage path
        with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = dim_writer.write_dim_pollutant()
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) >= 5  # Should have at least PM2.5, PM10, O3, CO, SO2
        assert "parameter_code" in df.columns
        assert "parameter_name" in df.columns
        assert "units" in df.columns
        
        # Check specific pollutants
        codes = df["parameter_code"].values
        assert "88101" in codes  # PM2.5
        assert "81102" in codes  # PM10
        assert "44201" in codes  # O3
    
    def test_write_dim_aqi_real(self, temp_data_dir):
        """Test dimension AQI table with real data."""
        # Mock the stage path
        with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = dim_writer.write_dim_aqi()
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) >= 15  # Should have multiple breakpoints per pollutant
        assert "pollutant" in df.columns
        assert "aqi_low" in df.columns
        assert "aqi_high" in df.columns
        assert "conc_low" in df.columns
        assert "conc_high" in df.columns
        assert "category" in df.columns
        assert "color" in df.columns
        
        # Check specific pollutants
        pollutants = df["pollutant"].unique()
        assert "88101" in pollutants  # PM2.5
        assert "81102" in pollutants  # PM10
        assert "44201" in pollutants  # O3
    
    def test_write_monitor_coverage_real(self, temp_data_dir):
        """Test monitor coverage file with real data."""
        # Create test hourly data
        for pollutant in ["88101", "44201"]:
            hourly_dir = temp_data_dir / "data" / "transform" / "hourly" / pollutant / "2024"
            hourly_dir.mkdir(parents=True, exist_ok=True)
            
            test_data = pd.DataFrame({
                "state_code": ["37", "37"],
                "county_code": ["183", "183"],
                "site_number": ["0014", "0014"],
                "date": ["2024-03-01", "2024-03-02"],
                "parameter_code": [pollutant, pollutant],
                "date_local": ["2024-03-01", "2024-03-02"],
                "time_local": ["01:00", "01:00"],
                "sample_measurement": [12.5, 15.2],
                "data_source": ["AQS", "AQS"]
            })
            
            test_file = hourly_dir / "hourly.parquet"
            test_data.to_parquet(test_file, index=False)
        
        # Mock the transform path
        with patch.object(monitor_coverage, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = monitor_coverage.write_monitor_coverage([2024])
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) >= 4  # Should have site-date combinations
        assert "pollutant" in df.columns
        assert "site_number" in df.columns
        assert "state_code" in df.columns
        assert "county_code" in df.columns
        assert "date" in df.columns
        assert "available" in df.columns
        
        # Check that we have data for both pollutants
        pollutants = df["pollutant"].unique()
        assert "88101" in pollutants
        assert "44201" in pollutants
    
    def test_write_dim_sites_real(self, temp_data_dir):
        """Test dimension sites table with real data."""
        # Create test raw data
        raw_dir = temp_data_dir / "data" / "raw"
        for pollutant in ["88101", "44201"]:
            pollutant_dir = raw_dir / pollutant
            pollutant_dir.mkdir(parents=True, exist_ok=True)
            
            test_data = pd.DataFrame({
                "state_code": ["37", "37"],
                "county_code": ["183", "183"],
                "site_number": ["0014", "0014"],
                "latitude": [35.2271, 35.2271],
                "longitude": [-80.8431, -80.8431],
                "parameter_code": [pollutant, pollutant],
                "date_local": ["2024-03-01", "2024-03-02"],
                "sample_measurement": [12.5, 15.2]
            })
            
            test_file = pollutant_dir / "test.csv.zip"
            test_data.to_csv(test_file, index=False, compression="zip")
        
        # Mock the raw path
        with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = dim_writer.write_dim_sites()
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) >= 2  # Should have site data
        assert "state_code" in df.columns
        assert "county_code" in df.columns
        assert "site_number" in df.columns
        assert "latitude" in df.columns
        assert "longitude" in df.columns
        assert "pollutant" in df.columns
    
    def test_write_dim_county_real(self, temp_data_dir):
        """Test dimension county table with real data."""
        # Create test raw data
        raw_dir = temp_data_dir / "data" / "raw"
        for pollutant in ["88101", "44201"]:
            pollutant_dir = raw_dir / pollutant
            pollutant_dir.mkdir(parents=True, exist_ok=True)
            
            test_data = pd.DataFrame({
                "state_code": ["37", "37"],
                "county_code": ["183", "183"],
                "state_name": ["North Carolina", "North Carolina"],
                "county_name": ["Mecklenburg", "Mecklenburg"],
                "site_number": ["0014", "0014"],
                "parameter_code": [pollutant, pollutant],
                "date_local": ["2024-03-01", "2024-03-02"],
                "sample_measurement": [12.5, 15.2]
            })
            
            test_file = pollutant_dir / "test.csv.zip"
            test_data.to_csv(test_file, index=False, compression="zip")
        
        # Mock the raw path
        with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
            result = dim_writer.write_dim_county()
        
        assert result.exists()
        assert result.suffix == ".zip"
        
        # Verify content
        df = pd.read_csv(result, compression="zip")
        assert len(df) >= 1  # Should have county data
        assert "state_code" in df.columns
        assert "county_code" in df.columns
        assert "state_name" in df.columns
        assert "county_name" in df.columns
    
    def test_stage_layer_requirements_compliance(self, temp_data_dir):
        """Test that stage layer complies with client requirements."""
        # Test that all outputs are zipped CSVs
        test_functions = [
            (pbi_writer.write_pm25_hourly, [2024]),
            (pbi_writer.write_aqi_daily, ["88101", 2024]),
            (dim_writer.write_dim_date, [[2024]]),
            (dim_writer.write_dim_pollutant, []),
            (dim_writer.write_dim_aqi, []),
        ]
        
        for func, args in test_functions:
            # Mock paths
            with patch.object(pbi_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
                with patch.object(dim_writer, 'STG_ROOT', temp_data_dir / "data" / "stage"):
                    with patch.object(pbi_writer, 'TFM_HOURLY', temp_data_dir / "data" / "transform" / "hourly"):
                        with patch.object(pbi_writer, 'TFM_DAILY', temp_data_dir / "data" / "transform" / "daily_aqi" / "synchronous"):
                            # Create minimal test data
                            if "hourly" in str(func):
                                hourly_dir = temp_data_dir / "data" / "transform" / "hourly" / "88101" / "2024"
                                hourly_dir.mkdir(parents=True, exist_ok=True)
                                test_data = pd.DataFrame({
                                    "state_code": ["37"],
                                    "county_code": ["183"],
                                    "site_number": ["0014"],
                                    "parameter_code": ["88101"],
                                    "date_local": ["2024-03-01"],
                                    "time_local": ["01:00"],
                                    "sample_measurement": [12.5],
                                    "data_source": ["AQS"]
                                })
                                test_data.to_parquet(hourly_dir / "test.parquet", index=False)
                            
                            if "daily" in str(func):
                                daily_dir = temp_data_dir / "data" / "transform" / "daily_aqi" / "synchronous" / "88101" / "2024"
                                daily_dir.mkdir(parents=True, exist_ok=True)
                                test_data = pd.DataFrame({
                                    "state_code": ["37"],
                                    "county_code": ["183"],
                                    "site_number": ["0014"],
                                    "date": ["2024-03-01"],
                                    "aqi": [45],
                                    "conc_avg": [12.5],
                                    "data_source": ["AQS"]
                                })
                                test_data.to_parquet(daily_dir / "daily_aqi.parquet", index=False)
                            
                            result = func(*args)
                            
                            # Verify zipped CSV output
                            assert result.exists()
                            assert result.suffix == ".zip"
                            
                            # Verify readable content
                            df = pd.read_csv(result, compression="zip")
                            assert len(df) > 0
                            assert isinstance(df, pd.DataFrame) 