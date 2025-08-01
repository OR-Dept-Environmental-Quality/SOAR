library(dplyr)
library(lubridate)
library(openxlsx)
library(zoo)
library(purrr)
library(jsonlite)
library(svDialogs)  # For GUI input dialogs
library(RAQSAPI)
library(tidyr)

`%!in%`        <- Negate(`%in%`)

# Function to securely load credentials from JSON
loadCredentials <- function(credentials_file = "credentials.json") {
  fromJSON(credentials_file)
}
# Load all credentials
credentials <- loadCredentials()
# Access AQS credentials separately
signin_aqs <- credentials$AQS
# Extract Envista credentials
signin_envista <- credentials$Envista

# Load all R scripts from the "data_processing" directory
data_processing_scripts <- list.files(file.path(".", "src", "r", "data_processing"), pattern = "\\.R$", full.names = TRUE)
sapply(data_processing_scripts, source, simplify = FALSE)

# Load all R scripts from the "data_ingestion" directory
data_ingestion_scripts <- list.files(file.path(".", "src", "r", "data_ingestion"), pattern = "\\.R$", full.names = TRUE)
sapply(data_ingestion_scripts, source, simplify = FALSE)

# Load utility functions
utility_scripts <- list.files(file.path(".", "src", "r", "utils"), pattern = "\\.R$", full.names = TRUE) # nolint
sapply(utility_scripts, source, simplify = FALSE)

# Extract Oregon API base URL (if needed)
base_url <- credentials$OregonAPI$baseurl

aqs_credentials(credentials$AQS$email, credentials$AQS$api_key)

### Load Metadata and Supporting Scripts ###
# Load all R scripts from the "data_processing" directory

# Set root path using the function

# If you are in RStudio → use selectDirectory() popup.
root_path <- getRootPath()
message("Root path set to: ", root_path)

# If not → use default_path automatically.
# root_path <-  "C:/Users/X/OneDrive - Washington State University (email.wsu.edu)/Documents/VScode Projects/RProjects/SOAR_V1"

#***********************************************************************************************************************
# Start working with metadata
## Make a table of Envista metadata
# (iii) Pre-loading Envista 

criteria_pollutants_list <- c(
  "nitric oxide", "nitrogen dioxide", "oxides of nitrogen", "carbon monoxide", "ozone",
  "pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor", "nephelometer", 
  "sulfur dioxide", "pm10(s)"
)

meteorological_data_list <- c(
  "wind direction", "wind speed", "ambient temperature", 
  "solar radiation", "barometric pressure"
)

seasonal_pollutants <- c("ozone")

# get envista meta table
envista_meta_table <- getEnvistaStations() %>%
  buildEnvistaMetadata() %>% 
  unpackLatLong()

colnames(envista_meta_table) <- tolower(colnames(envista_meta_table))

envista_meta_table <- envista_meta_table %>%
  mutate(across(where(is.list), ~ sapply(., unlist)))

envista_meta_table <- envista_meta_table %>%
  mutate(across(where(is.list), ~ sapply(., toString)))

# Update alias_type without altering monitor_type
# Update alias_type without altering monitor_type
envista_meta_table <- envista_meta_table %>%
  mutate(
    alias_type = if_else(
      monitor_type %in% c("sensor a pm2.5est", "sensor b pm2.5est"),
      "pm2.5 est sensor",
      alias_type
    ))

# # Export Envista metadata 
# Will uncomment for midnight to midnight run of the script
# write.csv(as.data.frame(aqm_monitors_envista), 
#           file = paste0(root_path, "Additional_Data/EnvistaMeta_aqm_monitors_envista.csv", row.names = FALSE))

# Ensure alias_type and webname are correctly assigned
envista_meta_table <- envista_meta_table %>%
  mutate(
    alias_type = tolower(alias_type),  # Ensure alias_type is in lowercase
    webname = tolower(coalesce(alias_type, monitor_type))  # Use monitor_type if alias_type is NA
  )

# Extract unique parameter names
parameter_list <- envista_meta_table %>%
  select(alias_type) %>%
  distinct() %>%
  filter(!is.na(alias_type))

# Load site information from manually maintained Excel file
cross_tables <- loadCrossTables("./reference_data")

# End of working with metadata

### ********************************************************************************************************************
#*# list of modules starts from here 
# Start API requests to retrieve HOURLY data from AQS and Envista
#make a table of Envista meta data
# Get user input
# Get user input
user_input <- getExportPreferences()

# --- Ensure PM2.5 is prioritized before Ozone in grouped_parameters ---
if (all(c("pm2.5", "ozone") %in% user_input$grouped_parameters)) {
  user_input$grouped_parameters <- c(
    "pm2.5",
    setdiff(user_input$grouped_parameters, "pm2.5")
  )
}
# Main execution loop
for (year_to_foc in user_input$selected_years) {
  
  from_date <- user_input$from_date
  to_date   <- user_input$to_date
  
  # Initialize lists if not already
  if (!exists("variant_hourly_list")) variant_hourly_list <- list()
  if (!exists("variant_daily_list"))  variant_daily_list  <- list()
  
  for (poll_variant in user_input$grouped_parameters) {
    
    category <- getParameterCategory(poll_variant)
    
    # ==== Step 1: Load from file if not already in memory ====
    if (!poll_variant %in% names(variant_hourly_list) || is.null(variant_hourly_list[[poll_variant]])) {
      hourly_path <- file.path(root_path, "Hourly", category, "xlsx", poll_variant, as.character(year_to_foc),
                               paste0(poll_variant, "_hourly_", year_to_foc, ".xlsx"))
      if (file.exists(hourly_path)) {
        message("Loading hourly data from file for ", poll_variant)
        variant_hourly_list[[poll_variant]] <- read.xlsx(hourly_path, colNames = TRUE, detectDates = TRUE)
      }
    }
    
    if (!poll_variant %in% names(variant_daily_list) || is.null(variant_daily_list[[poll_variant]])) {
      daily_path <- file.path(root_path, "Daily", category, "xlsx", poll_variant, as.character(year_to_foc),
                              paste0(poll_variant, "_daily_", year_to_foc, ".xlsx"))
      if (file.exists(daily_path)) {
        message("Loading daily data from file for ", poll_variant)
        variant_daily_list[[poll_variant]] <- read.xlsx(daily_path, colNames = TRUE, detectDates = TRUE)
      }
    }
    
    # --- Step 2: If daily is still missing, check and load hourly ---
    # --- Step 2: If daily is still missing, check and load hourly ---
    if ((is.null(variant_daily_list[[poll_variant]]) || nrow(variant_daily_list[[poll_variant]]) == 0) &&
        (!poll_variant %in% names(variant_hourly_list) || is.null(variant_hourly_list[[poll_variant]]))) {
      
      hourly_path <- file.path(root_path, "Hourly", category, "xlsx", poll_variant, as.character(year_to_foc),
                               paste0(poll_variant, "_hourly_", year_to_foc, ".xlsx"))
      
      if (file.exists(hourly_path)) {
        loaded_hourly <- read.xlsx(hourly_path, colNames = TRUE, detectDates = TRUE)
        
        if (!is.null(loaded_hourly) && nrow(loaded_hourly) > 0) {
          variant_hourly_list[[poll_variant]] <- loaded_hourly
          message("Loaded hourly data to derive daily for ", poll_variant)
          
          # Initialize list only if we're going to use it
          all_processed_daily <- list()
          
          daily_from_hourly <- processDailyData(
            data               = variant_hourly_list[[poll_variant]],
            year_to_foc        = year_to_foc,
            root_path          = root_path,
            grouped_parameters = poll_variant,
            monitor_table      = monitor_table
          )
          
          all_processed_daily <- append(all_processed_daily, list(daily_from_hourly))
        } else {
          message("Hourly file exists but is empty for parameter: ", poll_variant)
        }
      } else {
        message("Hourly file not found for parameter: ", poll_variant)
      }
    }
    
    
    # ==== Step 3: Fetch from API if either is missing ====
    if (is.null(variant_hourly_list[[poll_variant]]) || is.null(variant_daily_list[[poll_variant]])) {
      message("Fetching data from source for ", poll_variant)
      
      fetched_data <- fetchAirQualityData(
        from_date, to_date, year_to_foc,
        user_input$grouped_parameters,
        user_input$selected_parameters, poll_variant
      )
      
      monitor_table <- fetched_data$meta
      variant_hourly_list[[poll_variant]] <- fetched_data$hourly_data[[poll_variant]]
      variant_daily_list [[poll_variant]] <- fetched_data$daily_data [[poll_variant]]
      
      # ==== Step 4: Process fetched data ====
      # Temporary list to collect all daily data for this parameter
      all_processed_daily <- list()
      
      # Process from hourly → daily
      if (!is.null(variant_hourly_list[[poll_variant]]) &&
          is.data.frame(variant_hourly_list[[poll_variant]]) &&
          nrow(variant_hourly_list[[poll_variant]]) > 0) {
        
        processed_hourly <- processHourlyData(
          data               = variant_hourly_list[[poll_variant]],
          year_to_foc        = year_to_foc,
          root_path          = root_path,
          grouped_parameters = poll_variant,
          monitor_table      = monitor_table
        )
        
        daily_from_hourly <- processDailyData(
          data               = processed_hourly,
          year_to_foc        = year_to_foc,
          root_path          = root_path,
          grouped_parameters = poll_variant,
          monitor_table      = monitor_table
        )
        
        all_processed_daily <- append(all_processed_daily, list(daily_from_hourly))
      }
      
      # Fallback: Process any raw daily data already in list
      if (!is.null(variant_daily_list[[poll_variant]]) &&
          is.data.frame(variant_daily_list[[poll_variant]]) &&
          nrow(variant_daily_list[[poll_variant]]) > 0) {
        
        daily_from_raw <- processDailyData(
          data               = variant_daily_list[[poll_variant]],
          year_to_foc        = year_to_foc,
          root_path          = root_path,
          grouped_parameters = poll_variant,
          monitor_table      = monitor_table
        )
        
        all_processed_daily <- append(all_processed_daily, list(daily_from_raw))
      }
      
      # Combine all sources and save to variant_daily_list
      if (length(all_processed_daily) > 0) {
        variant_daily_list[[poll_variant]] <- bind_rows(all_processed_daily)
        message("Bound all daily data sources into variant_daily_list[[", poll_variant, "]]")
      }
      
      # ==== Step 5: If PM2.5, calculate hierarchy + wildfire trends ====
      if (poll_variant == "pm2.5" &&
          !is.null(variant_daily_list[[poll_variant]]) &&
          !exists("test_hierarchy", inherits = FALSE)) {
        
        test_hierarchy <- hierarchyPM25Daily(variant_daily_list[[poll_variant]])
        
        save_dir <- file.path(root_path, "Daily", category, "xlsx", poll_variant, as.character(year_to_foc))
        if (!dir.exists(save_dir)) dir.create(save_dir, recursive = TRUE)
        
        # Save hierarchy only if file doesn't already exist
        hierarchy_file <- file.path(save_dir, paste0("pm25_daily_hierarchy_", year_to_foc, ".xlsx"))
        if (!file.exists(hierarchy_file)) {
          tryCatch({
            write.xlsx(test_hierarchy, file = hierarchy_file, rowNames = FALSE)
            message("Saved hierarchy PM2.5 daily file to: ", hierarchy_file)
          }, error = function(e) {
            fallback_file <- file.path(save_dir, paste0("pm25_daily_hierarchy_", year_to_foc, ".csv"))
            write.csv(test_hierarchy, file = fallback_file, row.names = FALSE)
            message("Excel write failed. Saved as CSV to: ", fallback_file)
          })
        } else {
          message("Hierarchy file already exists: ", hierarchy_file)
        }
        
        # Wildfire trends
        if (!exists("wf_trends", inherits = FALSE)) {
          tryCatch({
            wf_trends <- generatePM25WFTrends(test_hierarchy, year_to_foc)
            
            trends_file <- file.path(save_dir, paste0("wf_trends_", year_to_foc, ".xlsx"))
            if (!file.exists(trends_file)) {
              write.xlsx(wf_trends, file = trends_file, rowNames = FALSE)
              message("Saved wildfire PM2.5 trends to: ", trends_file)
            } else {
              message("Wildfire trends file already exists: ", trends_file)
            }
          }, error = function(e) {
            warning("Failed to save wf_trends file: ", e$message)
          })
        }
      }
    }
  }
}