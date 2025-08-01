
# Functions to support EPA API calls
library(httr)
library(jsonlite)

########################################################################################################################
# Fetch data from AQS API and parse JSON to a dataframe
########################################################################################################################

fetchAqsResponse <- function(api_url) {
  response <- GET(api_url)
  json_parsed <- content(response, as = "text")
  data_out <- as.data.frame(fromJSON(json_parsed)[[2]], stringsAsFactors = FALSE)
  return(data_out)
}

########################################################################################################################
# Build AQS monitor metadata request URL

getAqsMonitors <- function(state_code, parameter_codes, start_date, end_date, aqs_credentials) {
  paste0("https://aqs.epa.gov/data/api/monitors/byState?",
         "email=",  aqs_credentials$email,
         "&key=",   aqs_credentials$api_key,
         "&param=", parameter_codes,
         "&bdate=", start_date,
         "&edate=", end_date,
         "&state=", state_code)
}

########################################################################################################################
# Construct URLs for AQS data download
########################################################################################################################

buildAqsRequests <- function(state_code, county_code, site_number, parameter_code, start_date, end_date, aqs_credentials) {
  api_requests <- list()
  print(start_date)
  all_years    <- year(start_date):year(end_date)
  
  start_date <- as.Date(start_date, format = "%Y/%m/%d")
  end_date   <- as.Date(end_date, format = "%Y/%m/%d")
  
  for (i in seq_along(all_years)) {
    year_val <- all_years[i]
    request_start <- if (i == 1) format(start_date, "%Y%m%d") else paste0(year_val, "0101")
    request_end   <- if (i == length(all_years)) format(end_date, "%Y%m%d") else paste0(year_val, "1231")
    
    request_url <- paste0(
      "https://aqs.epa.gov/data/api/sampleData/bySite?",
      "email=",   aqs_credentials$email,
      "&key=",    aqs_credentials$api_key,
      "&param=",  parameter_code,
      "&bdate=",  request_start,
      "&edate=",  request_end,
      "&state=",  state_code,
      "&county=", county_code,
      "&site=",   site_number
    )
    
    api_requests[[i]] <- request_url
  }
  
  return(api_requests)
}

########################################################################################################################
# Fetch AQS data for a specific site
########################################################################################################################

getAqsDataBySite <- function(site_pollutant_meta, site_index) {
  state_code     <- substr(site_pollutant_meta$meta$epa_id[site_index], 1, 2)
  county_code    <- substr(site_pollutant_meta$meta$epa_id[site_index], 3, 5)
  site_number    <- substr(site_pollutant_meta$meta$epa_id[site_index], 6, 9)
  
  parameter_code <- site_pollutant_meta$meta$aqs_name[site_index]
  start_date     <- site_pollutant_meta$meta$from_date[site_index]
  end_date       <- site_pollutant_meta$meta$to_date[site_index]

  api_requests   <- buildAqsRequests(state_code, county_code, site_number, parameter_code, start_date, end_date, signin_aqs)
  
  for (request_url in api_requests) {
    print(request_url)
    if (!exists("aqs_data")) {
      aqs_data <- fetchAqsResponse(request_url)
    } else {
      aqs_data <- bind_rows(aqs_data, fetchAqsResponse(request_url))
    }
  }
  
  aqs_data$parameter        <- site_pollutant_meta$meta$envista_name[site_index]
  aqs_data$requested_method <- site_pollutant_meta$meta$aqs_method_code[site_index]
  aqs_data$site             <- site_pollutant_meta$meta$site[site_index]
  
  return(aqs_data)
}

########################################################################################################################
# standardizes AQS API or CSV data into a consistent format to merge with AQS data.
########################################################################################################################
#Neda's edits:
# ✔️ Cleans and standardizes sample_frequency
# 
# ✔️ Handles hourly, daily, and every Nth day frequencies via datetime
# 
# ✔️ Adds data_source and parses qualifier into simple_code and simple_qual
# 
# ✔️ Maps QA codes using cross_tables$qualifier_codes
# 
# ✔️ Includes unit conversions for ozone and wind speed
# 
# ✔️ Applies method filtering correctly
# 
# ✔️ Cleans and casts all core fields at the end
# 
# ✔️ Returns a clean, ready-to-merge dataframe

#' Standardize AQS Data
#' This function takes raw AQS data and formats it into a standardized structure,
#' handling sampling frequencies, qualifiers, units, and method filters.
standardizeAqsData <- function(aqs_data) {
  # Clean and normalize frequency column
  aqs_data$sample_frequency <- tolower(trimws(aqs_data$sample_frequency))
  
  # Detect and print sample frequencies
  freqs <- unique(aqs_data$sample_frequency)
  message("Sample Frequencies Detected: ", paste(freqs, collapse = ", "))
  
  # Tag rows that are hourly-like
  aqs_data <- aqs_data %>%
    mutate(is_hourly_like = sample_frequency %in% c("hourly", "daily: 24 - 1 hr samples -pams"))
  
  # Create datetime and date columns appropriately
  aqs_data <- aqs_data %>%
    mutate(
      datetime = case_when(
        is_hourly_like & !is.na(time_local) ~ as.POSIXct(paste(date_local, time_local), format = "%Y-%m-%d %H:%M", tz = "Etc/GMT+8"),
        grepl("every", sample_frequency) | grepl("daily", sample_frequency) ~ as.POSIXct(date_local, format = "%Y-%m-%d", tz = "Etc/GMT+8"),
        TRUE ~ as.POSIXct(date_local, format = "%Y-%m-%d", tz = "Etc/GMT+8")
        ))
  
  aqs_data$simple_code <- sapply(aqs_data$qualifier, function(x) {
    if (!is.na(x)) strsplit(x, " ")[[1]][1] else NA
  })
  
  aqs_data$simple_qual <- aqs_data$simple_code
  aqs_data$simple_qual[is.na(aqs_data$qualifier)] <- 'ok'
  
  # Optional mapping using qualifier codes
  if (exists("cross_tables") && "qualifier_codes" %in% names(cross_tables)) {
    qual_map <- cross_tables$qualifier_codes %>% distinct(aqs_qualifier, .keep_all = TRUE)
    for (flag in unique(na.omit(aqs_data$simple_code))) {
      mapped <- qual_map %>% filter(aqs_qualifier == flag) %>% pull(simple_qualifier)
      if (length(mapped) > 0) {
        aqs_data$simple_qual[aqs_data$simple_code == flag] <- mapped
      }
    }
  }
  
  # Unit conversions
  is_o3 <- tolower(aqs_data$parameter) %in% c("ozone", "o3")
  ppm_check <- is_o3 & aqs_data$units_of_measure == "Parts per million"
  aqs_data$sample_measurement[ppm_check] <- 1000 * aqs_data$sample_measurement[ppm_check]
  aqs_data$units_of_measure[ppm_check] <- "Parts per billion"
  
  is_ws <- tolower(aqs_data$parameter) %in% c("wind speed", "ws")
  knots_check <- is_ws & aqs_data$units_of_measure == "Knots"
  aqs_data$sample_measurement[knots_check] <- 1.15078 * aqs_data$sample_measurement[knots_check]
  aqs_data$units_of_measure[knots_check] <- "Miles per hour"
  
  # Convert method fields only for hourly-like records
  aqs_data <- aqs_data %>%
    mutate(
      method_code = ifelse(is_hourly_like, as.numeric(method_code), -9999),
      requested_method = ifelse(is_hourly_like, as.numeric(requested_method), -9999),
      method_type = as.character(method_type)
    )
  
  # Filter based on requested_method, but only for hourly-like
  if (any(aqs_data$is_hourly_like, na.rm = TRUE)) {
    if (any(aqs_data$method_code == aqs_data$requested_method, na.rm = TRUE)) {
      aqs_data <- aqs_data %>%
        filter((requested_method == method_code) | (requested_method == -9999) | is.na(requested_method))
    } else {
      message("No matching method_code found — skipping requested_method filter")
    }
  }
  
  aqs_data$data_source <- 'aqs'
  
  # Drop method_type first if already exists
  aqs_data <- aqs_data %>% select(-method_type, everything())
  
  # Now mutate safely
  aqs_data <- aqs_data %>% mutate(
    datetime           = datetime,
    sample_measurement = as.numeric(sample_measurement),
    units_of_measure   = as.character(units_of_measure),
    qualifier          = as.character(qualifier),
    simple_qual        = as.character(simple_qual),
    data_source        = as.character(data_source),
    method_code        = ifelse(is_hourly_like, as.numeric(method_code), -9999),
    method_type        = as.character(parameter),  
    poc                = as.numeric(poc),
    site               = as.character(site),
    latitude           = as.numeric(latitude),
    longitude          = as.numeric(longitude),
    sample_frequency   = as.character(sample_frequency)
  ) %>%
    select(datetime, sample_measurement, units_of_measure, qualifier, simple_qual,
           data_source, method_code, method_type, poc, site,
           latitude, longitude, sample_frequency)
  
  # Return both data and frequency list
  return(list(
    data      = aqs_data,
    frequency = freqs
  ))
}
