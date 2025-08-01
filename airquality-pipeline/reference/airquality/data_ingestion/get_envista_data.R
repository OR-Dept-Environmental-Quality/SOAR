#For code review
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(stringr)

########################################################################################################################
# Build a complete Envista monitor metadata table
########################################################################################################################

buildEnvistaMetadata <- function(envista_stations) {
  monitor_list       <- lapply(seq_len(nrow(envista_stations)), function(i_site) {
    station_info     <- envista_stations[i_site, ]
    station_meta     <- station_info %>% select(-monitors)
    station_monitors <- station_info$monitors[[1]]
    
    monitor_rows <- lapply(seq_len(nrow(station_monitors)), function(i_monitor) {
      monitor    <- station_monitors[i_monitor, ]
      tibble(
        station_meta,
        channel_id    = monitor$channelId %||% -9999,
        monitor_type  = tolower(monitor$name %||% 'none'),
        alias_type    = monitor$alias_type %||% 'none',
        type_id       = monitor$typeId %||% -9999,
        pollutant_id  = monitor$pollutantId %||% -9999,
        units         = monitor$units %||% 'none'
      )
    })
    bind_rows(monitor_rows)
  })
  monitor_data <- bind_rows(monitor_list)

    # Apply snake_case renaming to match your conventions
  monitor_data <- monitor_data %>%
    rename(
      site           = shortName,
      # station_name = name,
      stations_tag   = stationsTag,
      station_id     = stationId,
      # station_type = type
    ) %>%
    mutate(site = tolower(site))
  
  return(monitor_data)
}

########################################################################################################################
# Get all available regions from the Envista API
########################################################################################################################

getEnvistaRegions <- function() {
  query <- paste0(base_url, 'v1/envista/regions')
  response <- GET(query, authenticate(signin_envista$username, signin_envista$password))
  regions <- fromJSON(content(response, type = "text", encoding = "UTF-8"))
  colnames(regions)[colnames(regions) == 'name'] <- 'region_name'
  return(regions)
}

########################################################################################################################
# Get all stations from the Envista API with associated regions using stored credentials
########################################################################################################################

getEnvistaStations <- function() {
  query <- paste0(base_url, "v1/envista/stations")
  response <- GET(query, authenticate(signin_envista$username, signin_envista$password))
  stations <- fromJSON(content(response, type = "text", encoding = "UTF-8"))
  regions <- getEnvistaRegions()
  stations <- left_join(stations, regions[, c('regionId', 'region_name')], by = 'regionId')
  colnames(stations)[colnames(stations) == 'address'] <- 'census_classifier'
  return(stations)
}

########################################################################################################################
# Get AQI parameter data from a station's channel
########################################################################################################################

getEnvistaDataBySite <- function(site_index, site_pollutant_meta) {
  time_base <- 60
  
  channel_id <- site_pollutant_meta$meta$channel_id[site_index]
  station_id <- site_pollutant_meta$meta$station_id[site_index]
  start_date <- format(as.Date(site_pollutant_meta$meta$from_date[site_index]), "%Y-%m-%d")
  end_date   <- format(as.Date(site_pollutant_meta$meta$to_date[site_index]), "%Y-%m-%d")
  
  query <- paste0(
    base_url, "v1/envista/stations/", station_id, "/data/", channel_id,
    "?from=", start_date, "&to=", end_date, "&timebase=", time_base
  )
  
  print(query)
  
  response <- GET(query, authenticate(signin_envista$username, signin_envista$password))
  
  
  if (status_code(response) != 200 || status_code(response) == 204) {
    warning("Request failed or no content (status ", status_code(response), ")")
    return(NULL)
  }
  
  content_text <- content(response, type = "text", encoding = "UTF-8")
  
  # Short-circuit if empty content
  if (nchar(content_text) == 0) {
    warning("Empty content in Envista response")
    return(NULL)
  }
  
  envista_raw <- fromJSON(content(response, type = "text", encoding = "UTF-8")) %>% parseEnvistaApiResponse()

  envista_raw$site             <-  site_pollutant_meta$meta$site[site_index]
  envista_raw$method_code      <-  site_pollutant_meta$meta$aqs_method_code[site_index]
  envista_raw$parameter        <-  site_pollutant_meta$meta$envista_name[site_index]
  envista_raw$units_of_measure <-  site_pollutant_meta$meta$units_envista[site_index]
  envista_raw$latitude         <-  site_pollutant_meta$meta$latitude[site_index]#taken from aqs meta
  envista_raw$longitude        <-  site_pollutant_meta$meta$longitude[site_index]#taken from aqs meta

  # Handle completely empty response
  if (is.null(envista_raw) || length(envista_raw$datetime) == 0) {
    warning("Empty Envista response received")
    return(NULL)
  }

  return(envista_raw)
}

########################################################################################################################
# Parse Envista API response into a flat data.frame
########################################################################################################################

parseEnvistaApiResponse <- function(envista_response) {
  raw_data <- envista_response$data
  
  if (is.null(envista_response$data) || length(envista_response$data$datetime) == 0) {
    warning("No data found in Envista response")
    return(NULL)
  }
  
  # Check if data is empty
  if (length(raw_data$channels) == 0) {
    warning("Empty Envista response received")
    return(data.frame())  # return empty df to prevent errors
  }
  
  parsed <- t(sapply(seq_along(raw_data$channels), function(i) {
    c(
      raw_data$datetime[[i]],
      raw_data$channels[[i]]$id,
      raw_data$channels[[i]]$name,
      raw_data$channels[[i]]$alias,
      raw_data$channels[[i]]$value,
      raw_data$channels[[i]]$status,
      raw_data$channels[[i]]$valid,
      raw_data$channels[[i]]$description
    )
  }))
  
  df <- as.data.frame(parsed, stringsAsFactors = FALSE)
  names(df) <- c('datetime', 'channel_id', 'name', 'alias', 'value', 'status', 'valid', 'description')
  # then:
  df <- df %>% select(-name, -alias)
  df$datetime <- parse_date_time(
    str_replace_all(df$datetime, c('T' = ' ', '-08:00' = '', '-07:00' = '')),
    '%Y-%m-%d %H:%M:%S', tz = 'Etc/GMT+8'
  )
  df$value <- as.numeric(df$value)
  
  return(df)
}

########################################################################################################################
# Retrieve hourly Envista data for the focus parameter and site
########################################################################################################################

standardizeEnvistaData <- function(envista_data) {
  for (time_resolution in c('five_min', 'hour', 'day')) {
    if (time_resolution == 'five_min') offset <- minutes(5)
    if (time_resolution == 'hour') offset <- hours(1)
    if (time_resolution == 'day') offset <- days(1)
    
    matching_rows <- envista_data$by_date == time_resolution
    envista_data$datetime[matching_rows] <- envista_data$datetime[matching_rows] - offset
  }
  
  envista_data <- envista_data %>% 
    filter(!is.na(datetime)) %>% 
    select(datetime, value, status, valid, parameter, method_code, 
           units_of_measure, latitude, longitude, site)
  
  names(envista_data) <- c('datetime', 'sample_measurement', 'qualifier', 'valid',
                           'parameter', 'method_code', 'units_of_measure', 
                           'latitude', 'longitude', 'site')
  
  envista_data$simple_qual  <- envista_data$qualifier
  envista_data$data_source  <- 'envista'
  envista_data$poc          <- -9999
  
  # Assign sample_frequency based on resolution (if not present)
  if (!"sample_frequency" %in% names(envista_data)) {
    envista_data$sample_frequency <- "hourly"  # Default assumption for Envista
  } else {
    envista_data$sample_frequency <- tolower(trimws(envista_data$sample_frequency))
  }
  
  # Map Envista qualifier codes
  all_flags <- unique(envista_data$qualifier[!is.na(envista_data$qualifier)])
  
  for (flag_code in all_flags) {
    mapped_qualifier <- cross_tables$qualifier_codes %>% 
      filter(envista_qualifier_id == flag_code) %>% 
      pull(simple_qualifier)
    
    envista_data$simple_qual[envista_data$qualifier == flag_code] <- mapped_qualifier
  }

  envista_data$data_source <- 'envista'
  
envista_data <- envista_data %>% mutate(
  datetime           = datetime,
  sample_measurement = as.numeric(sample_measurement),
  units_of_measure   = as.character(units_of_measure),
  qualifier          = as.character(qualifier),
  simple_qual        = as.character(simple_qual),
  data_source        = as.character(data_source),
  method_code        = as.numeric(method_code),
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

  return(envista_data)
}
