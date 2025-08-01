
########################################################################################################################
# Fetch Air Quality Data (Envista & AQS)
########################################################################################################################

fetchAirQualityDataFromAPI <- function(site = 'none', 
                                       poll_name = 'none', 
                                       from_date = '01/01/01', 
                                       to_date = '01/01/01', 
                                       by_date = 'hour', 
                                       tz_date = 'Etc/GMT+8',
                                       analysis_level = 'not_provided',
                                       monitor_table = 'none') {
  
  if (site[1] == 'none') stop('ERROR: set site with 3 letter code')
  if (poll_name == 'none') stop('ERROR: set pollutant type: pm25, ozone, nox, sensor, etc')
  if (to_date == '01/01/01') stop('ERROR: set time range')
  if (monitor_table[1, 1] == 'none') stop('ERROR: build envista monitor list before doing query. See: getEnvistaStations()')
  
  site         <- tolower(site)
  poll_name    <- tolower(poll_name)
  by_date      <- tolower(by_date)
  
  meta         <- initMetadata(site, poll_name, from_date, to_date, by_date, monitor_table)
  timeseries   <- initTimeseries(from_date, to_date, poll_name, by_date, tz_date)
  session_info <- initSessionInfo(analysis_level)
  
  result        <- list(meta = meta, data = timeseries, info = session_info)
  return(result)
}

########################################################################################################################
#initialize meta data table
########################################################################################################################

initMetadata <- function(site_list, parameter_name, from_date, to_date, interval, monitor_table) {
  meta <- data.frame(matrix(ncol = 15, nrow = length(site_list)))
  names(meta) <- c('site', 'epa_id', 'latitude', 'longitude', 'parameter_name',
                   'envista_name', 'aqs_name', 'from_date', 'to_date', 'interval',
                   'units_envista','envista_method_code', 
                   'aqs_method_code', 'channel_id', 'station_id')

  parameter_name <- tolower(parameter_name)
  
  for (i_site in seq_along(site_list)) {
    current_site <- site_list[i_site]
    row_match <- monitor_table %>% filter(site == current_site)
    
    try({
      meta$site[i_site]                 <- current_site
      meta$parameter_name[i_site]       <- parameter_name
      meta$epa_id[i_site]               <- row_match$stations_tag
      meta$latitude[i_site]             <- row_match$latitude
      meta$longitude[i_site]            <- row_match$longitude
      meta$channel_id[i_site]           <- row_match$channel_id
      meta$station_id[i_site]           <- row_match$station_id
      meta$envista_name[i_site]         <- tolower(cross_tables$analyte_codes$envista_name[tolower(cross_tables$analyte_codes$name) == parameter_name])
      meta$aqs_name[i_site]             <- tolower(cross_tables$analyte_codes$aqs_name[tolower(cross_tables$analyte_codes$name) == parameter_name])
      # meta$alias_name[i_site]         <- tolower(cross_tables$analyte_codes$alias[tolower(cross_tables$analyte_codes$name) == parameter_name])
      meta$envista_method_code[i_site]  <- cross_tables$analyte_codes$method_code_filter[tolower(cross_tables$analyte_codes$name) == parameter_name]
      meta$aqs_method_code[i_site]      <- row_match$last_method_code
      meta$from_date[i_site]            <- from_date
      meta$to_date[i_site]              <- to_date
      meta$interval[i_site]             <- interval
      meta$units_envista[i_site]        <- row_match$units
    }, silent = TRUE)
  }
  return(meta)
}

########################################################################################################################
#
########################################################################################################################

initTimeseries <- function(from, to, parameter, interval, timezone) {
  interval_map <- c("five_min" = "5 min", "hour" = "hour", "day" = "day")
  interval_resolved <- ifelse(interval %in% names(interval_map), interval_map[[interval]], interval)
  
  datetime_seq <- seq(
    from = as.POSIXct(from, tz = timezone, tryFormats = c("%Y/%m/%d %H:%M", "%Y/%m/%d")),
    to = as.POSIXct(to, tz = timezone, tryFormats = c("%Y/%m/%d %H:%M", "%Y/%m/%d")),
    by = interval_resolved
  )
  
  return(data.frame(datetime = datetime_seq))
}

########################################################################################################################
#
########################################################################################################################

initSessionInfo <- function(analysis_level) {
  list(
    odeq = 'This data was compiled by the Oregon Department of Environmental Quality',
    processed_on = paste0('Processed on: ', Sys.time()),
    status = paste0('Data status: ', analysis_level),
    system_header = '_____________ system & session _____________',
    system_info = Sys.info(),
    session_info = sessionInfo(),
    footer = '_____________ end _____________'
  )
}