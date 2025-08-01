
########################################################################################################################
# add typical time intervals for daily DB
########################################################################################################################

addTimeIntervalsDaily <- function(grab_dat) {
  grab_dat$year <- year(grab_dat$date)
  grab_dat$month <- month(grab_dat$date)
  grab_dat$hour <- hour(grab_dat$date)
  grab_dat$day2foc <- day(grab_dat$date)
  grab_dat$doy <- yday(grab_dat$date)
  grab_dat$week <- epiweek(grab_dat$date)
  grab_dat$weekday_name <- weekdays(grab_dat$date, abbreviate = FALSE)
  grab_dat$weekend <- grab_dat$weekday_name == "Saturday" | grab_dat$weekday_name == "Sunday"
  return(grab_dat)
}

########################################################################################################################
# add typical time intervals for hourly DB
########################################################################################################################

addTimeIntervals <- function(grab_dat) {
  grab_dat$date         <- as.Date(grab_dat$datetime, format = "%Y-%m-%d", tz = 'Etc/GMT+8')
  grab_dat$year         <- year(grab_dat$datetime)
  grab_dat$month        <- month(grab_dat$datetime)
  grab_dat$hour         <- hour(grab_dat$datetime)
  grab_dat$day2foc      <- day(grab_dat$datetime)
  grab_dat$doy          <- yday(grab_dat$datetime)
  grab_dat$week         <- epiweek(grab_dat$datetime)
  grab_dat$weekday_name <- weekdays(grab_dat$datetime, abbreviate = FALSE)
  grab_dat$weekend      <- grab_dat$weekday_name == "Saturday" | grab_dat$weekday_name == "Sunday"
  return(grab_dat)
}

########################################################################################################################
# midnight to midnight average for pm2.5
########################################################################################################################
  
  #   # days with < 18 valid hours are excluded.
  #   # However, we need to include days if there are < 18 observations but the weighted pm25 still exceeds the NAAQS
  #   too_few               <- df_daily$missing_obs < 18 
  #   df_daily$scaled_value <- (df_daily$missing_obs/24) * df_daily$pm25
  #   too_low               <- df_daily$scaled_value < 35 # 35 is the daily dv for pm25
  #   df_daily$scaled_value[too_low] <- NA
  #   
  #   
  #   df_daily$pm25_valid[too_few] <- df_daily$scaled_value[too_few]
  #   df_daily$pm25_valid[is.nan( df_daily$pm25_valid)] <- NA
  #   df_daily$pm25_valid <- trunc( df_daily$pm25_valid * 10,2)/10 
  #   df_daily <- df_daily %>% select(!scaled_value)

calculate24HourAverage <- function(df) {
  df <- df %>% mutate(datetime = force_tz(datetime, "Etc/GMT+8"))  
  df_daily <- df %>%
    mutate(date = floor_date(datetime, "day")) %>%
    group_by(date, site, method_type, poc) %>%
    dplyr::summarize(
      missing_obs = 24 - n(),
      start_time  = format(min(datetime), format = '%Y-%m-%dT%H:%M'),
      end_time    = format(max(datetime), format = '%Y-%m-%dT%H:%M'),
      data_source = if ("data_source" %in% names(df)) first(na.omit(as.character(data_source))) else NA_character_,
      method_type = first(na.omit(method_type)),
      pm25        = if (all(is.na(sample_measurement_best))) NA_real_ else trunc(mean(sample_measurement_best, na.rm = TRUE) * 10, 2) / 10,
      min_pm25    = if (all(is.na(sample_measurement_best))) NA_real_ else trunc(min(sample_measurement_best, na.rm = TRUE) * 10, 2) / 10,
      max_pm25    = if (all(is.na(sample_measurement_best))) NA_real_ else trunc(max(sample_measurement_best, na.rm = TRUE) * 10, 2) / 10,
      site        = first(site),
      latitude    = first(latitude),
      longitude   = first(longitude),
      .groups = "drop"
    )
  return(df_daily)
}

########################################################################################################################
# Corrected Function (EPA-Compliant 8-Hour Ozone Calculation) NK
########################################################################################################################
# Here’s the corrected version of the function that follows EPA's 8-hour ozone max calculation method:
# 
# 🔹 Fixes Applied
# Uses time-beginning logic (7 AM → 7 AM the next day)
# Selects only 17 valid 8-hour periods per day
# Correctly substitutes missing values with 0.000 PPM if necessary
# Finds the max 8-hour ozone per day following truncation rules

# runOver8Hour <- function(Dat, Dat_times, DesignLimit) {
#   
#   # Convert PPB to PPM and truncate to 3 decimal places
#   Dat <- trunc(Dat / 1000 * 10^3) / 10^3 
#   
#   # Identify unique days
#   unique_days <- unique(as.Date(Dat_times))
#   
#   # Store daily max 8-hour ozone values
#   daily_max_8hr <- rep(NA, length(unique_days))  
#   
#   for (i in seq_along(unique_days)) {
#     day <- unique_days[i]
#     
#     # Extract indices for the **17** valid 8-hour periods (7 AM → 7 AM next day)
#     valid_indices <- which(Dat_times >= as.POSIXct(paste(day, "07:00:00")) & 
#                              Dat_times < as.POSIXct(paste(day, "07:00:00")) + lubridate::hours(24))
#     
#     if (length(valid_indices) < 17) next  # Need at least 17 hours to proceed
#     
#     # Track the max 8-hour average
#     max_8hr <- NA  
#     
#     # Compute 8-hour averages over the **17 defined periods** (7 AM → 7 AM)
#     for (j in 1:17) {
#       # Ensure we can get a full 8-hour window starting at j
#       if ((valid_indices[j] + 7) > length(Dat)) break  
#       
#       subDat <- Dat[valid_indices[j]:(valid_indices[j] + 7)]  # Extract 8-hour block
#       missing <- is.na(subDat)
#       
#       # Substitute missing hours with 0.000 PPM if fewer than 6 hours are missing
#       if (sum(missing) > 5) next  
#       subDat[missing] <- 0.000  
#       
#       # Compute 8-hour average and truncate to 3 decimal places
#       avg_8hr <- trunc(mean(subDat, na.rm = TRUE) * 10^3) / 10^3  
#       
#       # Track the maximum 8-hour value for the day
#       if (is.na(max_8hr) || avg_8hr > max_8hr) {
#         max_8hr <- avg_8hr
#       }
#     }
#     
#     # Store the max 8-hour ozone for this day
#     daily_max_8hr[i] <- max_8hr
#   }
#   
#   return(data.frame(Date = unique_days, Max_8hr_Ozone = daily_max_8hr))
# }

########################################################################################################################
#for proper rounding RP
########################################################################################################################

trueRound <- function(number, digits) {
  posneg <- sign(number)
  number <- abs(number) * 10^digits
  number <- number + 0.5 + sqrt(.Machine$double.eps)
  number <- trunc(number)
  number <- number / 10 ^ digits
  return(number * posneg)
}

########################################################################################################################
#Corrected Function (EPA-Compliant 8-Hour Ozone Calculation) RP
########################################################################################################################

runOver8Hour <- function(dat, design_limit) {
  # Ensure the data is numeric
  dat <- as.numeric(dat)
  
  # Initialize output vector with NA
  dat_8_hour <- rep(NA_real_, length(dat))  
  count <- 1
  
  for (index in 1:(length(dat) - 7)) {
    if (count >= 8) {
      sub_dat <- dat[index:(index + 7)]
      
      # Defensive check: skip if sub_dat is all NA or invalid
      if (all(is.na(sub_dat))) {
        dat_8_hour[index] <- NA_real_
      } else {
        val <- calc8hrMaxOzone(sub_dat, design_limit)
        dat_8_hour[index] <- val
      }
      
      if (count != 24) {
        count <- count + 1
      } else {
        count <- 1
      }
    } else {
      dat_8_hour[index] <- NA_real_
      count <- count + 1
    }
  }
  
  return(dat_8_hour)
}

########################################################################################################################
# calculates 8 hr o3 according to EPA guidelines RP
########################################################################################################################

calc8hrMaxOzone <- function(sub_dat, design_limit) {
  sub_dat <- as.numeric(sub_dat)  # Ensure it's numeric
  
  missing_values <- is.na(sub_dat)
  eight_hour_avg <- NA_real_  # Default value if not calculable
  
  if (sum(missing_values) <= 2) {
    eight_hour_avg <- trunc(mean(sub_dat, na.rm = TRUE) * 10^3, 3) / 10^3
    eight_hour_avg <- trueRound(eight_hour_avg, digits = 3)
  } else {
    mdl_value <- 0  # customizable minimum detection limit
    sub_dat[missing_values] <- mdl_value
    eight_hour_avg <- trunc(mean(sub_dat, na.rm = TRUE) * 10^3, 3) / 10^3
    eight_hour_avg <- trueRound(eight_hour_avg, digits = 3)
    
    if (!is.na(eight_hour_avg) && eight_hour_avg < design_limit) {
      eight_hour_avg <- NA_real_
    }
  }
  
  return(eight_hour_avg)
}

########################################################################################################################
# calculates 8 hr o3 according to EPA guidelines RP
########################################################################################################################

calculateDailyMax8hrOzone <- function(hourly_data) {
  hourly_data %>%
    mutate(datetime = force_tz(datetime, "Etc/GMT+8")) %>%
    arrange(site, datetime) %>%
    mutate(date = as.Date(datetime)) %>%
    group_by(site, date, method_type, poc) %>%
    summarize(
      o3 = ifelse(all(is.na(o3_8hr)), NA, max(o3_8hr, na.rm = TRUE)),
      n_obs = sum(!is.na(o3_8hr)),
      site           =    unique(site),
      latitude       =    unique(latitude),
      longitude      =    unique(longitude),
      .groups = "drop"
    )
}

########################################################################################################################
# mops 
########################################################################################################################

quickClean <- function(grab_dat) {
  good_data_filter <- grab_dat$simple_qual_best == "ok"
  grab_dat <- grab_dat[good_data_filter, ]
  grab_dat %>% filter(if_any(everything(), ~ !is.na(.))) -> grab_dat
  grab_dat <- grab_dat[!is.na(grab_dat$datetime), ]
  return(grab_dat)
}

########################################################################################################################
# Function to load or set the root path
########################################################################################################################

getRootPath <- function(config_file = "config.json") {
  # Check if config file exists
  if (file.exists(config_file)) {
    config <- fromJSON(config_file)
    if (!is.null(config$root_path) && dir.exists(config$root_path)) {
      return(config$root_path)  # Use saved path
    }
  }
  
  # Ask user to set the path only if it's missing
  user_path <- dlg_dir(default = ".", title = "Select Root Path")$res
  
  # Validate user input
  if (!dir.exists(user_path)) {
    stop("Invalid directory selected. Please select a valid path.")
  }
  
  # Save the selected path for future runs
  write_json(list(root_path = user_path), config_file, pretty = TRUE)
  
  return(user_path)
}

########################################################################################################################
# Ensure output folders exist
########################################################################################################################

ensureDirectory <- function(file_path) {
  dir_path <- dirname(file_path)
  
  if (!dir.exists(dir_path)) {
    message("Creating directory: ", dir_path)
    
    success <- dir.create(dir_path, recursive = TRUE, showWarnings = TRUE)
    
    if (success) {
      message("Created folder: ", dir_path)
    } else {
      stop("Failed to create folder: ", dir_path)
    }
  } else {
    message(" Folder already exists: ", dir_path)
  }
}

########################################################################################################################
# Return folder category for the parameter
########################################################################################################################

getParameterCategory<- function(parameter) {
  if (parameter %in% c("nitric oxide", "nitrogen dioxide", "oxides of nitrogen", "carbon monoxide", "ozone",
                       "pm2.5", "nephelometer", "sulfur dioxide", "pm10(s)")) {
    return("Criteria_Pollutants")
  } else if (parameter %in% c("wind direction", "wind speed", "ambient temperature", "solar radiation", "barometric pressure")) {
    return("Meteorological_Data")
  } else {
    return("Other")
  }
}

########################################################################################################################
# Define a mapping function for parameter grouping
########################################################################################################################

groupPm25Variants <- function(parameters) {
  pm25_variants <- c("pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor")
  parameters[parameters %in% pm25_variants] <- "pm2.5"
  return(unique(parameters))  # Optional: remove duplicates
}

########################################################################################################################
# Define pm25 tags
########################################################################################################################

getPm25Tag <- function(daily_data) {
  valid_tags <- c("neph", "bam", "sensor")
  
  if (!"method_type" %in% names(daily_data)) {
    warning("'method_type' column not found in daily_data. Returning 'unknown'")
    return("unknown")
  }
  
  found_tags <- unique(tolower(trimws(daily_data$method_type)))
  matched_tags <- intersect(valid_tags, found_tags)
  
  if (length(matched_tags) == 0) {
    warning("No matching PM2.5 tags found. Returning 'unknown'")
    return("unknown")
  }
  
  return(paste(sort(matched_tags), collapse = "_"))
}

########################################################################################################################
# rename column names in Envista metadata
########################################################################################################################

cleanEnvistaMetadata <- function(envista_df) {
  colnames(envista_df) <- tolower(colnames(envista_df))

  return(envista_df)
}

########################################################################################################################
# Add flattened lat/lon fields from nested 'location'
########################################################################################################################
unpackLatLong <- function(envista_data) {
  envista_data$latitude <- envista_data$location$latitude
  envista_data$longitude <- envista_data$location$longitude
  envista_data <- envista_data %>% select(-location)
  return(envista_data)
}

########################################################################################################################
# Simplify metadata for Envista or AQS
########################################################################################################################

standardizeMetadata <- function(metadata, source_type = "envista") {
  if (source_type == "aqs") {
    metadata$name <- metadata$local_site_name
    metadata$short_name <- "not_tracked"
    metadata$station_tag <- with(metadata, 1e7 * as.numeric(state_code) + 1e4 * as.numeric(county_code) + as.numeric(site_number))
    metadata$latitude <- as.numeric(metadata$latitude)
    metadata$longitude <- as.numeric(metadata$longitude)
  }
  return(metadata)
}

########################################################################################################################
# Function to get user preferences for exporting
########################################################################################################################

getExportPreferences <- function(
    criteria_pollutants_list = c("nitric oxide", "nitrogen dioxide", "oxides of nitrogen", "carbon monoxide", "ozone",
                                 "pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor", "nephelometer", "sulfur dioxide", "pm10(s)"),
    meteorological_data_list = c("wind direction", "wind speed", "ambient temperature", "solar radiation", "barometric pressure"),
    seasonal_pollutants = c("ozone")
) {
  ### Safe Date Conversion Function 
  safeConvertDate <- function(date_input, format_in = "%Y-%m-%d", format_out = "%Y/%m/%d") {
    if (is.null(date_input) || date_input == "") {
      stop("Invalid date input. The date cannot be empty.")
    }
    
    converted_date <- as.Date(date_input, format = format_in)
    
    if (is.na(converted_date)) {
      stop("Invalid date format. Please enter a valid date in YYYY-MM-DD format.")
    }
    
    return(format(converted_date, format_out))  # API-ready format
  }
  
  export_type <- dlg_list(c("Yearly", "Monthly", "Custom Date Range"), title = "Select Export Type")$res
  current_year <- as.numeric(format(Sys.Date(), "%Y"))
  
  if (export_type == "Yearly") {
    year_options <- as.character(seq(2000, current_year, by = 1))
    selected_years <- dlg_list(year_options, multiple = TRUE, title = "Select Year(s)")$res  
    
    if (length(selected_years) == 0) stop("At least one year must be selected.")
    if ("2025" %in% selected_years) {
      warning("2025 is set to Monthly mode automatically.")
      selected_years <- setdiff(selected_years, "2025")
    }
    
    from_vector <- as.Date(paste0(selected_years, "-01-01"))
    to_vector   <- as.Date(paste0(selected_years, "-12-31"))
    
  } else if (export_type == "Monthly") {
    selected_months <- dlg_list(month.name, multiple = TRUE, title = "Select Month(s)")$res
    if (length(selected_months) == 0) stop("At least one month must be selected.")
    
    selected_years <- dlg_list(as.character(seq(2000, current_year, by = 1)), multiple = TRUE, title = "Select Year(s)")$res
    if (length(selected_years) == 0) stop("At least one year must be selected.")
    
    combo <- expand.grid(year = selected_years, month = match(selected_months, month.name))
    from_vector <- as.Date(paste0(combo$year, "-", combo$month, "-01"))
    to_vector <- as.Date(paste0(combo$year, "-", combo$month, "-", days_in_month(from_vector)))
    
  } else if (export_type == "Custom Date Range") {
    from_input <- dlg_input("Enter start date (YYYY-MM-DD):")$res
    to_input   <- dlg_input("Enter end date (YYYY-MM-DD):")$res
    
    from_vector <- as.Date(from_input, format = "%Y-%m-%d")
    to_vector   <- as.Date(to_input, format = "%Y-%m-%d")
    
    if (is.na(from_vector) || is.na(to_vector) || from_vector > to_vector) {
      stop("Invalid custom date range.")
    }
    
    selected_years <- unique(format(from_vector, "%Y"))
    
  } else {
    stop("No valid selection made.")
  }
  
  # Reduce to one range
  from_date <- min(from_vector)
  to_date   <- max(to_vector)
  
  ### --- Step 5: Pollutant Selection --- ###
  select_criteria <- dlg_message("Do you want to select Criteria Pollutants?", type = "yesno")$res
  select_meteorology <- dlg_message("Do you want to select Meteorological Data?", type = "yesno")$res
  
  criteria_pollutants_selected <- if (select_criteria == "yes") {
    dlg_list(criteria_pollutants_list, multiple = TRUE, title = "Select Criteria Pollutants")$res
  } else character(0)
  
  meteorology_data_selected <- if (select_meteorology == "yes") {
    dlg_list(meteorological_data_list, multiple = TRUE, title = "Select Meteorological Data")$res
  } else character(0)
  
  if (length(criteria_pollutants_selected) == 0 && length(meteorology_data_selected) == 0) {
    stop("You must select at least one parameter.")
  }
  
  ### --- Step 6: Seasonal Ozone Filtering --- ###
  if ("ozone" %in% criteria_pollutants_selected) {
    message("Ozone is a seasonal pollutant (May - September).")
    ozone_seasonal <- dlg_message("Do you want to filter for seasonal ozone (May - September)?", type = "yesno")$res
    
    if (ozone_seasonal == "yes") {
      seasonal_from <- as.Date(paste0(min(selected_years), "-05-01"))
      seasonal_to   <- as.Date(paste0(max(selected_years), "-09-30"))
      from_date <- max(from_date, seasonal_from)
      to_date   <- min(to_date, seasonal_to)
      message("Applying seasonal ozone filter: May - September.")
    }
  }
  
  ### --- Step 7: Group Parameters --- ###
  selected_parameters <- c(criteria_pollutants_selected, meteorology_data_selected)
  
  if (!exists("groupPm25Variants")) {
    groupPm25Variants <- function(p) {
      if (p %in% c("pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor")) return("pm2.5")
      return(p)
    }
  }
  
  grouped_parameters <- unique(sapply(selected_parameters, groupPm25Variants))
  
  ### --- Final Output --- ###
  return(list(
    selected_years       = as.numeric(selected_years),
    from_date            = format(from_date, "%Y/%m/%d"),
    to_date              = format(to_date, "%Y/%m/%d"),
    selected_parameters  = selected_parameters,
    grouped_parameters   = grouped_parameters
  ))
}

########################################################################################################################
# # Fetch AQS metadata and merge it with Envista meta table to retrieve historical metadata &
# remove the package's dependency on the cross_tables
########################################################################################################################

mergeMetaTables <- function(selected_parameter, from_date, to_date) {
  all_meta_list   <- list()
  pm25_variants   <- c("pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor")
  selected_pm25   <- intersect(pm25_variants, selected_parameter)
  other_parameters <- setdiff(unique(selected_parameter), pm25_variants)
  
  ## ----- Handle PM2.5 variants -----
  for (poll_variant in selected_pm25) {
    message("Fetching metadata for: ", poll_variant)
    
    aqs_code <- if (poll_variant == "pm2.5l_bam1022") 88101 else 88502
    
    aqs_meta_table <- aqs_monitors_by_state(
      parameter = aqs_code,
      stateFIPS = "41",
      bdate = format(lubridate::ymd(from_date), "%Y%m%d"),
      edate = format(lubridate::ymd(to_date), "%Y%m%d")
    )
    
    aqs_meta_table$stations_tag <- paste0(
      aqs_meta_table$state_code,
      aqs_meta_table$county_code,
      aqs_meta_table$site_number
    )
    
    aqs_meta_table <- aqs_meta_table %>% distinct(stations_tag, .keep_all = TRUE)

    # PM2.5 method filtering
    if (poll_variant == "pm2.5 estimate") {
      aqs_meta_table <- aqs_meta_table %>%
        filter(!is.na(last_method_code) & !last_method_code %in% c(791, 707))
    } else if (poll_variant == "pm2.5 est sensor") {
      aqs_meta_table <- aqs_meta_table %>%
        filter(!is.na(last_method_code) & last_method_code == 791)
    }
    
    shortname_lookup <- envista_meta_table %>%
      filter(monitor_type == poll_variant) %>%
      distinct(stations_tag, .keep_all = TRUE)
    
    aqs_meta_table <- aqs_meta_table %>%
      left_join(shortname_lookup[, c("site", "stations_tag")], by = "stations_tag") %>%
      mutate(site = ifelse(is.na(site), stations_tag, site))
    
    if (nrow(aqs_meta_table) > 0) {
      colnames(shortname_lookup)[colnames(shortname_lookup) == "latitude"]  <- "latitude_envista"
      colnames(shortname_lookup)[colnames(shortname_lookup) == "longitude"] <- "longitude_envista"
      
      aqs_meta_sub <- aqs_meta_table[, c(
        "local_site_name", "address", "measurement_scale", "stations_tag", "poc",
        "parameter_code", "site", "latitude", "longitude", "city_name",
        "county_name", "last_method_code", "open_date", "last_method_begin_date"
      )]
      
      names(aqs_meta_sub)[names(aqs_meta_sub) == "city_name"]   <- "city"
      names(aqs_meta_sub)[names(aqs_meta_sub) == "county_name"] <- "county"
      
      aqs_envista_meta <- left_join(
        aqs_meta_sub,
        shortname_lookup[, c("region_name", "latitude_envista", "longitude_envista", "census_classifier",
                             "name", "stations_tag", "units", "channel_id", "station_id", "monitor_type")],
        by = "stations_tag"
      )
      
      all_meta_list[[poll_variant]] <- aqs_envista_meta
    }
  }
  
  ## ----- Handle other pollutants -----
  for (poll_variant in other_parameters) {
    message("Fetching metadata for: ", poll_variant)
    
    aqs_code <- cross_tables$analyte_codes$aqs_name[
      tolower(cross_tables$analyte_codes$name) == tolower(poll_variant)
    ]
    
    aqs_meta_table <- aqs_monitors_by_state(
      parameter = aqs_code,
      stateFIPS = "41",
      bdate = format(lubridate::ymd(from_date), "%Y%m%d"),
      edate = format(lubridate::ymd(to_date), "%Y%m%d")
    )
    
    aqs_meta_table$stations_tag <- paste0(
      aqs_meta_table$state_code,
      aqs_meta_table$county_code,
      aqs_meta_table$site_number
    )
    
    aqs_meta_table <- aqs_meta_table %>% distinct(stations_tag, .keep_all = TRUE)
    
    shortname_lookup <- envista_meta_table %>%
      filter(monitor_type == poll_variant) %>%
      distinct(stations_tag, .keep_all = TRUE)
    
    aqs_meta_table <- aqs_meta_table %>%
      left_join(shortname_lookup[, c("site", "stations_tag")], by = "stations_tag") %>%
      mutate(site = ifelse(is.na(site), stations_tag, site))
    
    if (nrow(aqs_meta_table) > 0) {
      colnames(shortname_lookup)[colnames(shortname_lookup) == "latitude"]  <- "latitude_envista"
      colnames(shortname_lookup)[colnames(shortname_lookup) == "longitude"] <- "longitude_envista"
      
      aqs_meta_sub <- aqs_meta_table[, c(
        "local_site_name", "address", "measurement_scale", "stations_tag", "poc",
        "parameter_code", "site", "latitude", "longitude", "city_name",
        "county_name", "last_method_code", "open_date", "last_method_begin_date"
      )]
      
      names(aqs_meta_sub)[names(aqs_meta_sub) == "city_name"]   <- "city"
      names(aqs_meta_sub)[names(aqs_meta_sub) == "county_name"] <- "county"
      
      aqs_envista_meta <- left_join(
        aqs_meta_sub,
        shortname_lookup[, c("region_name", "latitude_envista", "longitude_envista", "census_classifier",
                             "name", "stations_tag", "units", "channel_id", "station_id", "monitor_type")],
        by = "stations_tag"
      )
      
      all_meta_list[[poll_variant]] <- aqs_envista_meta
    }
  }
  
  ## Unified return
  monitor_table <- bind_rows(all_meta_list)
  return(monitor_table)
}

########################################################################################################################
# # Fetch AQ data 
########################################################################################################################

fetchAirQualityData  <- function(from_date, to_date, year_to_foc, grouped_parameter, selected_parameter, parameter) {
  pm25_variants      <- c("pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor")
  selected_pm25      <- intersect(pm25_variants, selected_parameter)
  
  is_pm25 <- FALSE
  if (parameter == "pm2.5") {
    is_pm25 <- TRUE
  } 
  
  all_hourly_data    <- list()
  all_daily_data     <- list()
  meta_variants_list <- list()
  aqs_envista_meta   <- NULL
  
  if (is_pm25) {
    message("Stacking data for pm2.5 variants")
    all_hourly_rows <- list()
    all_daily_rows  <- list()
    
    for (poll_variant in unique(selected_pm25)) {
      
      pollutant <- groupPm25Variants(poll_variant)
      
      aqs_envista_meta <- mergeMetaTables(selected_parameter, from_date, to_date) %>%
        filter(monitor_type == poll_variant)
      
      # Defensive check before fetching data
      if (!is.null(aqs_envista_meta) && "site" %in% names(aqs_envista_meta)) {
        site_vec <- aqs_envista_meta$site
        site_vec <- site_vec[!is.na(site_vec)]
        
        if (length(site_vec) > 0) {
          site_pollutant_meta <- fetchAirQualityDataFromAPI(
            site          = site_vec,
            poll_name     = poll_variant,
            from_date     = from_date,
            to_date       = to_date,
            monitor_table = aqs_envista_meta
          )
          
          if (is.null(site_pollutant_meta) || any(is.na(site_pollutant_meta)) || nrow(site_pollutant_meta$meta) == 0) {
            message("No metadata found for ", poll_variant, " in ", year_to_foc, ". Skipping...")
            next
          }
          
        } else {
          message("No valid site info in metadata for ", poll_variant, ". Skipping...")
          next
        }
        
        # Only append metadata once it's valid
        meta_variants_list[[poll_variant]] <- aqs_envista_meta
        
      } else {
        message("No metadata available for ", poll_variant, ". Skipping...")
        next
      }
      
      compiled_result <- compileAirQualityData(site_pollutant_meta, request_type = "aqs_envista", is_pm25 = TRUE)
      compiled_data   <- compiled_result$data %>%
        mutate(
          sample_frequency = as.character(sample_frequency),
          method_type      = as.character(method_type) 
        )
      if (is.null(compiled_data) || nrow(compiled_data) == 0) {
        message("No compiled data for ", poll_variant)
        next
      }
      all_hourly_rows[[poll_variant]] <- compiled_data %>%
        filter(tolower(sample_frequency) %in% tolower(compiled_result$hourly_like_freq)) %>%
        filter(lubridate::year(datetime) == year_to_foc) 
      
      all_daily_rows[[poll_variant]] <- compiled_data %>%
        filter(tolower(sample_frequency) %!in% tolower(compiled_result$hourly_like_freq)) 
    }
    all_hourly_data[["pm2.5"]] <- bind_rows(all_hourly_rows)
    all_daily_data [["pm2.5"]] <- bind_rows(all_daily_rows)
    
    # combine metadata across all PM2.5 variants
    aqs_envista_meta <- bind_rows(meta_variants_list) %>% distinct(site, monitor_type, .keep_all = TRUE)
    
  } else {
    # Now handle all other pollutants (ozone, nox, co, etc)
    other_parameters <- setdiff(selected_parameter, pm25_variants)
    
    for (poll_variant in unique(other_parameters)) {
      message("Fetching metadata for: ", poll_variant)
      
      aqs_envista_meta <- mergeMetaTables(selected_parameter, from_date, to_date) %>%
        filter(monitor_type == poll_variant)
      
      # Add this defensive check
      if (!is.null(aqs_envista_meta) && "site" %in% names(aqs_envista_meta)) {
        site_vec <- aqs_envista_meta$site
        site_vec <- site_vec[!is.na(site_vec)]
        
        if (length(site_vec) == 0) {
          message("No valid site info in metadata for ", poll_variant, ". Skipping...")
          next
        }
      } else {
        message("No metadata available for ", poll_variant, ". Skipping...")
        next
      }
      
      site_pollutant_meta <- fetchAirQualityDataFromAPI(
        site          = site_vec,
        poll_name     = poll_variant,
        from_date     = from_date,
        to_date       = to_date,
        monitor_table = aqs_envista_meta
      )
      
      if (is.null(site_pollutant_meta) || any(is.na(site_pollutant_meta)) || nrow(site_pollutant_meta$meta) == 0) {
        message("No metadata found for ", poll_variant, " in ", year_to_foc, ". Skipping...")
        next
      }
      
      compiled_result <- compileAirQualityData(site_pollutant_meta, request_type = "aqs_envista", is_pm25 = FALSE)
      compiled_data   <- compiled_result$data %>%
        mutate(
          sample_frequency = as.character(sample_frequency),
          method_type      = as.character(method_type)
        )
      all_hourly_data[[poll_variant]] <- compiled_data %>%
        filter(tolower(sample_frequency) %in% tolower(compiled_result$hourly_like_freq)) %>%
        filter(lubridate::year(datetime) == year_to_foc)
      
      all_daily_data[[poll_variant]] <- compiled_data %>%
        filter(tolower(sample_frequency) %!in% tolower(compiled_result$hourly_like_freq))
    }
  }
  
  return(list(
    hourly_data = all_hourly_data,
    daily_data  = all_daily_data,
    meta        = aqs_envista_meta
  ))
}

########################################################################################################################
# post processing of hourly Data
########################################################################################################################
# Check if 'hourlyData' exists in the environment before loading from disk.
# If available in memory, use it directly; otherwise, read the saved raw data.
# Merge metadata, export the processed data, and ensure 'fflag' calculation is complete for the hourly database 
# (3-hour moving average > 15 µg/m³, based on midnight-to-midnight data)

processHourlyData <- function(data, year_to_foc, root_path, grouped_parameters, monitor_table) {
  
  for (poll_variant in grouped_parameters) {
    message("Processing hourly data for ", poll_variant, " - Year: ", year_to_foc)
    
    #Define paths
    category <- getParameterCategory(poll_variant) 
    
    # Step 0: Normalize parameter and build file paths
    parameter <- groupPm25Variants(poll_variant)
    hourly_year_folder     <- file.path(root_path, "Hourly", category, "xlsx", poll_variant, as.character(year_to_foc))
    raw_file_path          <- file.path(hourly_year_folder, paste0(poll_variant, "_hourly_", year_to_foc, "_noMeta.xlsx"))
    final_hourly_file_path <- file.path(hourly_year_folder, paste0(poll_variant, "_hourly_", year_to_foc, ".xlsx"))
    
    ensureDirectory(final_hourly_file_path)
    
    # Step 1: Use in-memory or load from file
    # --- Step 1: Load Hourly Data ---
    if (is.null(data) || nrow(data) == 0) {
      if (file.exists(raw_file_path)) {
        message("Loading existing hourly data from file: ", raw_file_path)
        data <- read.xlsx(raw_file_path, colNames = TRUE, detectDates = TRUE)
        
        if (!"datetime" %in% colnames(data)) {
          stop("Error: 'datetime' column is missing in hourly data.")
        }
        
        data$datetime <- as.POSIXct(data$datetime, format = "%Y-%m-%d %H:%M:%S")
        data          <- data %>% filter(year(datetime) == year_to_foc)
      } else {
        message("No existing hourly data found for ", poll_variant, ". Skipping processing.")
        next
      }
    } else {
      message("Using in-memory hourly data for ", poll_variant, " in ", year_to_foc)
    }
    
    # --- Step 2: Quick Clean ---
    if ("simple_qual_best" %in% names(data)) {
      data$simple_qual_best[data$simple_qual_best == "character(0)"] <- "ok"
    }
    
    if ("datetime" %in% names(data)) {
      min_datetime <- min(data$datetime, na.rm = TRUE)
      data  <- data[data$datetime != min_datetime, ]
    }
    
    # --- Step 3: PM2.5-specific Calculations ---
    if (poll_variant == "pm2.5") {
      message("Calculating 3-hour rolling PM2.5 average...")
      
      if ("sample_measurement_best" %in% names(data)) {
        data <- data %>%
          arrange(site, method_type, poc, datetime) %>%
          group_by(site, method_type, poc) %>%
          mutate(pm25_3hr_avg = zoo::rollapplyr(sample_measurement_best, width = 3, FUN = mean, fill = NA, partial = TRUE)) %>%
          ungroup()
        
        data$fflag <- ifelse(data$pm25_3hr_avg > 15, 1, 0)
      } else {
        message("Warning: 'sample_measurement_best' column is missing, skipping PM2.5 calculations.")
      }
      pm25_short_names <- c(
        "pm2.5 estimate"    = "neph",
        "pm2.5l_bam1022"    = "bam",
        "pm2.5 est sensor"  = "sensor"
      )
      
      if ("method_type" %in% names(data)) {
        data <- data %>%
          mutate(method_type = recode(method_type, !!!pm25_short_names))
      }
    }
    
    # --- Step 4: Ozone-specific Calculations ---
    if (poll_variant == "ozone") {
      message("Calculating 8-hour ozone averages...")
      data <- data %>%
        arrange(site, year(datetime), datetime) %>%
        group_by(site, year = year(datetime), poc) %>%
        mutate(o3_8hr = runOver8Hour(sample_measurement_best, 70)) %>%
        ungroup()
    }
    
    # --- Step 4: Save raw version to Excel safely ---
    if (file.exists(raw_file_path)) {
      tryCatch({
        file.remove(raw_file_path)
      }, warning = function(w) {
        message("Warning removing file: ", w$message)
      }, error = function(e) {
        message("Cannot remove file. Is it open in Excel? ", e$message)
      })
    }
    
    tryCatch({
      write.xlsx(data, raw_file_path, overwrite = TRUE, , rowNames = FALSE)
      message("File written: ", raw_file_path)
    }, error = function(e) {
      message("Failed to write file: ", e$message)
    })
  }
    
    # --- Step 5: Add metadata ---
    message("Expanding hourly data with metadata...")
    data <- addTimeIntervals(data)
    
    if (!is.null(monitor_table) && nrow(monitor_table) > 0) {
      data <- left_join(
        data,
        monitor_table[, c("region_name", "census_classifier", "local_site_name",
                          "name", "city", "county", "address", "stations_tag",
                          "site",  "last_method_code", "open_date", "last_method_begin_date")] %>%
          distinct(site, .keep_all = TRUE),
        by = "site"
      )
    }
    
    if (!"year" %in% names(data)) {
      data$year <- format(data$datetime, "%Y")
    }
    
    colnames(data) <- tolower(colnames(data))
    
    # --- Step 6: Save final file ---
    message("Saving processed hourly data to: ", final_hourly_file_path)

    # Safely remove existing file if it exists
    if (file.exists(final_hourly_file_path)) {
      tryCatch({
        file.remove(final_hourly_file_path)
      }, warning = function(w) {
        message(" Warning removing existing file: ", conditionMessage(w))
      }, error = function(e) {
        message(" Could not remove file. It may be open in Excel: ", conditionMessage(e))
      })
    }
    
    # Try writing the file
    tryCatch({
      openxlsx::write.xlsx(data, file = final_hourly_file_path, overwrite = TRUE)
      message(" File written successfully to: ", final_hourly_file_path)
    }, error = function(e) {
      message(" Failed to write Excel file: ", conditionMessage(e))
    })
    
  return(data)
}

########################################################################################################################
# Process and save daily data
########################################################################################################################
# Here is the function that properly creates the daily database from the hourly database,
# ensures proper parameter-specific calculations (e.g., different approaches for PM2.5 and ozone),
# and follows the correct order of processing:
#   
# Load or read the hourly database
# Compute daily values (parameter-dependent) and export
# If the daily database exists in the environment, merge metadata and export
# If not, reload and then merge metadata
# 'fflag' calculation is complete for the daily database (24-hour average > 15 µg/m³, based on midnight-to-midnight data).

processDailyData <- function(data, year_to_foc, root_path, grouped_parameters, monitor_table) {
  pm25_variants     <- c("pm2.5 estimate", "pm2.5l_bam1022", "pm2.5 est sensor")
  
  for (poll_variant in grouped_parameters){
    message("Processing daily data for ", poll_variant, " - Year: ", year_to_foc)
    
    # Define file paths using the normalized category
    category <- getParameterCategory(poll_variant)
    
    daily_year_folder        <- file.path(root_path, "Daily", category, "xlsx", poll_variant, as.character(year_to_foc))
    raw_daily_file_path      <- file.path(daily_year_folder, paste0(poll_variant, "_daily_", year_to_foc, "_noMeta.xlsx"))
    final_daily_file_path    <- file.path(daily_year_folder, paste0(poll_variant, "_daily_", year_to_foc, ".xlsx"))
    
    # Ensure the final output directory exists
    ensureDirectory(final_daily_file_path)
    
    # Apply frequency check only for PM2.5 variants
    if (poll_variant == "pm2.5" && "sample_frequency" %in% names(data)) {
      
      hourly_like_freqs = c("hourly", "daily: 24 - 1 hr samples -pams")
      
      # Identify if any non-hourly-like sampling exists
      # is_already_daily <- any(!tolower(data$sample_frequency) %in% tolower(hourly_like_freqs))
      # Only use fallback if *all* records are non-hourly-like
      is_all_non_hourly <- all(!tolower(data$sample_frequency) %in% tolower(hourly_like_freqs))
      
      if (is_all_non_hourly) {
        message("Detected non-hourly-like PM2.5 data. Filtering for daily-only processing...")
        
        if (!"datetime" %in% colnames(data) && "date" %in% colnames(data)) {
          data <- data %>% mutate(datetime = as.POSIXct(date))
        }
        
        # Replace 'datetime' column with 'date' as Date
        if (all(c("datetime", "sample_measurement_best") %in% names(data))) {
          
          daily_data <- data %>%
            rename(date = datetime,
                   pm25 = sample_measurement_best)
          
          daily_data <- daily_data %>%
            mutate(
              start_time = paste0(date, "T00:00"),
              end_time   = paste0(date, "T23:00")
            )}
        
        daily_data <- daily_data %>%
          calc_aqi() %>%
          calc_aqi_old()
        
        
      } else {
        daily_data <- calculate24HourAverage(data) %>%
          calc_aqi() %>%
          calc_aqi_old()
        daily_data$sample_frequency <- 'hourly'
      }
      
      # Only values in the named vector are changed — others (like "ozone") remain untouched
      pm25_short_names <- c(
        "pm2.5 estimate"    = "neph",
        "pm2.5l_bam1022"    = "bam",
        "pm2.5 est sensor"  = "sensor"
      )
      
      # Replace monitor_type using named vector mapping
      if ("method_type" %in% names(daily_data)) {
        daily_data <- daily_data %>%
          mutate(method_type = recode(method_type, !!!pm25_short_names))
      }
      
      daily_subdata <- addTimeIntervalsDaily(daily_data)
      
      ### Merge HMS Data (PM2.5 gets direct HMS, ozone borrows it) --- ###
      hms_data <- retrieveHmsData(year_to_foc, daily_subdata)
      
      if (!is.null(hms_data) && nrow(hms_data) > 0) {
        # Clean up HMS date formats
        if ("date" %in% names(daily_data) && is.character(daily_data$date)) {
          daily_data <- daily_data %>%
            mutate(date = as.Date(str_remove(date, " -08"), format = "%Y-%m-%d"))
        }
        
        hms_data <- hms_data %>%
          mutate(date = as.Date(str_remove(date, " UTC"), format = "%Y-%m-%d"))
        
        # Merge HMS data into PM2.5 data
        daily_data <- left_join(daily_data, hms_data, by = c("site", "date"), relationship = "many-to-many")
        message("HMS datamerged with PM2.5 successfully")
      } else {
        message("No HMS data available for PM2.5 in ", year)
      }
      
      # Add fflag if not already present
      if (!"fflag" %in% colnames(daily_data) && "pm25" %in% colnames(daily_data)) {
        daily_data <- daily_data %>%
          mutate(
            fflag = case_when(
              pm25 >= 15 ~ 1,
              !is.na(smoke_level) & pm25 >= 10 & pm25 <= 15 ~ 1,
              TRUE ~ 0
            )
          )
      }
      
      # Step: Check for file-based daily data
      if (file.exists(raw_daily_file_path)) {
        message("Loading existing daily data from file: ", raw_daily_file_path)
        file_daily_data <- read.xlsx(raw_daily_file_path, colNames = TRUE, detectDates = TRUE)
        file_daily_data$date <- as.Date(file_daily_data$date)
        
        # Ensure consistent type for smoke_level before merging
        if ("smoke_level" %in% names(file_daily_data)) {
          file_daily_data$smoke_level <- as.character(file_daily_data$smoke_level)
        }
      } else {
        message("No existing file-based daily data found for ", poll_variant, " in ", year_to_foc)
        file_daily_data <- NULL
      }
      
      # Step: Combine in-memory and file-based data
      if (!is.null(daily_data) && !is.null(file_daily_data)) {
        message("Merging in-memory and file-based daily data...")
        daily_data <- bind_rows(daily_data, file_daily_data)
      } else if (is.null(daily_data) && !is.null(file_daily_data)) {
        message("Using only file-based daily data.")
        daily_data <- file_daily_data
      } else if (!is.null(daily_data)) {
        message("Using only in-memory daily data.")
        daily_data <- daily_data
      } else {
        message("No daily data found for ", poll_variant, " in ", year_to_foc, ". Skipping processing.")
        return(NULL)
      }
      
      pm25_tag  <- getPm25Tag(daily_data)
      print(pm25_tag)
      
      # Export wildfire-impacted data if applicable
      exportWildfireData(daily_data, pm25_tag, year_to_foc, root_path)
      
    } else if (poll_variant == "ozone") {
      daily_data <- calculateDailyMax8hrOzone(data)
      
      daily_data$o3_ppm <- daily_data$o3 / 1000  # Add new column in ppm
      daily_data <- calc_aqi(daily_data, pollutant_column = "o3_ppm", pollutant = "ozone")
      
      # --- Load corresponding PM2.5 hierarchy file ---
      hierarchy_path <- file.path(root_path, "Daily", category, "xlsx", poll_variant, as.character(year_to_foc),
                                  paste0("pm25_daily_hierarchy_", year_to_foc, ".xlsx"))
      
      if (file.exists(hierarchy_path)) {
        pm25_hierarchy <- read.xlsx(hierarchy_path, colNames = TRUE, detectDates = TRUE)
        
        # --- Add fflag and PM2.5 info from PM2.5 hierarchy ---
        daily_data <- addPM25FlagsToOzone(daily_data, pm25_hierarchy)
        message("Connected ozone with PM2.5 hierarchy for wildfire flagging.")
      } else {
        warning("PM2.5 hierarchy file not found — unable to flag ozone with wildfire data.")
      }
      
    } else if (poll_variant %in% c("nox", "so2", "co")) {
      daily_data <- calculate24HourAverage(data) %>%
        calc_aqi()
    } else {
      message("No daily calculation function defined for ", poll_variant, ". Skipping.")
      return(NULL)
    }
    
    # Safely remove existing file if it exists
    if (file.exists(raw_daily_file_path)) {
      tryCatch({
        file.remove(raw_daily_file_path)
      }, warning = function(w) {
        message(" Warning removing existing file: ", conditionMessage(w))
      }, error = function(e) {
        message(" Could not remove file. It may be open in Excel: ", conditionMessage(e))
      })
    }
    
    # Try writing the file
    tryCatch({
      openxlsx::write.xlsx(daily_data, file = raw_daily_file_path, overwrite = TRUE)
      message(" File written successfully to: ", output_path)
    }, error = function(e) {
      message(" Failed to write Excel file: ", conditionMessage(e))
    })
    
    message("Raw daily data saved: ", raw_daily_file_path)
    
    ### Merge Metadata (Only if Data Exists) --- ###
    daily_data <- addTimeIntervalsDaily(daily_data)
    
    if (!is.null(monitor_table) && nrow(monitor_table) > 0) {
      daily_data <- left_join(
        daily_data,
        monitor_table[, c("region_name", "census_classifier", "local_site_name",
                          "name", "city", "county", "address", "stations_tag",
                          "site",  "last_method_code", "open_date", "last_method_begin_date"
        )] %>% distinct(site, .keep_all = TRUE), by = "site")
    }
    
    # Convert all column names to lowercase
    colnames(daily_data) <- tolower(colnames(daily_data))
    
    ### --- Step 7: Export Final Daily Data with Metadata --- ###
    # Safely remove existing file if it exists
    if (file.exists(final_daily_file_path)) {
      tryCatch({
        file.remove(final_daily_file_path)
      }, warning = function(w) {
        message(" Warning removing existing file: ", conditionMessage(w))
      }, error = function(e) {
        message(" Could not remove file. It may be open in Excel: ", conditionMessage(e))
      })
    }
    
    # Try writing the file
    tryCatch({
      openxlsx::write.xlsx(daily_data, file = final_daily_file_path, overwrite = TRUE)
      message(" File written successfully to: ", final_daily_file_path)
    }, error = function(e) {
      message(" Failed to write Excel file: ", conditionMessage(e))
    })
    
    message("Expanded data saved: ", final_daily_file_path)
    
    return(daily_data)
  }
}

########################################################################################################################
# Function to retrieve HMS Data
########################################################################################################################

retrieveHmsData <- function(year, daily_pm25) {
  
  # Define the folder path for the selected year
  hms_folder_path <- file.path(root_path, "Additional_Data", "HMS_Daily", "xlsx", as.character(year))
  
  # Define the file path
  hms_file_path <- file.path(hms_folder_path, paste0("HMS_daily_", as.character(year), ".xlsx"))
  
  if (file.exists(hms_file_path)) {
    message("HMS data loaded from file: ", hms_file_path)
    return(read.xlsx(hms_file_path, colNames = TRUE, detectDates = TRUE))
  } else {
    message("No existing HMS file found. Generating HMS data for ", year, "...")
    hms_data <- addHmsLevelsDaily(daily_pm25)  # Assuming daily_pm25 is defined elsewhere
    
    # Ensure the directory exists, create it if it doesn't
    if (!dir.exists(hms_folder_path)) {
      dir.create(hms_folder_path, recursive = TRUE)
      message("Created missing directory: ", hms_folder_path)
    }
    
    # Save the HMS data
    write.xlsx(hms_data, file = hms_file_path, rowNames = FALSE)
    message("HMS data successfully saved to: ", hms_file_path)
    
    return(hms_data)
  }
}
 
########################################################################################################################
# reusable function that applies the PM2.5 hierarchy
########################################################################################################################  

hierarchyPM25Daily <- function(df, value_col = "pm25") {
  stopifnot(all(c("site", "method_type") %in% colnames(df)))
  if (!"date" %in% names(df)) stop("This function only supports daily data. A 'date' column is required.")
  
  # Normalize method_type
  df <- df %>% mutate(method_type = tolower(trimws(method_type)))
  
  # Summarize to avoid duplicates before pivoting
  df_summarized <- df %>%
    group_by(site, date, method_type) %>%
    summarise(
      pm25           = first(na.omit(.data[[value_col]])),
      aqi_pm25       = first(na.omit(aqi_pm25)),
      aqi_pm25_old   = first(na.omit(aqi_pm25_old)),
      hcat_pm25      = first(na.omit(hcat_pm25)),
      hcat_pm25_old  = first(na.omit(hcat_pm25_old)),
      poc            = paste(unique(na.omit(poc)), collapse = ";"),
      .groups = "drop"
    )
  
  # Pivot to wide format by method_type
  df_wide <- df_summarized %>%
    pivot_wider(
      names_from = method_type,
      values_from = c(pm25, aqi_pm25, aqi_pm25_old, hcat_pm25, hcat_pm25_old, poc),
      names_glue = "{.value}_{method_type}"
    )
  
  # Derive best value by hierarchy: bam > neph > sensor
  df_prioritized <- df_wide %>%
    mutate(
      pm25_best = coalesce(pm25_bam, pm25_neph, pm25_sensor),
      method_used_best = case_when(
        !is.na(pm25_bam)    ~ "bam",
        !is.na(pm25_neph)   ~ "neph",
        !is.na(pm25_sensor) ~ "sensor",
        TRUE ~ NA_character_
      ),
      aqi_pm25_best = coalesce(aqi_pm25_bam, aqi_pm25_neph, aqi_pm25_sensor),
      aqi_pm25_old_best = coalesce(aqi_pm25_old_bam, aqi_pm25_old_neph, aqi_pm25_old_sensor),
      hcat_pm25_best = coalesce(hcat_pm25_bam, hcat_pm25_neph, hcat_pm25_sensor),
      hcat_pm25_old_best = coalesce(hcat_pm25_old_bam, hcat_pm25_old_neph, hcat_pm25_old_sensor)
    )
  
  # Join back additional metadata if available
  cols_to_keep <- df %>%
    select(site, date, any_of(c("name", "city", "county", "name", "address",
                                "census_classifier", "smoke_level", "fflag",
                                "missing_obs", "latitude", "longitude",
                                "sample_frequency" ))) %>% distinct()
  
  df_final <- left_join(df_prioritized, cols_to_keep, by = c("site", "date"))
  
  return(df_final)
}

  
  # # Optional: Drop lower-priority methods when BAM exists
  # df_prioritized <- df_prioritized %>%
  #   mutate(
  #     pm25_neph       = ifelse(!is.na(pm25_bam), NA, pm25_neph),
  #     pm25_sensor     = ifelse(!is.na(pm25_bam), NA, pm25_sensor),
  #     aqi_pm25_neph   = ifelse(!is.na(pm25_bam), NA, aqi_pm25_neph),
  #     aqi_pm25_sensor = ifelse(!is.na(pm25_bam), NA, aqi_pm25_sensor),
  #     hcat_pm25_neph  = ifelse(!is.na(pm25_bam), NA, hcat_pm25_neph),
  #     hcat_pm25_sensor= ifelse(!is.na(pm25_bam), NA, hcat_pm25_sensor)
  #   )


########################################################################################################################
# get fflag from surrogate sites
########################################################################################################################

addPM25FlagsToOzone <- function(ozone_df, pm25_df) {
  library(dplyr)
  
  # Step 1: Direct merge by site/date (same location)
  ozone_merged <- ozone_df %>%
    left_join(
      pm25_df %>%
        select(site, date, fflag, pm25_best, method_used_best),
      by = c("site", "date")
    )
  
  # Step 2: Ozone records still missing fflag
  ozone_missing <- ozone_merged %>%
    filter(is.na(fflag)) %>%
    select(site, date, ozone_lat = latitude, ozone_lon = longitude)
  
  # Step 3: PM2.5 sites with fflag for the same dates
  pm25_available <- pm25_df %>%
    filter(!is.na(fflag)) %>%
    select(date,
           site_pm25 = site,
           fflag,
           pm25_best,
           method_used_best,
           latitude,
           longitude)
  
  # Step 4: Join and find nearest PM2.5 site for each ozone site
  nearest_matches <- ozone_missing %>%
    inner_join(pm25_available, by = "date", relationship = "many-to-many") %>%
    mutate(dist = sqrt((ozone_lat - latitude)^2 + (ozone_lon - longitude)^2)) %>%
    group_by(site, date) %>%
    slice_min(dist, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(site, date,
           surrogate_site_pm25 = site_pm25,
           surrogate_fflag     = fflag,
           surrogate_pm25      = pm25_best,
           surrogate_method    = method_used_best)
  
  # Step 5: Fill in missing fields with surrogate data
  ozone_final <- ozone_merged %>%
    left_join(nearest_matches, by = c("site", "date")) %>%
    mutate(
      fflag            = ifelse(is.na(fflag), surrogate_fflag, fflag),
      pm25_best        = ifelse(is.na(pm25_best), surrogate_pm25, pm25_best),
      method_used_best = ifelse(is.na(method_used_best), surrogate_method, method_used_best),
      site_pm25        = surrogate_site_pm25
    ) %>%
    select(-surrogate_fflag, -surrogate_pm25, -surrogate_method, -surrogate_site_pm25)
  
  return(ozone_final)
}

########################################################################################################################
# create cross tables
########################################################################################################################

loadCrossTables <- function(path) {
  
  # Read tables
  analyte_code_map <- read.csv(file.path(path, "analyteXcode.csv"))
  qualifier_code_map <- read.csv(file.path(path, "envista_api_qualifier_code.csv"))

  # Convert site names to lowercase safely
  try({aqm_sites_metadata$site <- tolower(aqm_sites_metadata$site)}, silent = TRUE)
  
  # Store them in a named list
  cross_table_list <- list(
    analyte_codes = analyte_code_map, 
    qualifier_codes = qualifier_code_map
  )
  
  return(cross_table_list)
}

########################################################################################################################
# export wildifre dates
########################################################################################################################

# Function to export wildfire-flagged data
exportWildfireData <- function(daily_data, parameter, year, root_path) {
  message("Exporting wildfire data for ", parameter, " in ", as.character(year))
  
  if (is.null(daily_data) || nrow(daily_data) == 0) {
    message("No wildfire data available to export for ", parameter, ". Skipping.")
    return(NULL)
  }
  
  # Select only the relevant columns
  wildfire_export <- daily_data %>%
    select(date, site, fflag, pm25, smoke_level, method_type)
  
  wildfire_file_path <- file.path(root_path, "Additional_Data", "Wildfire_Impact", paste0("fflag_", parameter, "_", year, ".xlsx"))
  
  # Ensure the directory exists before writing
  ensureDirectory(wildfire_file_path)
  
  write.xlsx(wildfire_export, file = wildfire_file_path, rowNames = FALSE)
  message("Wildfire data exported successfully: ", wildfire_file_path)
}

########################################################################################################################
# Export wildfire trends based on PM2.5 data
########################################################################################################################

generatePM25WFTrends <- function(pm25_hierarchy_df, year_to_foc) {
  tryCatch({
    wf_aqi_categories <- c("USG", "Unhealthy", "Very Unhealthy", "Hazardous")
    
    # Filter to wildfire-relevant categories
    wf_summary_raw <- pm25_hierarchy_df %>%
      filter(hcat_pm25_best %in% wf_aqi_categories) %>%
      mutate(year = year(date))
    
    # Site-level metadata grid
    site_meta <- wf_summary_raw %>%
      distinct(site, city, county, name, address, year, sample_frequency)
    
    # Create full site-year-health grid
    full_grid <- expand_grid(
      site_meta,
      health_category = wf_aqi_categories
    )
    
    # Summarize actual days per health category
    summary_counts <- wf_summary_raw %>%
      group_by(site, city, county, name, address, year, sample_frequency, health_category = hcat_pm25_best) %>%
      summarise(n_days = n(), .groups = "drop")
    
    # Merge with full grid and fill in 0s
    wf_summary <- full_grid %>%
      left_join(summary_counts, by = c("site", "city", "county", "name", "address",
                                       "year", "sample_frequency", "health_category")) %>%
      mutate(n_days = replace_na(n_days, 0)) %>%
      arrange(site, year, health_category)
    
    return(wf_summary)
    
  }, error = function(e) {
    warning("Failed to generate wildfire PM2.5 summary: ", e$message)
    return(NULL)
  })
  
}

