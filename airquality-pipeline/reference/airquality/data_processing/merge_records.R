########################################################################################################################
# make one datastream from aqs and envista data
########################################################################################################################

homogenizeDataStreams <- function(aqs_data, envista_data){
  aqs_data     <- aqs_data     %>% standardizeAqsData() %>% pluck("data")
  envista_data <- envista_data %>% standardizeEnvistaData() 

  merged_data <- full_join(aqs_data, envista_data, by = 'datetime', na_matches = "never", suffix = c("_aqs", "_envista"))
  merged_data <- merged_data[!(is.na(merged_data$data_source_envista) & is.na(merged_data$data_source_aqs)), ]
  
  merged_data$sample_measurement_best <- NA
  merged_data$units_of_measure_best   <- NA
  merged_data$qualifier_best          <- NA
  merged_data$simple_qual_best        <- NA
  merged_data$data_source             <- NA
  merged_data$method_code             <- NA
  merged_data$method_type             <- NA
  merged_data$poc                     <- NA
  merged_data$latitude                <- NA
  merged_data$longitude               <- NA
  merged_data$site                    <- NA
  merged_data$sample_frequency <- NA_character_
  
  
  fill_with_aqs <- !is.na(merged_data$data_source_aqs)
  merged_data$sample_measurement_best[fill_with_aqs] <- merged_data$sample_measurement_aqs[fill_with_aqs]
  merged_data$units_of_measure_best[fill_with_aqs]   <- merged_data$units_of_measure_aqs[fill_with_aqs]
  merged_data$qualifier_best[fill_with_aqs]          <- merged_data$qualifier_aqs[fill_with_aqs]
  merged_data$simple_qual_best[fill_with_aqs]        <- merged_data$simple_qual_aqs[fill_with_aqs]
  merged_data$data_source[fill_with_aqs]             <- merged_data$data_source_aqs[fill_with_aqs]
  merged_data$method_code[fill_with_aqs]             <- merged_data$method_code_aqs[fill_with_aqs]
  merged_data$method_type[fill_with_aqs]             <- merged_data$method_type_aqs[fill_with_aqs]
  merged_data$poc[fill_with_aqs]                     <- merged_data$poc_aqs[fill_with_aqs]
  merged_data$latitude[fill_with_aqs]                <- merged_data$latitude_aqs[fill_with_aqs]
  merged_data$longitude[fill_with_aqs]               <- merged_data$longitude_aqs[fill_with_aqs]
  merged_data$site[fill_with_aqs]                    <- merged_data$site_aqs[fill_with_aqs]
  merged_data$sample_frequency[fill_with_aqs]        <- merged_data$sample_frequency_aqs[fill_with_aqs]
  
  
  fill_with_envista <- !is.na(merged_data$data_source_envista) & is.na(merged_data$data_source_aqs)
  merged_data$sample_measurement_best[fill_with_envista]  <- merged_data$sample_measurement_envista[fill_with_envista]
  merged_data$units_of_measure_best[fill_with_envista]    <- merged_data$units_of_measure_envista[fill_with_envista]
  merged_data$qualifier_best[fill_with_envista]           <- merged_data$qualifier_envista[fill_with_envista]
  merged_data$simple_qual_best[fill_with_envista]         <- merged_data$simple_qual_envista[fill_with_envista]
  merged_data$data_source[fill_with_envista]              <- merged_data$data_source_envista[fill_with_envista]
  merged_data$method_code[fill_with_envista]              <- merged_data$method_code_envista[fill_with_envista]
  merged_data$method_type[fill_with_envista]              <- merged_data$parameter_envista[fill_with_envista]
  merged_data$poc[fill_with_envista]                      <- merged_data$poc_envista[fill_with_envista]
  merged_data$latitude[fill_with_envista]                 <- merged_data$latitude_envista[fill_with_envista]
  merged_data$longitude[fill_with_envista]                <- merged_data$longitude_envista[fill_with_envista]
  merged_data$site[fill_with_envista]                     <- merged_data$site_envista[fill_with_envista]
  merged_data$sample_frequency[fill_with_envista]             <- merged_data$sample_frequency_envista[fill_with_aqs]
  
  
  merged_data <- merged_data %>% select(
    datetime, site,
    sample_measurement_aqs, sample_measurement_envista,
    units_of_measure_best, qualifier_best, simple_qual_best,
    data_source, poc, method_code, method_type,
    latitude, longitude, sample_measurement_best,
    sample_frequency)

  return(merged_data)
}

########################################################################################################################
# make one datastream from aqs and envista data
########################################################################################################################

chooseDatastream <- function(data, source_type) {
  if (source_type == 'aqs_only') {
    data_list <- standardizeAqsData(data)  # returns list
    data <- data_list$data
    sample_frequency <- data_list$frequency
  } else if (source_type == 'envista_only') {
    data <- standardizeEnvistaData(data)
    if (!"sample_frequency" %in% names(data)) {
      data$sample_frequency <- NA
    }
    sample_frequency <- unique(tolower(trimws(data$sample_frequency)))
  } else {
    stop("Invalid source_type in chooseDatastream()")
  }
  
  # Harmonized output fields
  data$sample_measurement_best     <- data$sample_measurement
  data$units_of_measure_best       <- data$units_of_measure
  data$qualifier_best              <- data$qualifier
  data$simple_qual_best            <- data$simple_qual
  data$data_source                 <- data$data_source
  data$method_code                 <- data$method_code
  data$method_type                 <- data$method_type
  data$poc                         <- data$poc
  data$site                        <- data$site
  data$sample_measurement_aqs      <- if (source_type == 'aqs_only') data$sample_measurement else NA
  data$sample_measurement_envista  <- if (source_type == 'envista_only') data$sample_measurement else NA
  
  if (source_type == "aqs_only") {
    data$sample_measurement_best <- data$sample_measurement_aqs
  } else if (source_type == "envista_only") {
    data$sample_measurement_best <- data$sample_measurement_envista
  } else {
    # merged case: prefer AQS
    data$sample_measurement_best <- dplyr::coalesce(data$sample_measurement_aqs, data$sample_measurement_envista)
  }
  
  
  # Ensure sample_frequency is assigned correctly
  if (!"sample_frequency" %in% names(data)) {
    data$sample_frequency <- if (!is.null(sample_frequency) && length(sample_frequency) == 1) {
      rep(sample_frequency, nrow(data))
    } else {
      NA
    }
  }
  
  # Expected columns
  columns <- c('datetime', 'sample_measurement_aqs', 'sample_measurement_envista',
               'units_of_measure_best', 'qualifier_best', 'simple_qual_best',
               'data_source', 'poc', 'method_code', 'method_type',
               'latitude', 'longitude', 'sample_measurement_best',
               'site', 'sample_frequency')
  
  if (nrow(data) > 0) {
    data <- dplyr::select(data, dplyr::all_of(columns))
  } else {
    data <- as.data.frame(matrix(ncol = length(columns), nrow = 1))
    names(data) <- columns
  }
  
  return(list(data = data, frequency = sample_frequency))
}

########################################################################################################################
# 
########################################################################################################################

blankDatastream <- function() {
  columns <- c('datetime', 'sample_measurement_aqs', 'sample_measurement_envista',
               'units_of_measure_best', 'qualifier_best', 'simple_qual_best',
               'data_source', 'poc', 'method_code', 'method_type',
               'latitude', 'longitude', 'sample_measurement_best', 'site')
  df <- as.data.frame(matrix(ncol = length(columns), nrow = 1))
  names(df) <- columns
  return(df)
}

########################################################################################################################
# Make requests to AQS & Envista APIs
########################################################################################################################

compileAirQualityData <- function(site_pollutant_meta, request_type = "aqs_envista", is_pm25 = is_pm25) {
  all_site_data          <- NULL
  all_site_hourly        <- NULL
  hourly_merged_data     <- NULL
  non_hourly_merged_data <- NULL
  
  sampling_frequencies_all <- character()
  
  if (isTRUE(is_pm25)) {
    raw_aqs_data <- NULL
    
    # First loop: Download raw AQS data for all sites
    for (i_site in seq_len(nrow(site_pollutant_meta$meta))) {
      site_name <- site_pollutant_meta$meta$site[i_site]

      # Skip AQS fetch for sensors
      if (tolower(site_pollutant_meta$meta$envista_name[i_site]) == "pm2.5 est sensor") {
        message("Skipping AQS fetch for sensor site: ", site_name)
        next
      }
      
      message("Processing site: ", site_name)
      
      subdata <- tryCatch(
        getAqsDataBySite(site_pollutant_meta, i_site),
        error = function(e) NULL
      )
      
      if (!is.null(subdata)) {
        subdata$site <- site_name
        subdata$method_code <- site_pollutant_meta$meta$aqs_method_code[i_site]
        raw_aqs_data <- if (is.null(raw_aqs_data)) subdata else bind_rows(raw_aqs_data, subdata)
      }
    }

    if (!is.null(raw_aqs_data)) {
      # --- AQS-driven part ---
      raw_aqs_data$sample_frequency <- tolower(trimws(as.character(raw_aqs_data$sample_frequency)))
      detected_sampling_frequency   <- unique(raw_aqs_data$sample_frequency)
      sampling_frequencies_all      <- union(sampling_frequencies_all, detected_sampling_frequency)
      
      hourly_like_freqs <- c("hourly", "daily: 24 - 1 hr samples -pams")
      non_hourly_freqs  <- setdiff(detected_sampling_frequency, hourly_like_freqs)
      
      non_hourly_data   <- raw_aqs_data %>% filter(sample_frequency %in% non_hourly_freqs)
      
      non_hourly_merged_data <- NULL
      if (nrow(non_hourly_data) > 0) {
        message("Merging non-hourly AQS data globally...")
        non_hourly_merged_data   <- chooseDatastream(non_hourly_data, "aqs_only") %>% purrr::pluck("data")
        sampling_frequencies_all <- union(sampling_frequencies_all, non_hourly_freqs)
      }
    } else {
      hourly_like_freqs <- c("hourly", "daily: 24 - 1 hr samples -pams")  # still define
      non_hourly_merged_data <- NULL
    }

    # Site-by-site hourly processing
    all_site_hourly <- NULL
    for (i_site in seq_len(nrow(site_pollutant_meta$meta))) {
      site_name <- site_pollutant_meta$meta$site[i_site]
      
      # Skip AQS for sensor
      is_sensor_site <- tolower(site_pollutant_meta$meta$envista_name[i_site]) == "pm2.5 est sensor"
      site_data <- if (!is.null(raw_aqs_data)) raw_aqs_data %>% filter(site == site_name) else NULL
      hourly_aqs_data <- if (!is.null(site_data)) site_data %>% filter(sample_frequency %in% hourly_like_freqs) else NULL
      has_hourly_aqs  <- !is.null(hourly_aqs_data) && nrow(hourly_aqs_data) > 0
      
      envista_data <- tryCatch(
        getEnvistaDataBySite(i_site, site_pollutant_meta),
        error = function(e) {
          message("Envista fetch failed for site: ", site_name)
          return(NULL)
        }
      )

      has_envista <- is.data.frame(envista_data) &&
        nrow(envista_data) > 0 
      # && all(c("datetime", "sample_measurement") %in% names(envista_data))

      try({
        if (has_hourly_aqs && has_envista) {
          print(paste("Scenario 1 - merging both for", site_name))
          hourly_merged_data <- homogenizeDataStreams(hourly_aqs_data, envista_data)
        } else if (has_hourly_aqs) {
          print(paste("Scenario 2 - AQS only for", site_name))
          hourly_merged_data <- chooseDatastream(hourly_aqs_data, "aqs_only") %>% pluck("data")
        } else if (has_envista) {
          print(paste("Scenario 3 - Envista only for", site_name))
          hourly_merged_data <- tryCatch({
            chooseDatastream(envista_data, "envista_only") %>% pluck("data")
          }, error = function(e) {
            message("chooseDatastream failed for Envista site: ", site_name, " — ", e$message)
            NULL
          })
        # } else if (is_sensor_site) {
        #   print(paste("No Envista data for sensor site", site_name))
        #   hourly_merged_data <- NULL  # or fallback logic
        } else {
          message("No AQS or Envista data for site: ", site_name)
          next
        }
      }, silent = TRUE)
      
      if (!is.null(hourly_merged_data) && nrow(hourly_merged_data) > 0) {
        all_site_hourly <- if (is.null(all_site_hourly)) hourly_merged_data else bind_rows(all_site_hourly, hourly_merged_data)
      }
    }
    # Assign all_site_hourly to all_site_data if not already set
    # Combine hourly and non-hourly data if available
    if (!is.null(all_site_hourly) && !is.null(non_hourly_merged_data)) {
      all_site_data <- bind_rows(all_site_hourly, non_hourly_merged_data)
    } else if (!is.null(all_site_hourly)) {
      all_site_data <- all_site_hourly
    } else if (!is.null(non_hourly_merged_data)) {
      all_site_data <- non_hourly_merged_data
    } else {
      all_site_data <- NULL
    }
  } else {
    # Non-PM2.5 path
    message("Processing non-PM2.5 data...")
    for (i_site in seq_len(nrow(site_pollutant_meta$meta))) {
      site_name <- site_pollutant_meta$meta$site[i_site]
      
      hourly_aqs_data <- tryCatch(
        getAqsDataBySite(site_pollutant_meta, i_site),
        error = function(e) {
          message("AQS fetch failed for site: ", site_name)
          return(NULL)
        }
      )
      
      envista_data <- tryCatch(
        getEnvistaDataBySite(i_site, site_pollutant_meta),
        error = function(e) {
          message("Envista fetch failed for site: ", site_name)
          return(NULL)
        }
      )

      has_aqs     <- !is.null(hourly_aqs_data) && nrow(hourly_aqs_data) > 0
      has_envista <- !is.null(envista_data) && nrow(envista_data) > 0
      
      merged_data <- NULL
      if (has_aqs && has_envista) {
        merged_data <- homogenizeDataStreams(hourly_aqs_data, envista_data)
      } else if (has_aqs) {
        merged_data <- chooseDatastream(hourly_aqs_data, "aqs_only") %>% purrr::pluck("data")
      } else if (has_envista) {
        merged_data <- chooseDatastream(envista_data, "envista_only") %>% purrr::pluck("data")
      } else {
        message("No AQS or Envista data available for site: ", site_name)
        next
      }
      merged_data$site <- site_name
      all_site_data <- if (is.null(all_site_data)) merged_data else bind_rows(all_site_data, merged_data)
    }
  }
  # Step 5: Clean sample_frequency
  all_site_data <- all_site_data %>%
    mutate(
      sample_frequency = tolower(trimws(as.character(
        if ("sample_frequency" %in% names(.)) sample_frequency else rep("unknown", nrow(.))
      )))
    )
  
  # all_site_data$sample_frequency <- tolower(trimws(as.character(all_site_data$sample_frequency)))
  unique_sampling_freq <- unique(all_site_data$sample_frequency)
  
  # Step 6: Detect hourly-like
  hourly_like_freqs <- c("hourly", "daily: 24 - 1 hr samples -pams")
  has_hourly        <- any(unique_sampling_freq %in% hourly_like_freqs)
  
  # Step 7: Return
  return(list(
    data = all_site_data,
    sampling_frequency = sampling_frequencies_all,
    has_hourly = any(sampling_frequencies_all %in% c("hourly", "daily: 24 - 1 hr samples -pams")),
    hourly_like_freq = c("hourly", "daily: 24 - 1 hr samples -pams")
  ))
}