
# Main function to extract HMS smoke levels by date and site
library(sf)
library(maps)
library(dplyr)
library(ggplot2)

# Main function to extract HMS smoke levels by date and site
addHmsLevelsDaily <- function(daily_pm25) {
  states <- st_as_sf(map("state", plot = FALSE, fill = TRUE))
  state_sf_transformed <- states %>% filter(ID == "oregon")
  states <- subset(states, !grepl("oregon", states$ID))
  
  light_smoke <- medium_smoke <- heavy_smoke <- NULL
  HMS_list <- NULL
  
  date_index <- which(
    daily_pm25$month %in% 6:9 |
      (daily_pm25$month == 10 & daily_pm25$day2foc <= 25)
  )
  
  dates_filtered <- unique(daily_pm25$date[date_index])
  
  for (index in seq_along(dates_filtered)) {
    focdate <- dates_filtered[index]
    sub_dailypm25 <- daily_pm25 %>% filter(date == focdate) %>% 
      select(c('date', 'site', 'longitude', 'latitude'))
    
    url <- paste0("https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/KML/",
                  format(focdate, "%Y/%m/hms_smoke"), format(focdate, "%Y%m%d"), ".kml")
    
    layers_info <- tryCatch({ st_layers(url) }, error = function(e) NULL)
    if (!is.null(layers_info)) {
      for (layer in layers_info$name) {
        all_data <- readHmsLayer(focdate, url, layer, state_sf_transformed, sub_dailypm25)
        
        if (!is.null(all_data) && nrow(all_data) > 0) {
          if (layer == "Smoke (Light)") {
            light_smoke <- bind_rows(light_smoke, all_data)
          } else if (layer == "Smoke (Medium)") {
            medium_smoke <- bind_rows(medium_smoke, all_data)
          } else if (layer == "Smoke (Heavy)") {
            heavy_smoke <- bind_rows(heavy_smoke, all_data)
          }
        }
      }
    }
    
    for (site in sub_dailypm25$site) {
      smoke_level <- c()
      if (!is.null(light_smoke) && site %in% light_smoke$site)   smoke_level <- c(smoke_level, "Smoke (Light)")
      if (!is.null(medium_smoke) && site %in% medium_smoke$site) smoke_level <- c(smoke_level, "Smoke (Medium)")
      if (!is.null(heavy_smoke) && site %in% heavy_smoke$site)   smoke_level <- c(smoke_level, "Smoke (Heavy)")
      
      HMS <- data.frame(
        date = focdate,
        site = site,
        smoke_level = ifelse(length(smoke_level) > 0, paste(smoke_level, collapse = ", "), NA),
        stringsAsFactors = FALSE
      )
      
      HMS_list <- bind_rows(HMS_list, HMS)
    }
  }
  
  HMS_list <- HMS_list %>% 
    mutate(date = as.Date(date)) %>%
    filter(!is.na(smoke_level)) %>%
    distinct(site, date, .keep_all = TRUE)
  
  return(HMS_list)
}


# Supporting KML reader function
readHmsLayer <- function(focdate, url, layer, state_sf_transformed, sub_dailypm25) {
  tryCatch({
    kml_data <- tryCatch({
      sf::st_read(url, layer = layer, quiet = TRUE)
    }, error = function(e) {
      warning(paste("Error reading KML layer:", layer, "-", e$message))
      return(NULL)
    })
    
    if (is.null(kml_data) || nrow(kml_data) == 0) return(NULL)
    
    kml_data <- sf::st_make_valid(kml_data)
    state_trans <- sf::st_transform(state_sf_transformed, sf::st_crs(kml_data))
    
    state_smoke <- tryCatch({
      sf::st_intersection(kml_data, state_trans)
    }, error = function(e) {
      buffered <- sf::st_buffer(state_trans, dist = 0.01)
      tryCatch({
        sf::st_intersection(kml_data, buffered)
      }, error = function(e2) {
        warning(paste("Buffered intersection failed:", e2$message))
        return(NULL)
      })
    })
    
    if (is.null(state_smoke) || nrow(state_smoke) == 0) return(NULL)
    
    sites_sf <- sf::st_as_sf(sub_dailypm25, coords = c("longitude", "latitude"), crs = 4326)
    sites_trans <- sf::st_transform(sites_sf, sf::st_crs(state_smoke))
    
    matched_sites <- tryCatch({
      sf::st_intersection(state_smoke, sites_trans)
    }, error = function(e) {
      warning(paste("Intersection error:", e$message))
      return(NULL)
    })
    
    if (is.null(matched_sites) || nrow(matched_sites) == 0) return(NULL)
    
    matched_sites <- matched_sites %>% dplyr::distinct(site, .keep_all = TRUE)
    return(matched_sites)
    
  }, error = function(e) {
    warning(paste("readHmsLayer failed for", focdate, "layer:", layer, "-", e$message))
    return(NULL)
  })
}


