# locations <- tar_read(preprocessed_cnes)
geocode_cnes <- function(locations) {
  reticulate::use_condaenv(
    "C://Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3"
  )
  
  address_fields <- geocodepro::address_fields_const(
    Address_or_Place = "LOGRADOURO_COMPLETO",
    Neighborhood = "BAIRRO",
    City = "MUNICIPIO",
    State = "UF",
    ZIP = "CEP"
  )
  
  geocoded_cnes <- geocode_addresses(
    locations,
    locator = "../../data-raw/locators/BRA/BRA.loc",
    address_fields = address_fields
  )
  
  return(geocoded_cnes)
}

geocode_addresses <- function(locations, locator, address_fields) {
  geocodepro:::assert_address_fields(address_fields, locations)
  
  match_cols <- c(
    "Status",
    "Score",
    "Match_type",
    "Match_addr",
    "Addr_type",
    "Lon",
    "Lat"
  )
  
  # only geocode addresses not yet geocoded - i.e. not in the geocode cache
  
  cache_path <- "../../data-raw/geocode_cache/cache.rds"
  
  if (file.exists(cache_path)) {
    cache <- readRDS(cache_path)
    locations <- data.table::as.data.table(locations)
    
    addresses_cache_lookup <- merge(
      locations,
      cache,
      by.x = address_fields,
      by.y = names(address_fields),
      all.x = TRUE
    )
    
    relevant_cached_cols <- c(names(locations), match_cols)
    cached_addresses <- addresses_cache_lookup[!is.na(Score)]
    cached_addresses[
      ,
      setdiff(names(cached_addresses), relevant_cached_cols) := NULL
    ]
    
    not_cached_addresses <- addresses_cache_lookup[is.na(Score)]
    not_cached_addresses[
      ,
      setdiff(names(addresses_cache_lookup), names(locations)) := NULL
    ]
    
    if (nrow(not_cached_addresses) == 0) {
      geocoded_not_cached <- empty_geocoded(address_fields)
    } else {
      geocoded_not_cached <- geocodepro::geocode(
        not_cached_addresses,
        locator = locator,
        address_fields = address_fields
      )
      geocoded_not_cached <- point_to_coords(geocoded_not_cached)
      append_to_geocode_cache(geocoded_not_cached, address_fields)
    }
    
    geocoded_data <- rbind(
      cached_addresses,
      geocoded_not_cached
    )
  } else {
    geocoded_data <- geocodepro::geocode(
      locations,
      locator = locator,
      address_fields = address_fields
    )
    geocoded_data <- point_to_coords(geocoded_data)
    append_to_geocode_cache(geocoded_data, address_fields)
  }
  
  data.table::setcolorder(geocoded_data, c(names(locations), match_cols))
  
  return(geocoded_data)
}

empty_geocoded <- function(address_fields) {
  relevant_cols <- c(
    address_fields,
    "Status",
    "Score",
    "Match_type",
    "Match_addr",
    "Addr_type",
    "Lon",
    "Lat"
  )
  empty_list <- lapply(1:length(relevant_cols), function(i) character(0))
  empty_table <- data.table::setDT(empty_list)
  names(empty_table) <- relevant_cols
  empty_table[, c("Score", "Lon", "Lat") := numeric(0)]
  
  return(empty_table[])
}

append_to_geocode_cache <- function(geocoded_addresses, address_fields) {
  cache_path <- "../../data-raw/geocode_cache/cache.rds"
  if (file.exists(cache_path)) cache <- readRDS(cache_path)
  
  addresses_in_cache_structure <- to_cache_structure(
    geocoded_addresses,
    address_fields,
    lookup_only = FALSE
  )
  
  if (!exists("cache")) {
    cache <- addresses_in_cache_structure
   
    suppressWarnings(
      cache_dir <- normalizePath(file.path(cache_path, ".."))
    )
    if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  } else {
    cache <- rbind(cache, addresses_in_cache_structure)
  }
  
  cache <- unique(cache)
  
  saveRDS(cache, cache_path)
  
  return(invisible(cache_path))
}

to_cache_structure <- function(x, address_fields, lookup_only) {
  all_address_fields <- c(
    "Address_or_Place",
    "Address2",
    "Address3",
    "Neighborhood",
    "City",
    "County",
    "State",
    "ZIP",
    "ZIP4",
    "Country"
  )
  
  structure_fields <- if (lookup_only) {
    all_address_fields
  } else {
    c(
      all_address_fields,
      "Status",
      "Score",
      "Match_type",
      "Match_addr",
      "Addr_type",
      "Lon",
      "Lat"
    )
  }
  
  empty_list <- lapply(1:length(structure_fields), function(i) character(0))
  empty_table <- data.table::setDT(empty_list)
  names(empty_table) <- structure_fields
  
  if (!lookup_only) empty_table[, c("Score", "Lon", "Lat") := numeric(0)]
  
  x <- data.table::as.data.table(x)
  x <- data.table::setnames(
    x,
    old = address_fields,
    new = names(address_fields)
  )
  if (!is.null(x$ZIP)) x[, ZIP := as.character(ZIP)]
  
  cache_structured_x <- rbind(empty_table, x, fill = TRUE)
  
  return(cache_structured_x)
}

point_to_coords <- function(geocoded_addresses) {
  coords <- data.table::as.data.table(sf::st_coordinates(geocoded_addresses))
  names(coords) <- c("Lon", "Lat")
  
  geocoded_addresses <- sf::st_drop_geometry(geocoded_addresses)
  geocoded_addresses <- data.table::setDT(cbind(geocoded_addresses, coords))
  
  return(geocoded_addresses)
}
