# standardize_cities

## adapt transit coverage list to standard ipea codes and join with other data to clusterize

# library(dplyr); library(purrr); library(sf); library(geobr)

standardize_cities <- function(pop_codes) {

  ## get standard names and ibge codes
  df <- pop_codes |> 
    pull(code_pop_unit) |> 
    purrr::map(
      \(x) geobr::lookup_muni(code_muni = x)
    ) |> 
    bind_rows() |> 
    select(code_muni, name_muni)
  
  df <- inner_join(df, pop_codes, by = c("code_muni" = "code_pop_unit"))
  
  
  ## create "population units" clusters, following https://github.com/ipeaGIT/aoppop/
  metro_areas <- geobr::read_metro_area() |> 
    st_drop_geometry() |> 
    filter(
      !(subdivision %in% c("Colar Metropolitano", "Área de Expansão Metropolitana",
                           "Entorno Metropolitano da RM Vale do Rio Cuiabá"))
    ) |> 
    select(code_muni, name_metro)
  
  urban_concentrations <- geobr::read_urban_concentrations() |> 
    st_drop_geometry() |> 
    select(code_urban_concentration, name_urban_concentration, code_muni)
  
  pop_arr <- geobr::read_pop_arrangements() |> 
    st_drop_geometry() |> 
    select(code_pop_arrangement, name_pop_arrangement, code_muni)

  df <- df |> 
    left_join(urban_concentrations) |> 
    left_join(pop_arr) |> 
    left_join(metro_areas)
  
  
  ## urban concentration when possible, otherwise pop arrangement
  df <- df |> 
    mutate(
      name_pop_unit = case_when(
        !is.na(name_urban_concentration) ~ name_urban_concentration,
        !is.na(name_pop_arrangement) ~ name_pop_arrangement
      ),
      code_pop_unit = case_when(
        !is.na(code_urban_concentration) ~ code_urban_concentration,
        !is.na(code_pop_arrangement) ~ code_pop_arrangement
      ),
      type_pop_unit = case_when(
        !is.na(name_urban_concentration) ~ "urban_concentration",
        !is.na(name_pop_arrangement) ~ "pop_arrangement",
      )
    ) |>
    filter(!is.na(code_pop_unit))
  
  
  ## tidy name following aopop's rule and concatenate with the code to create a label
  df$name_pop_unit <- treat_name(df)
  
  df <- df |> 
    mutate(label = paste(code_pop_unit, name_pop_unit, sep = "_"))
  
  
  ## unify everything
  df <- df |> 
    distinct(abbrev_pop_unit = code_aop, code_pop_unit, name_pop_unit, type_pop_unit, label) |> 
    arrange(code_pop_unit) 
  
  return(df)
  
}



# get_muni_list -------------------------------------------------------------------------------

get_muni_list <- function(pop_unit) {
  
  checkmate::assert_data_frame(pop_unit, max.rows = 1)
  
  pop_type <- pop_unit$type_pop_unit
  
  munis <- do.call(
    get(
      paste0("read_", pop_type, 
             ifelse(pop_type %in% c("urban_concentration", "pop_arrangement"), "s", "")), 
      envir = asNamespace("geobr")
    ), 
    list()
  ) 
  
  munis <- munis |> 
    sf::st_drop_geometry() |> 
    filter(code_urban_concentration == pop_unit$code_pop_unit) |> 
    select(name_muni, code_muni) |> 
    mutate(codemun = as.numeric(stringr::str_sub(code_muni, 1, 6)))
  return(munis)
}

