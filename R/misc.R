# points <- geolocalizado_nao_vazio
# res <- 9
calcular_h3 <- function(points, res) {
  corte_grupos <- cut(1:nrow(points), breaks = getOption("TARGETS_N_CORES"))
  indices_grupos <- split(1:nrow(points), corte_grupos)
  
  points <- select(points, geom)
  points <- sf::st_coordinates(points)
  
  future::plan(future.callr::callr, workers = getOption("TARGETS_N_CORES"))
  
  ids_h3 <- furrr::future_map(
    indices_grupos,
    function(i) {
      suppressMessages(ids <- h3jsr::point_to_cell(points[i, ], res = res))
    },
    .options = furrr::furrr_options(seed = TRUE, globals = c("points", "res"))
  )
  
  future::plan(future::sequential)
  
  ids_h3 <- unlist(ids_h3, use.names = FALSE)
  
  return(ids_h3)
}


calcular_indices_h3 <- function(geolocalizado,
                                workers = getOption("TARGETS_N_CORES")) {
  geoms_vazias <- sf::st_is_empty(geolocalizado)
  geolocalizado_nao_vazio <- geolocalizado[!geoms_vazias, ]
  geolocalizado_vazio <- geolocalizado[geoms_vazias, ]
  
  n_linhas <- nrow(geolocalizado_nao_vazio)
  
  if (workers == 1) {
    indices_grupos <- 1:n_linhas
  } else {
    corte_grupos <- cut(1:n_linhas, breaks = workers)
    indices_grupos <- split(1:n_linhas, corte_grupos)
  }
  
  coords_nao_vazias <- select(geolocalizado_nao_vazio, geom)
  coords_nao_vazias <- sf::st_coordinates(coords_nao_vazias)
  
  if (workers > 1) future::plan(future.callr::callr, workers = workers)
  
  h3_res9 <- calcular_h3_future_fora(coords_nao_vazias, indices_grupos, res = 9)
  h3_res8 <- calcular_h3_future_fora(coords_nao_vazias, indices_grupos, res = 8)
  h3_res7 <- calcular_h3_future_fora(coords_nao_vazias, indices_grupos, res = 7)
  
  future::plan(future::sequential)
  
  geoloc_tratado <- rbind(geolocalizado_nao_vazio, geolocalizado_vazio)
  geoloc_tratado <- cbind(
    data.table::as.data.table(sf::st_drop_geometry(geoloc_tratado)),
    data.table::as.data.table(sf::st_coordinates(geoloc_tratado))
  )
  geoloc_tratado[
    ,
    `:=`(
      h3_res9 = c(h3_res9, rep(NA_character_, times = nrow(geolocalizado_vazio))),
      h3_res8 = c(h3_res8, rep(NA_character_, times = nrow(geolocalizado_vazio))),
      h3_res7 = c(h3_res7, rep(NA_character_, times = nrow(geolocalizado_vazio)))
    )
  ]
  data.table::setnames(geoloc_tratado, old = c("X", "Y"), new = c("lon", "lat"))
  
  return(geoloc_tratado[])
}


calcular_h3_future_fora <- function(coords_nao_vazias, indices_grupos, res) {
  ids_h3 <- furrr::future_map(
    indices_grupos,
    function(i) {
      suppressMessages(
        ids <- h3jsr::point_to_cell(coords_nao_vazias[i, ], res = res)
      )
    },
    .options = furrr::furrr_options(
      seed = TRUE,
      globals = c("coords_nao_vazias", "res")
    )
  )
  
  ids_h3 <- unlist(ids_h3, use.names = FALSE)
  
  return(ids_h3)
}
