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