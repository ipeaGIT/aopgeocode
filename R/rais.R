# anos_rais <- tar_read(anos_rais)
gerar_arquivos_rais <- function(anos_rais) {
  arquivos <- purrr::map_chr(
    anos_rais,
    function(ano) {
      if (ano == 2008 || ano >= 2016) {
        file.path(
          Sys.getenv("RESTRICTED_DATA_PATH"),
          paste0("RAIS/csv/estab", ano, ".csv")
        )
      } else {
        file.path(
          Sys.getenv("RESTRICTED_DATA_PATH"),
          paste0("RAIS/parquet/estab", ano)
        )
      }
    }
  )
  
  return(arquivos)
}

# arquivo <- tar_read(arquivos_rais)[1]
# ano <- tar_read(anos_rais)[1]
tratar_rais <- function(arquivo, ano) {
  colunas_a_manter <- c(
    "id_estab", # identificador
    "uf", # estado
    "codemun", # municipio
    "endereco", # logradouro
    "cep" # cep
  )
  if (ano == 2008 || ano >= 2011) {
    colunas_a_manter <- c(setdiff(colunas_a_manter, "endereco"), "logradouro") # campo logradouro muda de nome a partir de 2011
  }
  if (ano >= 2011)  colunas_a_manter <- c(colunas_a_manter, "bairro")
  
  if (ano == 2008 || ano >= 2016) {
    original <- data.table::fread(
      arquivo,
      select = list(character = colunas_a_manter),
      na.strings = ""
    )
  } else {
    original <- arrow::open_dataset(arquivo)
    original <- select(original, all_of(colunas_a_manter))
    original <- data.table::setDT(collect(original))
  }
  
  padronizado <- enderecopadrao::padronizar_enderecos(
    original,
    campos_do_endereco = enderecopadrao::correspondencia_campos(
      logradouro = ifelse(ano == 2008 | ano >= 2011, "logradouro", "endereco"),
      estado = "uf",
      municipio = "codemun",
      cep = "cep",
      bairro = if (ano >= 2011) "bairro" else NULL
    )
  )
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/rais"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  destino <- file.path(
    dir_geocode,
    paste0("rais_padronizada_", ano, ".parquet")
  )
  arrow::write_parquet(padronizado, sink = destino)
  
  return(destino)
}

# arquivo <- tar_read(rais_tratada, branches = 1)
# ano <- tar_read(anos_rais)[1]
geolocalizar_rais <- function(arquivo, ano) {
  # condaenv setado no .Rprofile pra que nao houvesse problemas ao usar o arrow,
  # que tambem usa o reticulate e causa problemas no geocodepro
  
  padronizado <- arrow::read_parquet(arquivo)
  
  localizador <- paste0(
    "C://StreetMap/NewLocators/BRA_",
    getOption("TARGETS_N_CORES"),
    "/BRA.loc"
  )
  campos_do_endereco <- geocodepro::address_fields_const(
    Address_or_Place = "logradouro",
    City = "municipio",
    State = "estado",
    ZIP = "cep",
    Neighborhood = if (ano >= 2011) "bairro" else NULL
  )
  
  geolocalizado <- geocodepro::geocode(
    padronizado,
    locator = localizador,
    address_fields = campos_do_endereco,
    verbose = getOption("TARGETS_VERBOSE")
  )
  
  geoms_vazias <- sf::st_is_empty(geolocalizado)
  geolocalizado_nao_vazio <- geolocalizado[!geoms_vazias, ]
  geolocalizado_vazio <- geolocalizado[geoms_vazias, ]
  
  h3_res9 <- calcular_h3(geolocalizado_nao_vazio, res = 9)
  h3_res8 <- calcular_h3(geolocalizado_nao_vazio, res = 8)
  h3_res7 <- calcular_h3(geolocalizado_nao_vazio, res = 7)
  
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
  data.table::setcolorder(geoloc_tratado, "id_estab")
  
  geoloc_tratado[, (campos_do_endereco) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/rais"
  )
  
  destino <- file.path(
    dir_geocode,
    paste0("rais_geolocalizada_", ano, ".parquet")
  )
  arrow::write_parquet(geoloc_tratado, sink = destino)
  
  return(destino)
}