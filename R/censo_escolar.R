# anos <- tar_read(anos_censo_escolar)
gerar_arquivos_censo_escolar <- function(anos) {
  arquivos <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    paste0(
      "Censo_Escolar/csv/",
      anos,
      "/dados/microdados_ed_basica_",
      anos,
      ".csv"
    )
  )
  
  return(arquivos)
}

# arquivo <- tar_read(arquivos_censo_escolar)[1]
# ano <- tar_read(anos_censo_escolar)[1]
tratar_censo_escolar <- function(arquivo, ano) {
  colunas_a_manter <- c(
    "CO_ENTIDADE", # identificador
    "CO_UF", # estado
    "CO_MUNICIPIO", # municipio
    "DS_ENDERECO", # logradouro
    "NU_ENDERECO", # numero
    "NO_BAIRRO",
    "CO_CEP" # cep
  )
  
  original <- data.table::fread(
    arquivo,
    select = list(character = colunas_a_manter),
    na.strings = "",
    encoding = "Latin-1"
  )
  
  padronizado <- enderecopadrao::padronizar_enderecos(
    original,
    campos_do_endereco = enderecopadrao::correspondencia_campos(
      logradouro = "DS_ENDERECO",
      numero = "NU_ENDERECO",
      estado = "CO_UF",
      municipio = "CO_MUNICIPIO",
      cep = "CO_CEP",
      bairro = "NO_BAIRRO"
    )
  )
  
  padronizado[
    ,
    logr_completo := data.table::fcase(
      !(is.na(logradouro) | is.na(numero)), paste(logradouro, numero),
      is.na(numero), logradouro,
      is.na(logradouro), numero
    )
  ]
  padronizado[, c("logradouro", "numero") := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/censo_escolar"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  destino <- file.path(
    dir_geocode,
    paste0("censo_escolar_padronizado_", ano, ".parquet")
  )
  arrow::write_parquet(padronizado, sink = destino)
  
  return(destino)
}

# arquivo <- tar_read(censo_escolar_tratado, branches = 1)
# ano <- tar_read(anos_censo_escolar)[1]
geolocalizar_censo_escolar <- function(arquivo, ano) {
  # condaenv setado no .Rprofile pra que nao houvesse problemas ao usar o arrow,
  # que tambem usa o reticulate e causa problemas no geocodepro
  
  padronizado <- arrow::read_parquet(arquivo)
  
  localizador <- paste0(
    "C://StreetMap/NewLocators/BRA_",
    getOption("TARGETS_N_CORES"),
    "/BRA.loc"
  )
  campos_do_endereco <- geocodepro::address_fields_const(
    Address_or_Place = "logr_completo",
    City = "municipio",
    State = "estado",
    ZIP = "cep",
    Neighborhood = "bairro"
  )
  
  geolocalizado <- geocodepro::geocode(
    padronizado,
    locator = localizador,
    address_fields = campos_do_endereco,
    verbose = getOption("TARGETS_VERBOSE"),
    cache = "censo_escolar"
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
  data.table::setcolorder(geoloc_tratado, "CO_ENTIDADE")
  
  geoloc_tratado[, (campos_do_endereco) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/censo_escolar"
  )
  
  destino <- file.path(
    dir_geocode,
    paste0("censo_escolar_geolocalizado_", ano, ".parquet")
  )
  arrow::write_parquet(geoloc_tratado, sink = destino)
  
  return(destino)
}