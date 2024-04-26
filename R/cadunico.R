# arquivo <- tar_read(arquivos_cadunico)[1]
# ano <- tar_read(anos_cadunico)[1]
tratar_cadunico <- function(arquivo, ano) {
  colunas_a_manter <- c(
    "co_familiar_fam", # identificador
    "co_uf", # estado
    "cd_ibge_cadastro", # municipio
    "no_localidade_fam", # bairro, povoado, vila, etc
    "no_tip_logradouro_fam", # tipo de logradouro
    "no_tit_logradouro_fam", # titulo (e.g. general, papa, santa, etc)
    "no_logradouro_fam", # logradouro
    "nu_logradouro_fam", # numero
    "nu_cep_logradouro_fam" # cep
  )
  
  original <- arrow::open_dataset(arquivo)
  original <- select(original, all_of(colunas_a_manter))
  original <- data.table::setDT(collect(original))
  
  # tratamento não coverto pelo {enderecopadrao} ainda
  
  original[no_tip_logradouro_fam == "10A", no_tip_logradouro_fam := "RUA"]
  original[no_tit_logradouro_fam == "PIO", no_tit_logradouro_fam := NA_character_]
  
  padronizado <- enderecopadrao::padronizar_enderecos(
    original,
    campos_do_endereco = enderecopadrao::correspondencia_campos(
      logradouro = "no_logradouro_fam",
      numero = "nu_logradouro_fam",
      estado = "co_uf",
      municipio = "cd_ibge_cadastro",
      cep = "nu_cep_logradouro_fam",
      bairro = "no_localidade_fam"
    )
  )
  padronizado[
    !(is.na(numero) | is.na(logradouro)),
    logradouro := paste(logradouro, numero)
  ]
  padronizado[, numero := NULL]
  
  padronizado[
    ,
    `:=`(
      id_familia = original$co_familiar_fam,
      tipo_logradouro = original$no_tip_logradouro_fam,
      titulo_logradouro = original$no_tit_logradouro_fam,
      prim_palav_logr = stringr::word(logradouro, 1)
    )
  ]
  
  padronizado[
    ,
    logr_completo := data.table::fcase(
      prim_palav_logr == tipo_logradouro, logradouro,
      is.na(tipo_logradouro) & is.na(titulo_logradouro), logradouro,
      is.na(tipo_logradouro), paste(titulo_logradouro, logradouro),
      is.na(titulo_logradouro), paste(tipo_logradouro, logradouro),
      default = NA_character_
    )
  ]
  padronizado[
    is.na(logr_completo) & !is.na(logradouro),
    logr_completo := paste(tipo_logradouro, titulo_logradouro, logradouro)
  ]
  data.table::setcolorder(padronizado, "id_familia")
  
  colunas_a_remover <- c(
    "tipo_logradouro",
    "titulo_logradouro",
    "logradouro",
    "prim_palav_logr"
  )
  padronizado[, (colunas_a_remover) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/cadunico"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  destino <- file.path(
    dir_geocode,
    paste0("cad_padronizado_", ano, ".parquet")
  )
  arrow::write_parquet(padronizado, sink = destino)
  
  return(destino)
}

# arquivo <- tar_read(cadunico_tratado, branches = 1)
# ano <- tar_read(anos_cadunico)[1]
geolocalizar_cadunico <- function(arquivo, ano) {
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
    Neighborhood = "bairro",
    City = "municipio",
    State = "estado",
    ZIP = "cep"
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
  data.table::setcolorder(geoloc_tratado, "id_familia")
  
  geoloc_tratado[, (campos_do_endereco) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/cadunico"
  )
  
  destino <- file.path(
    dir_geocode,
    paste0("cad_geolocalizado_", ano, ".parquet")
  )
  arrow::write_parquet(geoloc_tratado, sink = destino)
  
  return(destino)
}