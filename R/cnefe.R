gerar_basenames_cnefe <- function() {
  basenames <- paste0(
    enderecopadrao::codigos_estados$codigo_estado,
    "_",
    enderecopadrao::codigos_estados$abrev_estado
  )
  
  return(basenames)
}

# ano <- tar_read(anos_cnefe)[1]
# basename <- tar_read(basenames_cnefe)[1]
gerar_arquivos_cnefe <- function(ano, basename) {
  caminho <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    "CNEFE/parquet",
    ano,
    "arquivos",
    paste0(basename, ".parquet")
  )
  
  return(caminho)
}

# arquivo <- tar_read(arquivos_cnefe, branches = 1)
# ano <- tar_read(anos_cnefe)[1]
# basename <- tar_read(basenames_cnefe)[1]
tratar_cnefe <- function(arquivo, ano, basename) {
  colunas_a_manter <- c(
    "code_address", # identificador
    "code_state", # estado
    "code_muni", # municipio
    "desc_localidade", # bairro, povoado, vila, etc
    "nom_tipo_seglogr", # tipo de logradouro
    "nom_titulo_seglogr", # titulo (e.g. general, papa, santa, etc)
    "nom_seglogr", # logradouro
    "num_andress", # numero
    "dsc_modificador", # modificador do numero
    "cep" # cep
  )
  
  original <- arrow::open_dataset(arquivo)
  original <- select(original, all_of(colunas_a_manter))
  original <- data.table::setDT(collect(original))
  
  padronizado <- data.table::copy(original)
  
  padronizado[, num_andress := as.character(num_andress)]
  padronizado[num_andress == "0", num_andress := "S/N"]
  padronizado[, dsc_modificador := NULL]
  
  padronizado[
    ,
    logradouro := data.table::fifelse(
      nom_titulo_seglogr == "",
      nom_seglogr,
      paste(nom_titulo_seglogr, nom_seglogr)
    )
  ]
  padronizado[, c("nom_titulo_seglogr", "nom_seglogr") := NULL]
  
  padronizado[, estado := enderecopadrao::padronizar_estados(code_state)]
  padronizado[, code_state := NULL]
  
  padronizado[, municipio := enderecopadrao::padronizar_municipios(code_muni)]
  padronizado[, code_muni := NULL]
  
  padronizado[, cep := enderecopadrao::padronizar_ceps(cep)]
  
  padronizado[
    ,
    logradouro_completo := paste(nom_tipo_seglogr, logradouro, num_andress)
  ]
  padronizado[, c("nom_tipo_seglogr", "logradouro", "num_andress") := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/cnefe"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  dir_tratado <- file.path(dir_geocode, paste0("tratado_", ano))
  if (!dir.exists(dir_tratado)) dir.create(dir_tratado)
  
  destino <- file.path(dir_tratado, paste0(basename, ".parquet"))
  arrow::write_parquet(padronizado, sink = destino)
  
  return(destino)
}

# arquivo <- tar_read(cnefe_tratado, branches = 1)
# ano <- tar_read(anos_cnefe)[1]
# basename <- tar_read(basenames_cnefe)[1]
geolocalizar_cnefe <- function(arquivo, ano, basename) {
  # setando condaenv e importando arcpy pra evitar problemas com o arrow, que
  # tambem usa o reticulate e causa conflitos com o geocodepro
  reticulate::use_condaenv(
    "C://Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3"
  )
  invisible(reticulate::import("arcpy"))
  
  padronizado <- arrow::read_parquet(arquivo)
  
  localizador <- paste0(
    "C://StreetMap/NewLocators/BRA_",
    getOption("TARGETS_N_CORES"),
    "/BRA.loc"
  )
  campos_do_endereco <- geocodepro::address_fields_const(
    Address_or_Place = "logradouro_completo",
    Neighborhood = "desc_localidade",
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
  
  geolocalizado <- calcular_indices_h3(
    geolocalizado,
    workers = min(15, getOption("TARGETS_N_CORES"))
  )
  
  data.table::setcolorder(geolocalizado, "code_address")
  
  geolocalizado[, (campos_do_endereco) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/cnefe"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  dir_geolocalizado <- file.path(dir_geocode, paste0("geolocalizado_", ano))
  if (!dir.exists(dir_geolocalizado)) dir.create(dir_geolocalizado)
  
  destino <- file.path(dir_geolocalizado, paste0(basename, ".parquet"))
  arrow::write_parquet(geolocalizado, sink = destino)
  
  return(destino)
}