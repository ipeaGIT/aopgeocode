options(
  TARGETS_VERBOSE = TRUE,
  TARGETS_N_CORES = 30 # ver opcoes disponiveis em C:\StreetMap\NewLocators
)

suppressPackageStartupMessages({
  library(targets)
  library(dplyr)
  loadNamespace("sf")
})

source("R/preprocess_cnes.R", encoding = "UTF-8")
source("R/geocode.R", encoding = "UTF-8")
source("R/cadunico.R", encoding = "UTF-8")

if (!interactive()) {
  future::plan(future.callr::callr)
}

list(
  tar_target(anos_censo_escolar, 2011:2020),
  tar_target(
    arquivos_censo_escolar,
    file.path(
      Sys.getenv("RESTRICTED_DATA_PATH"),
      paste0("RAIS/csv/estab", anos_rais, ".csv")
    ),
    format = "file_fast"
  ),
  
  tar_target(anos_rais, 1985:2021),
  tar_target(
    arquivos_rais,
    file.path(
      Sys.getenv("RESTRICTED_DATA_PATH"),
      paste0("RAIS/csv/estab", anos_rais, ".csv")
    ),
    format = "file_fast"
  ),
  
  tar_target(anos_cadunico, 2011:2022),
  tar_target(
    arquivos_cadunico,
    file.path(
      Sys.getenv("RESTRICTED_DATA_PATH"),
      paste0("CADASTRO_UNICO/parquet/cad_familia_12", anos_cadunico, ".parquet")
    ),
    format = "file_fast"
  ),
  tar_target(
    cadunico_tratado,
    tratar_cadunico(arquivos_cadunico, anos_cadunico),
    pattern = map(arquivos_cadunico, anos_cadunico),
    format = "file_fast"
  ),
  tar_target(
    cadunico_geolocalizado,
    geolocalizar_cadunico(cadunico_tratado, anos_cadunico),
    pattern = map(cadunico_tratado, anos_cadunico),
    format = "file_fast"
  )
  # 
  # tar_target(
  #   cnes_path,
  #   "../../data-raw/cnes/2022/BANCO_ESTAB_IPEA_COMP_08_2022_DT_25_10_2023.xlsx",
  #   format = "file_fast"
  # ),
  # tar_target(preprocessed_cnes, preprocess_cnes(cnes_path)),
  # tar_target(geocoded_cnes, geocode_cnes(preprocessed_cnes))
)
