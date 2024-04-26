options(
  TARGETS_VERBOSE = TRUE,
  TARGETS_N_CORES = 30 # ver opcoes disponiveis em C:\StreetMap\NewLocators
)
data.table::setDTthreads(getOption("TARGETS_N_CORES"))

suppressPackageStartupMessages({
  library(targets)
  library(dplyr)
  loadNamespace("sf")
})

source("R/preprocess_cnes.R", encoding = "UTF-8")
source("R/geocode.R", encoding = "UTF-8")
source("R/cadunico.R", encoding = "UTF-8")
source("R/rais.R", encoding = "UTF-8")
source("R/cpf.R", encoding = "UTF-8")
source("R/censo_escolar.R", encoding = "UTF-8")
source("R/misc.R", encoding = "UTF-8")

if (!interactive()) {
  future::plan(future.callr::callr)
}

list(
  # tar_target(anos_censo_escolar, 2011:2020),
  # tar_target(
  #   arquivos_censo_escolar,
  #   file.path(
  #     Sys.getenv("RESTRICTED_DATA_PATH"),
  #     paste0("RAIS/csv/estab", anos_rais, ".csv")
  #   ),
  #   format = "file_fast"
  # ),
  
  tar_target(anos_rais, 2002:2021),
  tar_target(
    arquivos_rais,
    gerar_arquivos_rais(anos_rais),
    format = "file_fast"
  ),
  tar_target(
    rais_tratada,
    tratar_rais(arquivos_rais, anos_rais),
    pattern = map(arquivos_rais, anos_rais),
    format = "file_fast"
  ),
  tar_target(
    rais_geolocalizada,
    geolocalizar_rais(rais_tratada, anos_rais),
    pattern = map(rais_tratada, anos_rais),
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
  ),
  
  tar_target(n_lotes_cpf, 50),
  tar_target(
    arquivos_cpf,
    file.path(
      Sys.getenv("RESTRICTED_DATA_PATH"),
      "B_CADASTRO/CPF/20230816_cpf.csv"
    ),
    format = "file_fast"
  ),
  tar_target(
    n_linhas_lote_cpf,
    calcular_n_linhas_lote_cpf(arquivos_cpf, n_lotes_cpf)
  ),
  tar_target(
    n_linhas_a_pular_cpf,
    calcular_n_linhas_a_pular_cpf(n_linhas_lote_cpf)
  ),
  tar_target(
    cpf_tratado,
    tratar_cpf(arquivos_cpf, n_linhas_lote_cpf, n_linhas_a_pular_cpf),
    pattern = map(n_linhas_lote_cpf, n_linhas_a_pular_cpf),
    format = "file_fast"
  )
)
