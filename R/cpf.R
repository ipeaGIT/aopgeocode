# arquivo <- tar_read(arquivos_cpf)
# n_lotes <- tar_read(n_lotes_cpf)
calcular_n_linhas_lote_cpf <- function(arquivo, n_lotes) {
  cpf <- data.table::fread(arquivo, select = list(character = "codSexo"))
  
  n_linhas_cpf <- nrow(cpf)
  aprox_n_linhas_lote <- ceiling(n_linhas_cpf / n_lotes)
  
  n_linhas_lote <- rep(aprox_n_linhas_lote, times = n_lotes - 1)
  n_linhas_lote[n_lotes] <- n_linhas_cpf - sum(n_linhas_lote)
  
  return(n_linhas_lote)
}

# n_linhas_lote <- tar_read(n_linhas_lote_cpf)
calcular_n_linhas_a_pular_cpf <- function(n_linhas_lote) {
  n_linhas_a_pular <- cumsum(n_linhas_lote)
  n_linhas_a_pular <- c(0, n_linhas_a_pular)
  n_linhas_a_pular <- n_linhas_a_pular[-length(n_linhas_a_pular)]
  return(n_linhas_a_pular)
}

# arquivo <- tar_read(arquivos_cpf)
# n_linhas_lote <- tar_read(n_linhas_lote_cpf)[2]
# n_linhas_a_pular <- tar_read(n_linhas_a_pular_cpf)[2]
tratar_cpf <- function(cpf, n_linhas_lote, n_linhas_a_pular) {
  colunas_a_manter <- c(
    "cpf", # identificador
    "uf_dom", # estado
    "codmun_dom", # municipio
    "cep",
    "bairro",
    "tipoLogradouro",
    "logradouro",
    "nroLogradouro"
  )
  
  amostra <- data.table::fread(arquivo, na.strings = "", nrows = 1)
  posicao_colunas_desejadas <- which(names(amostra) %in% colunas_a_manter)

  original <- data.table::fread(
    arquivo,
    select = list(character = posicao_colunas_desejadas),
    col.names = colunas_a_manter,
    na.strings = "",
    nrows = n_linhas_lote,
    skip = n_linhas_a_pular + 1
  )
  
  padronizado <- enderecopadrao::padronizar_enderecos(
    original,
    campos_do_endereco = enderecopadrao::correspondencia_campos(
      logradouro = "logradouro",
      estado = "uf_dom",
      municipio = "codmun_dom",
      cep = "cep",
      bairro = "bairro",
      numero = "nroLogradouro"
    )
  )
  
  # padronizacao ainda nao coberta pelo {enderecopadrao}
  
  padronizado[tipoLogradouro == "OUTROS", tipoLogradouro := NA_character_]
  padronizado[logradouro == "X", logradouro := NA_character_]
  padronizado[bairro == "X", bairro := NA_character_]
  padronizado[numero == "X" | numero == "0", numero := NA_character_]
  
  padronizado[
    !(is.na(numero) | is.na(logradouro)),
    logradouro := paste(logradouro, numero)
  ]
  padronizado[!is.na(numero) & is.na(logradouro), logradouro := numero]
  padronizado[, numero := NULL]
  
  padronizado[, prim_palav_logr := stringr::word(logradouro, 1)]
  
  padronizado[
    ,
    logr_completo := data.table::fcase(
      prim_palav_logr == tipoLogradouro, logradouro,
      is.na(tipoLogradouro), logradouro,
      is.na(logradouro) & !is.na(tipoLogradouro), tipoLogradouro,
      !(is.na(tipoLogradouro) | is.na(logradouro)), paste(tipoLogradouro, logradouro),
      default = NA_character_
    )
  ]
  
  padronizado[, c("tipoLogradouro", "logradouro", "prim_palav_logr") := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/cpf"
  )
  if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
  
  dir_tratado <- file.path(dir_geocode, "tratado")
  if (!dir.exists(dir_tratado)) dir.create(dir_tratado)
  
  n_lote <- round((n_linhas_lote + n_linhas_a_pular) / n_linhas_lote)
  n_lote <- formatC(n_lote, width = 2, flag = "0")
  
  destino <- file.path(
    dir_tratado,
    paste0("cpf_tratado_lote_", n_lote, ".parquet")
  )
  arrow::write_parquet(padronizado, sink = destino)
  
  return(destino)
}