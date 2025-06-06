

# gerar_arquivos_rais -------------------------------------------------------------------------

# anos_rais <- tar_read(anos_rais)

gerar_arquivos_rais <- function(anos_rais) {
  arquivos <- purrr::map_chr(
    anos_rais,
    function(ano) {
      file.path(
        Sys.getenv("RESTRICTED_DATA_PATH"), 
        paste0("RAIS/parquet/estabs/ano=", ano, "/estabs.parquet")
      )
    }
  )
  
  return(arquivos)
}



# tratar_rais ---------------------------------------------------------------------------------

#' padronizar endereços da rais
#' @param arquivo `string`, caminho do arquivo da rais bruta.
#' @param ano `numeric`, ano da rais
#' @param filtro_muni `vector` códigos de município opcional para filtrar a rais
#' @param salvar_target se `TRUE` (default), salva o arquivo em "_targets/objects"
#' @param destino caminho para salvar o arquivo. se NULL (default), salva na pasta específica, 
#'  desde que salvar_target = `FALSE`

tratar_rais <- function(arquivo, ano, filtro_muni = NULL, salvar_target = TRUE, destino = NULL) {
  
  colunas_a_manter <- c(
    "id_estab", # identificador 1,
    "cei_vinc", # identificador 2,
    "uf", # estado
    "codemun", # municipio
    "endereco", # endereço antes de 2011
    "no_logradouro", # nome
    "nu_logradouro", # numero
    "bairro",
    "cep", 
    "qt_vinc_ativos",
    "nat_jur2009",
    "nat_jur2009_1", # concla 2009.1
    "nat_jur2014",
    "nat_jur2016",
    "nat_jur2018"
  )
  
  original <- arrow::read_parquet(arquivo, col_select = any_of(colunas_a_manter), as_data_frame = F)
  
  
  ## filtros -- adaptado de https://github.com/ipeaGIT/acesso_oport/blob/master/R/fun/empregos/empregos_geo.R
  if("nat_jur2009" %in% colnames(original)) {
    gov <- original |> 
      filter(nat_jur2009 %in% c(1015:1210))
  } else if("nat_jur2009_1" %in% colnames(original)) {
    gov <- original |> 
     filter(nat_jur2009_1 %in% c(1015:1210))
  } else if("nat_jur2014" %in% colnames(original)) {
    gov <- original |> 
      filter(nat_jur2014 %in% c(1015:1279))
  } else if("nat_jur2016" %in% colnames(original)) {
    gov <- original |> 
      filter(nat_jur2016 %in% c(1015:1279))
  } else if("nat_jur2018" %in% colnames(original)) {
    gov <- original |> 
      filter(nat_jur2018 %in% c(1015:1341))
  } else if("nat_jur2021" %in% colnames(original)) {
    gov <- original |> 
      filter(nat_jur2021 %in% c(1015:1341))
  } else {
    message(
      "Coluna de natureza jurídica não detectada. Verifique manualmente se há uma nova em vigência."
    )
  }
  
  if(!is.null(filtro_muni)) {
    original <- original |> 
      filter(codemun %in% filtro_muni$codemun)
  }
  
  original <- original |> 
    filter(qt_vinc_ativos > 0)
  
  original <- original |> 
    anti_join(gov) |> 
    collect()
  
  campos <- geocodebr::definir_campos(
    logradouro = ifelse(ano >= 2011, "no_logradouro", "endereco"),
    numero = if(ano >= 2011) "nu_logradouro" else NULL,
    cep = "cep",
    localidade = if(ano >= 2011) "bairro" else NULL,
    municipio = "codemun",
    estado = "uf"
  )
  
  # workaround para funcionar no padronizar_enderecos
  names(campos)[names(campos) == "localidade"] <- "bairro"
  
  padronizado <- enderecobr::padronizar_enderecos(original, campos_do_endereco = campos)
  
  if(salvar_target) {
    return(padronizado)
  } else {
    if(is.null(destino)) {
      dir_geocode <- file.path(
        Sys.getenv("USERS_DATA_PATH"),
        "CGDTI/IpeaDataLab/projetos/geolocalizacao/rais"
      )
      if (!dir.exists(dir_geocode)) dir.create(dir_geocode)
      destino <- file.path(
        dir_geocode,
        paste0("rais_padronizada_", ano, ".parquet")
      )
    } else {
      destino <- file.path(
        destino,
        paste0("rais_padronizada_", ano, ".parquet")
      )
    }
    
    arrow::write_parquet(padronizado, sink = destino)
    
    return(destino)
  }
}

## reprex
# library(targets)
# arquivo <- tar_read(arquivos_rais)[1]
# ano <- tar_read(anos_rais)[1]
# filtro_muni <- tar_read(muni_list) |>
#   pull(codemun)


# geolocalizar_rais ---------------------------------------------------------------------------

# arquivo <- tar_read(rais_tratada, branches = 1)
# ano <- tar_read(anos_rais)[1]
geolocalizar_rais <- function(arquivo, ano, cores = NULL, salvar_target = TRUE, destino = NULL) {
  
  cores <- if(is.null(cores)) {
    getOption("TARGETS_N_CORES")
  } else {
    cores
  }
  
  padronizado <- if("character" %in% class(arquivo)) {
    arrow::read_parquet("arquivo")
  } else {
    arquivo
  }
  
  campos_do_endereco <- geocodebr::definir_campos(
    logradouro = ifelse(ano >= 2011, "no_logradouro", "endereco"),
    numero = if(ano >= 2011) "nu_logradouro" else NULL,
    cep = "cep",
    localidade = if(ano >= 2011) "bairro" else NULL,
    municipio = "codemun",
    estado = "uf"
  )
  
  geolocalizado <- geocodebr::geocode(
    padronizado,
    campos_endereco = campos_do_endereco,
    resolver_empates = T, 
    resultado_sf = T, 
    n_cores = cores,
    verboso = getOption("TARGETS_VERBOSE")
  )
  
  geoms_vazias <- sf::st_is_empty(geolocalizado)
  geolocalizado_nao_vazio <- geolocalizado[!geoms_vazias, ]
  geolocalizado_vazio <- geolocalizado[geoms_vazias, ]
  
  h3_res9 <- calcular_h3(geolocalizado_nao_vazio, res = 9)
  h3_res8 <- calcular_h3(geolocalizado_nao_vazio, res = 8)
  h3_res7 <- calcular_h3(geolocalizado_nao_vazio, res = 7)
  
  geoloc_tratado <- bind_rows(geolocalizado_nao_vazio, geolocalizado_vazio)
  
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
  
  data.table::setcolorder(geoloc_tratado, c("id_estab", "cei_vinc", "codemun"))
  
  remover_campos <- campos_do_endereco[campos_do_endereco != "codemun"]
  
  geoloc_tratado[, (remover_campos) := NULL]
  
  dir_geocode <- file.path(
    Sys.getenv("USERS_DATA_PATH"),
    "CGDTI/IpeaDataLab/projetos/geolocalizacao/rais"
  )
  
  if(salvar_target) {
    return(geoloc_tratado)
  } else {
    if(is.null(destino)) {
      dir_geocode <- file.path(
        Sys.getenv("USERS_DATA_PATH"),
        "CGDTI/IpeaDataLab/projetos/geolocalizacao/rais"
      )
    } else {
      dir_geocode <- file.path(destino)
    }
    
    destino <- file.path(
      dir_geocode,
      paste0("rais_geolocalizada_", ano, ".parquet")
    )
    arrow::write_parquet(geoloc_tratado, sink = destino)
    
    return(destino)
  }
}