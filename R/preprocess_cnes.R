# cnes_path <- tar_read(cnes_path)
preprocess_cnes <- function(cnes_path) {
  cnes <- readxl::read_xlsx(cnes_path, skip = 15)
  cnes <- dplyr::mutate(
    cnes,
    LOGRADOURO_COMPLETO = paste(LOGRADOURO, NUMERO)
  )
  cnes <- dplyr::select(
    cnes,
    UF, MUNICIPIO = MUNICÍPIO, LOGRADOURO_COMPLETO, BAIRRO, CEP
  )
  
  return(cnes)
}