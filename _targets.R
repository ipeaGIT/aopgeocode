suppressPackageStartupMessages({
  library(targets)
  loadNamespace("sf")
})

source("R/preprocess_cnes.R", encoding = "UTF-8")
source("R/geocode.R", encoding = "UTF-8")

list(
  tar_target(
    cnes_path,
    "../../data-raw/cnes/2022/BANCO_ESTAB_IPEA_COMP_08_2022_DT_25_10_2023.xlsx",
    format = "file_fast"
  ),
  tar_target(preprocessed_cnes, preprocess_cnes(cnes_path)),
  tar_target(geocoded_cnes, geocode_cnes(preprocessed_cnes))
)
