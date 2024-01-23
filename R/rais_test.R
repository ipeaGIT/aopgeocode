rais_path <- "../../data-raw/rais/2019/rais_estabs_raw_2019.csv"

rais <- data.table::fread(
  rais_path,
  select = list(
    character = c("logradouro", "bairro", "codemun"),
    integer = c("uf", "cep")
  )
)

rais[, cep := formatC(cep, width = 8, format = "d", flag = 0)]

states_list <- geobr::read_state()
data.table::setDT(states_list)
rais[states_list, on = c(uf = "code_state"), estado := i.name_state]
rais[, uf := NULL]

municipalities_list <- geobr::read_municipality()
municipalities_list$code_subset <- substr(municipalities_list$code_muni, 1, 6)
data.table::setDT(municipalities_list)
rais[municipalities_list, on = c(codemun = "code_subset"), cidade := i.name_muni]
rais[, codemun := NULL]

address_fields <- geocodepro::address_fields_const(
  Address_or_Place = "logradouro",
  Neighborhood = "bairro",
  City = "cidade",
  State = "estado",
  ZIP = "cep"
)

reticulate::use_condaenv(
  "C://Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3"
)

tictoc::tic()
geocoded_rais <- geocodepro::geocode(
  rais,
  locator = "../../data-raw/locators/BRA/BRA.loc",
  address_fields = address_fields,
  cache = "rais"
)
tictoc::toc()
