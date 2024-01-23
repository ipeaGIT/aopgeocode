# locations <- tar_read(preprocessed_cnes)
geocode_cnes <- function(locations) {
  reticulate::use_condaenv(
    "C://Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3"
  )
  
  address_fields <- geocodepro::address_fields_const(
    Address_or_Place = "LOGRADOURO_COMPLETO",
    Neighborhood = "BAIRRO",
    City = "MUNICIPIO",
    State = "UF",
    ZIP = "CEP"
  )
  
  geocoded_cnes <- geocodepro::geocode(
    locations,
    locator = "../../data-raw/locators/BRA/BRA.loc",
    address_fields = address_fields,
    cache = "cnes"
  )
  
  return(geocoded_cnes)
}