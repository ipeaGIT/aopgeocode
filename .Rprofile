source("renv/activate.R")

# infelizmente isso precisa vir aqui pra que a gente consigar usar o {arrow}. o
# {arrow} tb usa o reticulate e acaba inviabilizando o uso do arcpy se a gente
# não importar o arcpy antes de usar qualquer coisa relacionada a parquet e
# afins. o rstudio tb tem um esquema de "pre-load" dos pacotes usados no código
# que faz com que a gente precise fazer essa chamada no .Rprofile, caso
# contrário não funcionaria
if (interactive()) {
  reticulate::use_condaenv(
    "C://Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3"
  )
  invisible(reticulate::import("arcpy"))
}

if (Sys.getenv("RUSER") == "DHERSZ") source("~/.Rprofile")
