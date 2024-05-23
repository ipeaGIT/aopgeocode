# só pra lembrar como rodar o pipeline, já que alguns targets podem ser rodados
# em paralelo e outros não (porque já usam a geocode() que roda em paralelo)

tar_make_future(c(cadunico_tratado, rais_tratada), workers = 5) # limitado a 5 workers porque senão estoura a memória do servidor
tar_make(c(cadunico_geolocalizado, rais_geolocalizada, cpf_geolocalizado, cnefe_geolocalizado))
tar_make_future(cpf_tratado, workers = 3)
tar_make_future(censo_escolar_tratado, workers = 17)
tar_make_future(cnefe_tratado, workers = 25)
