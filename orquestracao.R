# só pra lembrar como rodar o pipeline, já que alguns targets podem ser rodados
# em paralelo e outros não (porque já usam a geocode() que roda em paralelo)

tar_make_future(c(cadunico_tratado, rais_tratada), workers = 5) # limitado a 5 workers porque senão estoura a memória do servidor
tar_make(c(cadunico_geolocalizado, rais_geolocalizada))
tar_make_future(cpf_tratado, workers = 10)
