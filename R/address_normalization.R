# old_rais_path <- "../../data-raw/rais/2019/rais_estabs_raw_2019.csv"
# rais <- data.table::fread(old_rais_path, select = "logradouro")
# addresses <- rais$logradouro
#
# rais_path <- file.path(Sys.getenv("RESTRICTED_DATA_PATH"), "RAIS/parquet/estab2015/0_0_0.parquet")
# rais <- arrow::read_parquet(rais_path, col_select = c("logradouro", "bairro", "codemun", "uf"))
# data.table::setDT(rais)
# addresses <- rais$logradouro
normalize_street_address <- function(addresses) {
  addresses <- unique(addresses)
  original <- addresses
  
  addresses <- toupper(addresses)
  
  # remove accents
  addresses <- stringi::stri_trans_general(addresses, "Latin-ASCII")
  
  find_and_show_pattern <- function(p, data = addresses, view = FALSE) {
    matches <- data[stringr::str_detect(data, p)]
    if (view) print(stringr::str_view(data, p))
    return(matches)
  }
  
  search_original_show_changed <- function(p, data = addresses) {
    matches <- data[stringr::str_detect(original, p)]
    return(matches)
  }
  
  tictoc::tic()
  addresses <- stringr::str_replace_all(
    addresses,
    c(
      # pontuacao
      "\\.\\.+" = ".",         # ponto repetido
      ",,+" = ",",             # virgula repetida
      "\\.([^ ])" = "\\. \\1", # garantir que haja um espaco depois dos pontos
      ",([^ ])" = ", \\1",     # garantir que haja um espaco depois das virgulas
      
      # tipos de logradouro
      "^R(A|U)?\\b(\\.|,)? " = "RUA ",                                 # R. AZUL -> RUA AZUL
      "^(RUA|RODOVIA|ROD(\\.|,)?) (RUA|R(A|U)?)\\b(\\.|,)? " = "RUA ", # RUA R. AZUL -> RUA AZUL
      "^(RUA|R(A|U)?)\\b- *" = "RUA ",                                 # R-AZUL -> RUA AZUL
      "^(RUA|RODOVIA|ROD(\\.|,)?) (RUA|R(A|U)?)\\b- *" = "RUA ",       # RUA R-AZUL -> RUA AZUL
      
      "^ROD\\b(\\.|,)? " = "RODOVIA ",
      "^(RODOVIA|RUA) (RODOVIA|ROD)\\b(\\.|,)? " = "RODOVIA ",
      "^(RODOVIA|ROD)\\b- *" = "RODOVIA ",
      "^(RUA|RODOVIA) (RODOVIA|ROD)\\b- *" = "RODOVIA ",
      
      # outros pra rodovia: "RO", "RO D", "ROV"
      
      "^AV(E|N|D|DA|I)?\\b(\\.|,)? " = "AVENIDA ",
      "^(AVENIDA|RUA|RODOVIA) (AVENIDA|AV(E|N|D|DA|I)?)\\b(\\.|,)? " = "AVENIDA ",
      "^(AVENIDA|AV(E|N|D|DA|I)?)\\b- *" = "AVENIDA ",
      "^(AVENIDA|RUA|RODOVIA) (AVENIDA|AV(E|N|D|DA|I)?)\\b- * " = "AVENIDA ",
      
      "^ESTR?\\b(\\.|,)? " = "ESTRADA ",
      "^(ESTRADA|RUA|RODOVIA) (ESTRADA|ESTR?)\\b(\\.|,)? " = "ESTRADA ",
      "^(ESTRADA|ESTR?)\\b- *" = "ESTRADA ",
      "^(ESTRADA|RUA|RODOVIA) (ESTRADA|ESTR?)\\b- *" = "ESTRADA ",
      
      "^(PCA|PRC)\\b(\\.|,)? " = "PRACA ",
      "^(PRACA|RUA|RODOVIA) (PRACA|PCA|PRC)\\b(\\.|,)? " = "PRACA ",
      "^(PRACA|PCA|PRC)\\b- *" = "PRACA ",
      "^(PRACA|RUA|RODOVIA) (PRACA|PCA|PRC)\\b- *" = "PRACA ",
      
      "^(BEC|BCO?)\\b(\\.|,)? " = "BECO ",
      "^(BECO|RUA|RODOVIA) BE?CO?\\b(\\.|,)? " = "BECO ",
      "^BE?CO?\\b- *" = "BECO ",
      "^(BECO|RUA|RODOVIA) BE?CO?\\b- *" = "BECO ",

      "^(TV|TRV|TRAV?)\\b(\\.|,)? " = "TRAVESSA", # tem varios casos de TR tambem, mas varios desses sao abreviacao de TRECHO, entao eh dificil fazer uma generalizacao
      "^(TRAVESSA|RODOVIA) (TRAVESSA|TV|TRV|TRAV?)\\b(\\.|,)? " = "TRAVESSA ", # nao botei RUA nas opcoes iniciais porque tem varios ruas que realmente sao RUA TRAVESSA ...
      "^(TRAVESSA|TV|TRV|TRAV?)\\b- *" = "TRAVESSA ",
      "^(TRAVESSA|RUA|RODOVIA) (TRAVESSA|TV|TRV|TRAV?)\\b- *" = "TRAVESSA ", # aqui ja acho que faz sentido botar o RUA porque so da match com padroes como RUA TRAVESSA-1
      
      "^PR?Q\\b(\\.|,)? " = "PARQUE ",
      "^(PARQUE|RODOVIA) (PARQUE|PR?Q)\\b(\\.|,)? " = "PARQUE ", # mesmo caso de travessa
      "^(PARQUE|PR?Q)\\b- *" = "PARQUE ",
      "^(PARQUE|RUA|RODOVIA) (PARQUE|PR?Q)\\b- *" = "PARQUE ", # mesmo caso de travessa
      
      "^ALA?\\b(\\.|,)? " = "ALAMEDA ",
      "^ALAMEDA (ALAMEDA|ALA?)\\b(\\.|,)? " = "ALAMEDA ", # mesmo caso de travessa
      "^RODOVIA (ALAMEDA|ALA)\\b(\\.|,)? " = "ALAMEDA ", # RODOVIA precisa ser separado porque nesse caso nao podemos mudar RODOVIA AL pra ALAMEDA, ja que pode ser uma rodovia estadual de alagoas
      "^(ALAMEDA|ALA?)\\b- *" = "ALAMEDA ",
      "^ALAMEDA (ALAMEDA|ALA?)\\b- *" = "ALAMEDA ", # mesmo caso de travessa
      "^RODOVIA (ALAMEDA|ALA)\\b- *" = "ALAMEDA ", # mesmo caso acima
      
      "^LOT\\b(\\.|,)? " = "LOTEAMENTO ",
      "^(LOTEAMENTO|RUA|RODOVIA) LOT\\b(\\.|,)? " = "LOTEAMENTO ",
      "^LOT(EAMENTO)?\\b- *" = "LOTEAMENTO ",
      "^(LOTEAMENTO|RUA|RODOVIA) LOT(EAMENTO)?\\b- *" = "LOTEAMENTO ",
      
      # estabelecimentos
      
      "^AER\\b(\\.|,)?" = "AEROPORTO", # sera que vale? tem uns casos estranhos aqui, e.g. "AER GUANANDY, 1", "AER WASHINGTON LUIZ, 3318" 
      "^AEROPORTO (AEROPORTO|AER)\\b(\\.|,)?" = "AEROPORTO",
      "^AEROPORTO (INT|INTER)\\b(\\.|,)?" = "AEROPORTO INTERNACIONAL",
      
      "^COND\\b(\\.|,)?" = "CONDOMINIO",
      "^(CONDOMINIO|RODOVIA) (CONDOMINIO|COND)\\b(\\.|,)?" = "CONDOMINIO",
      
      "^FAZ\\b(\\.|,)?" = "FAZENDA",
      "^(FAZENDA|RODOVIA) (FAZ|FAZENDA)\\b(\\.|,)?" = "FAZENDA",
      
      # títulos
      "\\bSTA\\b\\.?" = "SANTA",
      "\\bSTO\\b\\.?" = "SANTO",
      "\\b(N\\.? S(RA)?)\\b\\.?" = "NOSSA SENHORA",
      
      "\\bALMTE\\b\\.?" = "ALMIRANTE",
      "\\bMAL\\b\\.?" = "MARECHAL",
      "\\b(GEN|GAL)\\b\\.?" = "GENERAL",
      "\\bSGTO?\\b\\.?" = "SARGENTO",
      "\\bCEL\\b\\.?" = "CORONEL",
      "\\bBRIG\\b\\.?" = "BRIGADEIRO",
      "\\bTEN\\b\\.?" = "TENENTE",
      "\\bTENENTE CORONEL\\b" = "TENENTE-CORONEL",
      "\\bTENENTE BRIGADEIRO\\b" = "TENENTE-BRIGADEIRO",
      "\\bTENENTE AVIADOR\\b" = "TENENTE-AVIADOR",
      "\\bSUB TENENTE\\b" = "SUBTENENTE",
      "\\b(PRIMEIRO|PRIM\\.?) TENENTE\\b" = "PRIMEIRO-TENENTE",
      "\\b(SEGUNDO|SEG\\.?) TENENTE\\b" = "SEGUNDO-TENENTE",
      
      "\\bPROF\\b\\.?" = "PROFESSOR",
      "\\bPROFA\\b\\.?" = "PROFESSORA",
      "\\bDR\\b\\.?" = "DOUTOR",
      "\\bDRA\\b\\.?" = "DOUTORA",
      "\\bENG\\b\\.?" = "ENGENHEIRO",
      "\\bENGA\\b\\.?" = "ENGENHEIRA",
      "\\bPE\\b\\." = "PADRE", # PE pode ser só pe mesmo, entao forcando o PE. (com ponto) pra ser PADRE
      
      "\\bPRES(ID)?\\b\\.?" = "PRESIDENTE",
      "\\bGOV\\b\\.?" = "GOVERNADOR",
      "\\bSEN\\b\\.?" = "SENADOR",
      "\\bPREF\\b\\.?" = "PREFEITO",
      "\\bDEP\\b\\.?" = "DEPUTADO",
      "\\bVER\\b\\.?" = "VEREADOR", # pelo menos um endereço fica zoado com isso: "Q QS 5 RUA 100 LOTE 2 APARTAMENTO 801 COSTA VER"
      "\\bMIN\\b\\.?" = "MINISTRO", # fica zoado sepa: "ESTRADA PIRACANJUBA RODOVIA MIN A PIRACA"
      
      # abreviacoes
      "\\bUNID\\b\\.?" = "UNIDADE",
      "\\b(CJ|CONJ)\\b\\.?" = "CONJUNTO",
      "\\bLT\\b\\.?" = "LOTE",
      "\\bLTS\\b\\.?" = "LOTES",
      "\\bQD\\b\\.?" = "QUADRA",
      "\\bLJ\\b\\.?" = "LOJA",
      "\\bLJS\\b\\.?" = "LOJAS",
      "\\bAPTO?\\b\\.?" = "APARTAMENTO",
      "\\bBL\\b\\.?" = "BLOCO",
      "\\bSLS\\b\\.?" = "SALAS",
      "\\bEDI?F\\.? EMP\\b\\.?" = "EDIFICIO EMPRESARIAL",
      "\\bEDI?F\\b\\.?" = "EDIFICIO",
      "\\bCOND\\b\\.?" = "CONDOMINIO", # apareceu antes mas como tipo de logradouro
      # SL pode ser sobreloja ou sala
      
      # intersecao entre nomes e titulos
      #   - D. pode ser muita coisa (e.g. dom vs dona), entao nao da pra
      #   simplesmente assumir que vai ser um valor especifico, so no contexto
      #   - MAR pode ser realmente só mar ou uma abreviação pra marechal
      "\\bD\\b\\.? (PEDRO|JOAO)" = "DOM \\1",
      "\\bMAR\\b\\.? ((CARMONA|JOFRE|HERMES|MALLET|DEODORO|MARCIANO|OTAVIO|FLORIANO|BARBACENA|FIUZA|MASCARENHAS|MASCARENHA|TITO|FONTENELLE|XAVIER|BITENCOURT|BITTENCOURT|CRAVEIRO|OLIMPO|CANDIDO|RONDON|HENRIQUE|MIGUEL|JUAREZ|FONTENELE|FONTENELLE|DEADORO|HASTIMPHILO|NIEMEYER|JOSE|LINO|MANOEL|HUMB?|HUMBERTO|ARTHUR|ANTONIO|NOBREGA|CASTELO|DEODORA)\\b)" = "MARECHAL \\1",
      
      # nomes
      "\\b(GETULHO|JETULHO|JETULIO|JETULHO|GET|JET)\\.? VARGAS\\b" = "GETULIO VARGAS",
      "\\b(JUS|JUSC|JUSCELINO)\\.? (K|KUBI|KUB|KUBITSC|KUBITSCH|KUBITSCHEK)\\b\\.?" = "JUSCELINO KUBITSCHEK",
      
      # expressoes hifenizadas ou nao
      #   - beira-mar deveria ter pelo novo acordo ortografico, mas a grafia da
      #   grande maioria das ruas (se nao todas, nao tenho certeza) eh beira
      #   mar, sem hifen
      "\\bBEIRA-MAR\\b" = "BEIRA MAR",
      
      # rodovias
      "\\b(RODOVIA|BR\\.?|RODOVIA BR\\.?) CENTO D?E (DESESSEIS|DESESEIS|DEZESSEIS|DEZESEIS)\\b" = "RODOVIA BR-116",
      "\\b(RODOVIA|BR\\.?|RODOVIA BR\\.?) CENTO D?E H?UM\\b" = "RODOVIA BR-101",
      
      # 0 à esquerda
      " (0)(\\d+)" = " \\2", # isso pode dar ruim em casos como RODOVIA BR 020 KM 140. no fundo melhor sempre transformar BR \\d{3} em BR-\\d{3}
      
      # correcoes de problemas ocasionados pelos filtros acima
      "\\bTENENTE SHI\\b" = "TEN SHI"
      
      
      # ALM é um caso complicado, pode ser alameda ou almirante. inclusive no mesmo endereço podem aparecer os dois rs
    )
  )
  tictoc::toc()
  
  
  find_and_show_pattern("^(TRA?V?\\b)(\\.|,)?")
  
  show_first_2_words <- function(v) {
    l <- stringr::str_split(v, " ")
    l <- purrr::map_chr(l, function(i) paste(i[1], i[2]))
  }
}