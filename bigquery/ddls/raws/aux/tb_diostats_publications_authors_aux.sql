CREATE TABLE IF NOT EXISTS `poc-plazi-raw.aux.tb_diostats_publications_authors_aux`
(
    DocArticleUuid      STRING      OPTIONS(description="Article UUID"),
    DocUpdateDate       DATE        OPTIONS(description="Date of the last update of this publication."),
    AuthName            STRING      OPTIONS(description="Name of the author."),
    AuthAff             STRING      OPTIONS(description="Affiliation information associated with the author."),
    AuthEmail           STRING      OPTIONS(description="Email of the author."),
    AuthLsid            STRING      OPTIONS(description="LSID of the author."),
    AuthOrcid           STRING      OPTIONS(description="ORCID of the author."),
    AuthUrl             STRING      OPTIONS(description="URL associated with the author.")
)
PARTITION BY DocUpdateDate
CLUSTER BY DocArticleUuid
OPTIONS (
    description="Raw table with the data on authors of publications processed and available on Treatment Bank."
)