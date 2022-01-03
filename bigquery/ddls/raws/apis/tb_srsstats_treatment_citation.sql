CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.tb_srsstats_treatment_citation`
(
    DocUuid                     STRING      OPTIONS(description='Document UUID.'),
    DocArticleUuid              STRING      OPTIONS(description='Article UUID.'),
    DocUpdateDate               DATE        OPTIONS(description="Date of the last update of this publication."),
    TreatCitCitId               STRING      OPTIONS(description='Treatment Citation ID.'),
    TreatCitRank                STRING      OPTIONS(description='Rank of Cited Taxon.'),
    TreatCitKingdomEpithet      STRING      OPTIONS(description='Taxonomic Kingdom.'),
    TreatCitPhylumEpithet       STRING      OPTIONS(description='Taxonomic Phylum.'),
    TreatCitClassEpithet        STRING      OPTIONS(description='Taxonomic Class.'),
    TreatCitOrderEpithet        STRING      OPTIONS(description='Taxonomic Order.'),
    TreatCitFamilyEpithet       STRING      OPTIONS(description='Taxonomic Family.'),
    TreatCitGenusEpithet        STRING      OPTIONS(description='Cited Taxon Genus.'),
    TreatCitSpeciesEpithet      STRING      OPTIONS(description='Cited Taxon Species.'),
    TreatCitAuthName            STRING      OPTIONS(description='Cited Taxon Autority Name.'),
    TreatCitAuthYear            STRING      OPTIONS(description='Cited Taxon Autority Year.'),
    TreatCitCitAuthor           STRING      OPTIONS(description='Cited Authors.'),
    TreatCitCitYear             STRING      OPTIONS(description='Cited Year of Publication.'),
    TreatCitCitSource           STRING      OPTIONS(description='Cited Journal / Publisher.'),
    TreatCitCitVolume           STRING      OPTIONS(description='Cited Volume Number.'),
    TreatCitCitPage             STRING      OPTIONS(description='Cited Page.'),
    TreatCitCitRefString        STRING      OPTIONS(description='Verbatim Cited Reference.'),
)
PARTITION BY DocUpdateDate
CLUSTER BY DocArticleUuid, DocUuid, TreatCitClassEpithet
OPTIONS (
    description='Raw table with the data on treatment citations from Treatment Bank.'
)