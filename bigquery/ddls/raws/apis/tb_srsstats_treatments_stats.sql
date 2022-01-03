CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.tb_srsstats_treatments_stats`
(
    DocUuid                 STRING      OPTIONS(description='Document UUID.'),
    DocArticleUuid          STRING      OPTIONS(description='Article UUID.'),
    DocUploadUser           STRING      OPTIONS(description='User to first Upload Document.'),
    DocUploadDate           STRING      OPTIONS(description='Timestamp of first Upload.'),
    DocUpdateUser           STRING      OPTIONS(description='User to last Update Document.'),
    DocUpdateDate           DATE        OPTIONS(description='Timestamp of last Update.'),
    TaxKingdomEpithet       STRING      OPTIONS(description='Taxonomic Kingdom.'),
    TaxPhylumEpithet        STRING      OPTIONS(description='Taxonomic Phylum.'),
    TaxClassEpithet         STRING      OPTIONS(description='Taxonomic Class.'),
    TaxOrderEpithet         STRING      OPTIONS(description='Taxonomic Order.'),
    TaxFamilyEpithet        STRING      OPTIONS(description='Taxonomic Family.'),
    TaxGenusEpithet         STRING      OPTIONS(description='Taxon Genus.'),
    TaxSpeciesEpithet       STRING      OPTIONS(description='Taxon Species.'),
    TaxAuthName             STRING      OPTIONS(description='Taxon Autority Name.'),
    TaxAuthYear             STRING      OPTIONS(description='Taxon Autority Year.'),
    TaxStatus               STRING      OPTIONS(description='Taxonomic Status.'),
    TaxIsKey                STRING      OPTIONS(description='Treatment Is Key.'),
    CitTreatCitCount        STRING      OPTIONS(description='Number of Treatment Citations.'),
    CitBibRefCitCount       STRING      OPTIONS(description='Number of Bibliographic Citations.'),
    CitFigCitCount          STRING      OPTIONS(description='Number of Figure Citations.'),
    CitTabCitCount          STRING      OPTIONS(description='Number of Table Citations.'),
    MatMatCitCount          STRING      OPTIONS(description='Number of Materials Citations.')
)
PARTITION BY DocUpdateDate
CLUSTER BY DocUuid, DocArticleUuid, TaxClassEpithet
OPTIONS (
    description='Raw table containing basic metadata from treatments in Treatment Bank.'
)