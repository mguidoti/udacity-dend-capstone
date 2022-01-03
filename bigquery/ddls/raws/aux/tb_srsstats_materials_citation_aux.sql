CREATE TABLE IF NOT EXISTS `poc-plazi-raw.aux.tb_srsstats_materials_citation_aux`
(
    DocUuid                 STRING      OPTIONS(description='Document UUID.'),
    DocArticleUuid          STRING      OPTIONS(description='Article UUID.'),
    DocUpdateDate           DATE        OPTIONS(description="Date of the last update of this publication."),
    MatCitId                STRING      OPTIONS(description='Materials Citation UUID.'),
    MatCitCountry           STRING      OPTIONS(description='Collecting Country.'),
    MatCitRegion            STRING      OPTIONS(description='Collecting Region.'),
    MatCitMunicipality      STRING      OPTIONS(description='Collecting Municipality.'),
    MatCitLongitude         STRING      OPTIONS(description='Decimal Longitude.'),
    MatCitLatitude          STRING      OPTIONS(description='Decimal Latitude.'),
    MatCitLongLatStatus     STRING      OPTIONS(description='If the coordinates are present.'),
    MatCitDate              STRING      OPTIONS(description='Collecting Date.'),
    MatCitCollector         STRING      OPTIONS(description='Collector Name.'),
    MatCitCollectionCode    STRING      OPTIONS(description='Collection Code.'),
    MatCitSpecimenCode      STRING      OPTIONS(description='Specimen Code.'),
    MatCitTypeStatus        STRING      OPTIONS(description='Type Status.')
)
PARTITION BY DocUpdateDate
CLUSTER BY DocArticleUuid, DocUuid, MatCitCountry
OPTIONS (
    description='Raw table with the data on materials citations from Treatment Bank.'
)