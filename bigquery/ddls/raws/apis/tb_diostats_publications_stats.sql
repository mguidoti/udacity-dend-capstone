CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.tb_diostats_publications_stats`
(
  DocArticleUuid        STRING      OPTIONS(description="Publication unique identifier."),
  DocName               STRING      OPTIONS(description="Name of the processed name."),
  DocDoi                STRING      OPTIONS(description="DOI of the publication, if existent."),
  DocUploadUser         STRING      OPTIONS(description="User to first upload this publication."),
  DocUploadDate         STRING      OPTIONS(description="Date when the upload of this publication happened."),
  DocUploadYear         STRING      OPTIONS(description="Year when the upload of this publication happened."),
  DocUploadMonth        STRING      OPTIONS(description="Month when the upload of this publication happened."),
  DocUpdateUser         STRING      OPTIONS(description="User to last update this publication."),
  DocUpdateDate         DATE        OPTIONS(description="Date of the last update of this publication."),
  DocUpdateYear         STRING      OPTIONS(description="Year of the last update of this publication."),
  DocUpdateMonth        STRING      OPTIONS(description="Month of the last update of this publication."),
  BibTitle              STRING      OPTIONS(description="Publication title."),
  BibYear               STRING      OPTIONS(description="Year of publication."),
  BibSource             STRING      OPTIONS(description="Source of the publication (e.g., journal, book)."),
  BibVolume             STRING      OPTIONS(description="Volume of the publication, if applicable."),
  BibIssue              STRING      OPTIONS(description="Issue of the publication, if applicable."),
  BibNumero             STRING      OPTIONS(description="Numero of the publication, if applicable."),
  BibFirstPage          STRING      OPTIONS(description="First page of the publication, if applicable."),
  BibLastPage           STRING      OPTIONS(description="Last page of the publication, if applicable."),
  ContPageCount         STRING      OPTIONS(description="Total number of pages in this publication."),
  ContTreatCount        STRING      OPTIONS(description="Total number of treatments in this publication."),
  ContTreatCitCount     STRING      OPTIONS(description="Total number of treatments citations in this publication."),
  ContMatCitCount       STRING      OPTIONS(description="Total number of materials citations in this publication."),
  ContFigCount          STRING      OPTIONS(description="Total number of figures in this publication."),
  ContTabCount          STRING      OPTIONS(description="Total number of tables in this publication."),
  ContBibRefCitCount    STRING      OPTIONS(description="Total number of references in this publication."),
)
PARTITION BY DocUpdateDate
CLUSTER BY BibSource, DocUploadUser, DocUpdateUser
OPTIONS (
    description="Raw table containing basic metadata from processed publications, except author information."
)