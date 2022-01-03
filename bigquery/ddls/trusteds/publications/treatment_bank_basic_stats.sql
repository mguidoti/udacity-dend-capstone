CREATE TABLE IF NOT EXISTS `poc-plazi-trusted.publications.treatment_bank_basic_stats`
(
  article_uuid          STRING      OPTIONS(description="Publication unique identifier."),
  document_name         STRING      OPTIONS(description="Name of the processed name."),
  doi                   STRING      OPTIONS(description="DOI of the publication, if existent."),
  upload_user           STRING      OPTIONS(description="User to first upload this publication."),
  upload_date           DATE        OPTIONS(description="Date when the upload of this publication happened."),
  update_user           STRING      OPTIONS(description="User to last update this publication."),
  update_date           DATE        OPTIONS(description="Date of the last update of this publication."),
  title                 STRING      OPTIONS(description="Publication title."),
  year_published        STRING      OPTIONS(description="Year of publication."),
  publication           STRING      OPTIONS(description="Source of the publication (e.g., journal, book)."),
  volume                STRING      OPTIONS(description="Volume of the publication, if applicable."),
  issue                 INTEGER     OPTIONS(description="Issue of the publication, if applicable."),
  numero                INTEGER     OPTIONS(description="Numero of the publication, if applicable."),
  first_page            INTEGER     OPTIONS(description="First page of the publication, if applicable."),
  last_page             INTEGER     OPTIONS(description="Last page of the publication, if applicable."),
  num_pages             INTEGER     OPTIONS(description="Total number of pages in this publication."),
  num_treatments        INTEGER     OPTIONS(description="Total number of treatments in this publication."),
  num_treat_citations   INTEGER     OPTIONS(description="Total number of treatments citations in this publication."),
  num_mat_citations     INTEGER     OPTIONS(description="Total number of materials citations in this publication."),
  num_figures           INTEGER     OPTIONS(description="Total number of figures in this publication."),
  num_tables            INTEGER     OPTIONS(description="Total number of tables in this publication."),
  num_references        INTEGER     OPTIONS(description="Total number of references in this publication."),
)
PARTITION BY upload_date
CLUSTER BY publication, upload_user, update_user
OPTIONS (
    description="Trusted table containing basic metadata from processed publications, except author information."
)