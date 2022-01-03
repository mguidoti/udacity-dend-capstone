CREATE TABLE IF NOT EXISTS `poc-plazi-dashboards.ejt_v4.ejt_publication_stats`
(
    article_uuid          STRING      OPTIONS(description="Publication unique identifier."),
    title                 STRING      OPTIONS(description="Publication title."),
    year_published        STRING      OPTIONS(description="Year of publication."),
    num_treatments        INTEGER     OPTIONS(description="Total number of treatments in this publication."),
    num_treat_citations   INTEGER     OPTIONS(description="Total number of treatments citations in this publication."),
    num_mat_citations     INTEGER     OPTIONS(description="Total number of materials citations in this publication."),
    num_pages             INTEGER     OPTIONS(description="Total number of pages in this publication."),
    num_figures           INTEGER     OPTIONS(description="Total number of figures in this publication."),
    num_tables            INTEGER     OPTIONS(description="Total number of tables in this publication."),
    num_references        INTEGER     OPTIONS(description="Total number of references in this publication."),
)
PARTITION BY _PARTITIONDATE
CLUSTER BY year_published
OPTIONS (
    description='Refined table to power up EJT Dashboard, first page, basic stats.'
)