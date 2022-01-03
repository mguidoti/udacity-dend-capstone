CREATE TABLE IF NOT EXISTS `poc-plazi-raw.platform.publications_tunnel`
(
  publisher_full_name       STRING      OPTIONS(description='Full name of the journal or published when the publication is not an scholarly article.'),
  publisher_short_name      STRING      OPTIONS(description='Short name of the journal or published when the publication is not an scholarly article.'),
  scraper_name              STRING      OPTIONS(description='Name of the scraper used.'),
  year                      STRING      OPTIONS(description='Year of the publication. Used to guide new scrapping sections.'),
  volume                    STRING      OPTIONS(description='Volume of the publication. Used to guide new scrapping sections.'),
  issue                     STRING      OPTIONS(description='Issue of the publication. Used to guide new scrapping sections.'),
  start_page                STRING      OPTIONS(description='Starting page of the publication. Used to guide new scrapping sections.'),
  end_page                  STRING      OPTIONS(description='Final page of the publication. Used to guide new scrapping sections.'),
  publication_type          STRING      OPTIONS(description='Type of the publication. Used to guide new scrapping sections.'),
  doi                       STRING      OPTIONS(description='DOI of the publication.'),
  first_author_surname      STRING      OPTIONS(description='Type of the publication. Used to guide new scrapping sections.'),
  link                      STRING      OPTIONS(description='Link to download the main file associated with the publication.'),
  filename                  STRING      OPTIONS(description='Filename composed based on style rules and standards configured in the scraper.'),
  status                    STRING      OPTIONS(description='Status of the file in the platform. Could be either `scraped`, `downloaded`, `uploaded` or `processed`.'),
  inserted_at               DATETIME    OPTIONS(description='Date where this row was inserted.'),
)
PARTITION BY DATE(inserted_at)
CLUSTER BY status, publisher_full_name, filename
OPTIONS (
    description='Table that records any status update from the applications running the publications tunnel in the platform.'
)