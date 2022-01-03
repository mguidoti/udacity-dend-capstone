CREATE TABLE IF NOT EXISTS `data-tools-prod-336318.logs.crawlers`
(
  service                   STRING      OPTIONS(description='Name of the service that called the process.'),
  instance_name             STRING      OPTIONS(description='Name of the instance of the service that ran the job.'),
  scraper_name              STRING      OPTIONS(description='Name of the process, app, that run the job.'),
  website_name              STRING      OPTIONS(description='Name of the website scrapped.'),
  website_type              STRING      OPTIONS(description='Type of the scrapped website.'),
  website_link              STRING      OPTIONS(description='Root link to the websited scrapped.'),
  number_of_records_found   INTEGER     OPTIONS(description='Number of records found in this run.'),
  start_process_date        DATE        OPTIONS(description='Date when the job started to run.'),
  status                    STRING      OPTIONS(description='Status of this particular job run.'),
  response                  STRING      OPTIONS(description='Response of the job run. It may vary the type of response according to the service and process.'),
  message                   STRING      OPTIONS(description='Customized message passed by the programmer. Not always available.'),
  logged_from               STRING      OPTIONS(description='Service where the log was actually recorded from.'),
  ran_at                    DATETIME    OPTIONS(description='Timestamp when the function to record the log where called.'),
)
PARTITION BY DATE_TRUNC(start_process_date, MONTH)
CLUSTER BY status, website_name, website_type
OPTIONS (
    description='Table that records log message from all crawlers and scrappers running from the platform.'
)