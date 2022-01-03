CREATE TABLE IF NOT EXISTS `data-tools-prod-336318.logs.data_updates`
(
  service                   STRING      OPTIONS(description='Name of the service that called the process.'),
  process                   STRING      OPTIONS(description='Name of the process, app, that run the job.'),
  load_mode                 STRING      OPTIONS(description='The load mode of this process.'),
  zone                      STRING      OPTIONS(description='The data zone where this table is located.'),
  project_id                STRING      OPTIONS(description='Project ID of the destination table.'),
  dataset                   STRING      OPTIONS(description='Dataset of the destination table.'),
  table                     STRING      OPTIONS(description='Name of the destination table.'),
  start_process_date        DATE        OPTIONS(description='Date when the job started to run.'),
  status                    STRING      OPTIONS(description='Status of this particular job run.'),
  response                  STRING      OPTIONS(description='Response of the job run. It may vary the type of response according to the service and process.'),
  message                   STRING      OPTIONS(description='Customized message passed by the programmer. Not always available.'),
  num_rows_inserted         INTEGER     OPTIONS(description='Number of rows added in this process to the target table.'),
  logged_from               STRING      OPTIONS(description='Service where the log was actually recorded from.'),
  ran_at                    DATETIME    OPTIONS(description='Timestamp when the function to record the log where called.'),
)
PARTITION BY DATE_TRUNC(start_process_date, MONTH)
CLUSTER BY status, zone, project_id
OPTIONS (
    description='Table that records log message from all data updates processes to trusted and refined zones.'
)