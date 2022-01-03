CREATE TABLE IF NOT EXISTS `data-tools-prod-336318.logs.file_lifespan`
(
  service                   STRING      OPTIONS(description='Name of the service that called the process.'),
  process                   STRING      OPTIONS(description='Name of the process, app, that run the job.'),
  filepath_service          STRING      OPTIONS(description='Service where the file was found.'),
  filepath_project_id       STRING      OPTIONS(description='Name of the project where the file was located.'),
  filepath_instance_name    STRING      OPTIONS(description='Name of the instance of the service holding the file. Not always applicable.'),
  filepath                  STRING      OPTIONS(description='Filepath to the file.'),
  filename                  STRING      OPTIONS(description='Name of the file.'),
  filesize                  NUMERIC     OPTIONS(description='Size of the file.'),
  nature_of_file            STRING      OPTIONS(description='Nature of the file (e.g., publication).'),
  destination               STRING      OPTIONS(description='Destination of the file.'),
  start_process_date        DATE        OPTIONS(description='Date when the job started to run.'),
  status                    STRING      OPTIONS(description='Status of this particular job run.'),
  response                  STRING      OPTIONS(description='Response of the job run. It may vary the type of response according to the service and process.'),
  message                   STRING      OPTIONS(description='Customized message passed by the programmer. Not always available.'),
  logged_from               STRING      OPTIONS(description='Service where the log was actually recorded from.'),
  ran_at                    DATETIME    OPTIONS(description='Timestamp when the function to record the log where called.'),
)
PARTITION BY DATE_TRUNC(start_process_date, MONTH)
CLUSTER BY status, nature_of_file, service
OPTIONS (
    description='Table that records log message from all downloads and uploads of files into and from the platform.'
)