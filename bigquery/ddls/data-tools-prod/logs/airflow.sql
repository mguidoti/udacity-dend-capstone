CREATE TABLE IF NOT EXISTS `logs.airflow`
(
  state                         STRING      OPTIONS(description='State of the task.'),
  dag_id                        STRING      OPTIONS(description='Dag ID where the task ran.'),
  task_id                       STRING      OPTIONS(description='Task ID specific to this log entry.'),
  run_id                        STRING      OPTIONS(description='Run ID of this specific DagRun.'),
  job_id                        INTEGER     OPTIONS(description='Job ID specific to this log entry.'),
  execution_date                TIMESTAMP   OPTIONS(description='Execution date of this specific job ID.'),
  owner                         STRING      OPTIONS(description='Owner of this task. Usually, inherited form the DAG object.'),
  schedule_interval             STRING      OPTIONS(description='Scheduled interval of the DAG that contains this specific task.'),
  start_date                    TIMESTAMP   OPTIONS(description='Start date of the DAG that contains this specific task.'),
  end_date                      TIMESTAMP   OPTIONS(description='End date of the DAG that contains this specific task.'),
  duration                      NUMERIC     OPTIONS(description='Duration of this specific job ID.'),
  try_number                    INTEGER     OPTIONS(description='Number of the attempt of this specific job ID.'),
  max_tries                     INTEGER     OPTIONS(description='Max number of tries allowed for this particular task_id.'),
  prev_attempted_tries          INTEGER     OPTIONS(description='Number of the previous attempt of this specific job ID.'),
  log_url                       STRING      OPTIONS(description='URL to the log of this specific job ID.'),
  log_filepath                  STRING      OPTIONS(description='Filepath in the Composer bucket containing the log of this specific job ID.'),
  previous_successfull_ti       INTEGER     OPTIONS(description='Job ID of the previously successfull task instance of this particular task id.'),
  previous_start_date_success   TIMESTAMP   OPTIONS(description='Previous successfull start date of this specific task ID.'),
)
PARTITION BY TIMESTAMP_TRUNC(start_date, DAY)
OPTIONS (
    description="Table that records log message from all Airflow tasks."
)