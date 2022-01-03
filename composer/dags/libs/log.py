################################################################################
### Object:
### Created at: 2021-12-27
### Description:
### Author: Marcus Guidoti | Plazi's Data Engineer
### Email: guidoti@plazi.org

### Change Log:
################################################################################

import logging

from airflow.models import Variable
from airflow.utils.state import State

from google.cloud import bigquery

# Get environment variable from Airflow instance
env = Variable.get("logging", deserialize_json=True)

# Compile the table id where the log will be recorded
table_info = [env.get('project_id'),
                 env.get('dataset'),
                 env.get('table')]

table_id = '.'.join(table_info)


def log(context):
    """Log function that records the log a task in a specific, parametrized,
    table in BigQuery.

    Args:
        context (dict): Dictionary containing information regarding the dag run
        and that particular task instance.

    """

    # Instantiate a BigQuery client
    client = bigquery.Client()

    # Returns objects and information from the context dictionary
    ti = context.get('task_instance')
    dag = context.get('dag')
    run_id = context.get('run_id')

    # Logs initial message to Airflow UI and official log
    logging.info('Task instance: {}'.format(ti))
    logging.info(('Starting to log Airflow data into BigQuery '
                  'table `{table_id}`'.format(table_id=table_id)))

    # Prepares data to be inserted into the Airflow log table
    data = [
        (
            str(ti.state),
            str(ti.dag_id),
            str(ti.task_id),
            str(run_id),
            int(ti.job_id),
            str(ti.execution_date),
            (ti.task.owner if ti.task.owner != "N/A" and
                              ti.task.owner != 'None'
                           else None),
            str(ti.start_date),
            str(ti.end_date) if str(ti.end_date) != 'None' else None,
            dag.default_args.get('schedule_interval'),
            float(ti.duration),
            int(ti.try_number),
            int(ti.max_tries),
            int(ti.prev_attempted_tries),
            str(ti.log_url),
            str(ti.log_filepath),
            (ti.get_previous_ti(state=State.SUCCESS).job_id
                if (ti.get_previous_ti(state=State.SUCCESS) != 'None'
                and ti.get_previous_ti(state=State.SUCCESS) != None)
                else None),
            (ti.get_previous_start_date(state=State.SUCCESS)
                if (str(ti.get_previous_start_date(state=State.SUCCESS)) != 'None'
                and str(ti.get_previous_start_date(state=State.SUCCESS)) != None)
                else None)
        )
    ]

    # Returns table object and insert rows in it
    table = client.get_table(table_id)
    insert_return = client.insert_rows(table, data)

    # Log more messages, the first according to the status returned by BigQuery
    if len(insert_return) > 0:
        logging.info(f'BigQuery returned: {insert_return}')

    logging.info('Finished Airflow Log.')