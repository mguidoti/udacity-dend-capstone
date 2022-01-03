from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from google.cloud import bigquery

from datetime import datetime, timedelta

import json
import logging

from libs.log import log

default_args = {
    'owner': "Marcus Guidoti | Plazi's Data Engineer",
    'on_success_callback': log,
    'on_failure_callback': log,
    'on_retry_callback': log,
    'sla_miss_callback': log,
    'start_date': "2021-12-31",
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'dag_raw_file-ingestion_FULL',
    default_args=default_args,
    description='',
    schedule_interval="0 2 * * *", # At 2am, everyday
    tags=['raw', 'file-ingestion', 'csv', 'full', 'production', '@daily'])

# List of pipelines to iteractivelly be created
list_of_pipelines = [
    {
        'name': 'people_registry',
        'bucket': 'gs://poc-plazi-ingestion-csv',
        'object': 'notion/human_resources/people.csv',
        'destination_subfolder': 'notion/human_resources/processed',
        'new_object_name': 'people.csv'.replace('.csv', datetime.now().strftime('%Y-%m-%d-%H%M%S')),
        'zone': 'RAW',
        'project_id': 'poc-plazi-raw',
        'dataset': 'csv',
        'table': 'people_registry',
        'aux_dataset': '', # for now, just placeholders for future pipelines
        'aux_table': '', # for now, just placeholders for future pipelines
    }
]


def load_data_from_csv(**kwargs):
    """Loads the data from a file into BigQuery
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name
    table_id = ("{project_id}.{dataset}.{table}"
                        .format(project_id=kwargs.get('project_id'),
                                dataset=kwargs.get('dataset'),
                                table=kwargs.get('table')))

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    uri = kwargs.get('object_uri')

    # Make an API request.
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    # Waits for the job to complete.
    load_job.result()

    destination_table = client.get_table(table_id)

    if destination_table.num_rows > 0:

        xcom = {
            'status': 'Success',
            'message': 'CSV file loaded succesfully.',
            'num_rows_inserted': str(destination_table.num_rows),
            'ran_at': str(datetime.now())
        }

        kwargs['ti'].xcom_push(key='log_values', value=json.dumps(xcom))
    else:
        xcom = {
            'status': 'Failed',
            'message': 'CSV file loaded succesfully.',
            'num_rows_inserted': str(destination_table.num_rows),
            'ran_at': str(datetime.now())
        }

        kwargs['ti'].xcom_push(key='log_values', value=json.dumps(xcom))


def record_log(pipeline, **kwargs):
    """Records log into `data-tools-prod-336318.logs.ingestion`
    """
    client = bigquery.Client()

    log_table_config = {
        'project_id': 'data-tools-prod-336318',
        'dataset': 'logs',
        'table': 'ingestion'
    }

    # Getting the data to compose the BigQuery table id
    log_table_id = ("{project_id}.{dataset}.{table}"
                        .format(project_id=log_table_config.get('project_id'),
                                dataset=log_table_config.get('dataset'),
                                table=log_table_config.get('table')))

    ## Get Biq Query Set up
    log_table = client.get_table(log_table_id)

    # Get Xcom variables from previous task
    xcom = kwargs['ti'].xcom_pull(task_ids=['{pipeline}_load_data_from_csv'.format(pipeline=pipeline.get('name'))], key='log_values')

    # If this isn't available, overwrites the empty dictionary with appropriated
    # values for the situation
    if xcom[0] == None:
        xcom = {
            'status': 'Skipped',
            'message': 'CSV file not found.',
            'num_rows_inserted': '0',
            'ran_at': str(datetime.now())
        }
    else:
        # Converts the xcom into a dictionary
        xcom = json.loads(xcom[0])

    # Compose the data to be written in the log table
    data = [
        (
            'Composer',
            kwargs['dag'].dag_id,
            'FULL',
            'RAW',
            pipeline.get('project_id'),
            pipeline.get('dataset'),
            pipeline.get('table'),
            # datetime.now().strftime('%Y-%m-%d'),
            kwargs['ti'].execution_date.strftime('%Y-%m-%d'),
            xcom.get('status'),
            '',
            xcom.get('message'),
            xcom.get('num_rows_inserted'),
            'Composer', # as logged_from,
            xcom.get('ran_at')
        )
    ]

    insert_errors = client.insert_rows(log_table, data)

    if insert_errors == []:
        logging.info("SUCCESS! No errors where found in the process `{process}` "
                    "from service {service} "
                    "on {date}".format(process=kwargs['dag'].dag_id,
                                       service='Composer',
                                       date=datetime.now().strftime('%Y-%m-%d')))

    else:
        logging.info(("ERROR: Something happened while logging process `{process}` "
                      "from service {service} on {date}. Error messages "
                      "available: {error}".format(process=kwargs['dag'].dag_id,
                                                  service='Composer',
                                                  date=datetime.now().strftime('%Y-%m-%d'),
                                                  error=insert_errors)))



for pipeline in list_of_pipelines:

    gcs_sensor = GCSObjectExistenceSensor(
        task_id=('{pipeline_name}_gcs_sensor'
                    .format(pipeline_name=pipeline.get('name'))),
        bucket=pipeline.get('bucket').replace('gs://', ''),
        object=pipeline.get('object'),
        mode='poke',
        poke_interval=5,
        timeout=10,
        soft_fail=True,
        retries=0,
        dag=dag
    )

    load_data_from_csv = PythonOperator(
        task_id=('{pipeline_name}_load_data_from_csv'
                    .format(pipeline_name=pipeline.get('name'))),
        python_callable=load_data_from_csv,
        op_kwargs={
            'object_uri': ('{bucket}/{object}'
                                .format(bucket=pipeline.get('bucket'),
                                object=pipeline.get('object'))),
            'project_id': pipeline.get('project_id'),
            'dataset': pipeline.get('dataset'),
            'table': pipeline.get('table')
        },
        provide_context=True,
        dag=dag
    )

    record_log = PythonOperator(
        task_id='{pipeline_name}_record_log'
                    .format(pipeline_name=pipeline.get('name')),
        python_callable=record_log,
        op_kwargs={
            'pipeline': pipeline
        },
        provide_context=True,
        trigger_rule="all_done",
        dag=dag
    )

    move_processed_file = BashOperator(
        task_id='{pipeline_name}_move_processed_file'
                    .format(pipeline_name=pipeline.get('name')),
        bash_command=('gsutil mv {bucket}/{object} '
                      '{bucket}/{destination_subfolder}/{new_object_name}'
                      .format(
                        bucket=pipeline.get('bucket'),
                        object=pipeline.get('object'),
                        destination_subfolder=(pipeline
                                                .get('destination_subfolder')),
                        new_object_name=pipeline.get('new_object_name')
                        )
                    ),
        retries=8,
        dag=dag
    )

    gcs_sensor >> load_data_from_csv
    load_data_from_csv >> [move_processed_file, record_log]