from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.models import Variable

from datetime import timedelta
import urllib
import json
from ast import literal_eval

import google.auth.transport.requests
import google.oauth2.id_token

from libs.log import log

# Get environment variable from Airflow instance
# env = Variable.get('dag_raw_api_notion_document_tb_FULL', deserialize_json=True)

# Parameters
list_of_pipelines = [
    {
        'name': 'document_db',
        'id': 'ee8c72349da64d7286d222cd6201d3d0',
        'load_mode': 'BULK',
        'zone': 'RAW',
        'project_id': 'poc-plazi-raw',
        'dataset': 'apis',
        'table': 'notion_document_db',
        'filter': {
                    "filter": {
                        "property": "Created at (End Date)",
                        "created_time": {
                            "equals": "{{ prev_ds }}"
                        }
                    }
                }
    },
    {
        'name': 'batch_db',
        'id': 'bd9ba65ddbce40368d175f1527aa6d05',
        'load_mode': 'BULK',
        'zone': 'RAW',
        'project_id': 'poc-plazi-raw',
        'dataset': 'apis',
        'table': 'notion_batch_db',
        'filter': {
                    "filter":{
                        "property": "Updated",
                        "last_edited_time": {
                            "equals": "{{ prev_ds }}"
                        }
                    }
                },
    }
]
def make_authorized_get_request(**op_kwargs):
    """
    make_authorized_get_request makes a GET request to the specified HTTP endpoint
    in service_url (must be a complete URL) by authenticating with the
    ID token obtained from the google-auth client library.
    """
    # print(op_kwargs["service_url"])
    # req = urllib.request.Request(op_kwargs["service_url"])

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, op_kwargs["service_url"])

    print(id_token)

    return id_token


default_args = {
    'owner': "Marcus Guidoti | Plazi's Data Engineer",
    'on_success_callback': log,
    'on_failure_callback': log,
    'on_retry_callback': log,
    'sla_miss_callback': log,
    'start_date': "2022-01-01",
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_raw_api_notion_databases_UPDATE',
    default_args=default_args,
    description='',
    schedule_interval="@daily",
    tags=['raw', 'api', 'UPDATE', 'production', 'notion', '@daily'])

for params in list_of_pipelines:

    auth_token_function = PythonOperator(
    task_id='auth_token_function_{}'.format(params.get('name')),
    python_callable=make_authorized_get_request,
    op_kwargs={"service_url": "https://us-central1-data-tools-prod-336318.cloudfunctions.net/api_notion_official"},
    provide_context=True,
    dag=dag
    )


    call_function = SimpleHttpOperator(
        task_id='call_function_{}'.format(params.get('name')),
        http_conn_id='data-tools-prd_functions',
        method='POST',
        endpoint='api_notion_official',
        data=json.dumps({
                'name': params.get('name'),
                'load_mode': params.get('load_mode'),
                'zone': params.get('zone'),
                'project_id': params.get('project_id'),
                'dataset': params.get('dataset'),
                'table': params.get('table'),
                'payload': params.get('filter'),
                'endpoint': 'databases/{database_id}/query'.format(database_id=params.get('id'))
                }),
        response_check=lambda response: response.status_code == 200,
        headers={"Authorization": "Bearer {}".format("{{ task_instance.xcom_pull(task_ids='auth_token_function_" + params.get('name') + "', key='return_value') }}"),
                "Content-Type": "application/json"},
        do_xcom_push=False,
        dag=dag)


    dataquality = BigQueryOperator(
        task_id='dataquality_{}'.format(params.get('name')),
        sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
            project_id=params.get('project_id'),
            dataset=params.get('dataset'),
            table=params.get('table'),
            config_empty_check=True,
            config_duplicates_check=False,
            duplicates_table_key=[]
        ),
        use_legacy_sql=False,
        priority="BATCH",
        do_xcom_push=False,
        dag=dag
    )

    auth_token_function >> call_function
    call_function >> dataquality