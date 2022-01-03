from airflow import DAG

from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (ComputeEngineStartInstanceOperator,
                                                              ComputeEngineStopInstanceOperator)

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from datetime import datetime, timedelta
import urllib
import json

import google.auth.transport.requests
import google.oauth2.id_token

from libs.log import log


##############
# DECLARE DAG
##############

default_args = {
    'owner': "Marcus Guidoti | Plazi's Data Engineer",
    'on_success_callback': log,
    'on_failure_callback': log,
    'on_retry_callback': log,
    'sla_miss_callback': log,
    'start_date': "2021-07-31",
    "end_date": "2021-08-31",
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dag_raw_api_tb_diostats_publications_stats_FULL_august_2021',
    default_args=default_args,
    description='FULL load on tb_diostats_publications_stats with monthly dagruns.',
    schedule_interval="@daily",
    tags=['raw', 'api', 'treatment bank', 'FULL', 'production', '@daily', 'arcadia I'],
    max_active_runs=1
    )

#############
# PARAMETERS
#############
params = {
    'name': 'tb_diostats_publications_stats',
    'conn_id': {
        'http': 'data-tools-prd_functions',
        'gcp': 'compute_engine_conn',
    },
    'app_engine': {
        'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
        'name': 'api_treatment_bank_stats',
        'config': {
            'endpoint': 'dioStats',
            'table': 'basic_stats',
            'load_mode': 'FULL',
            'zone': 'RAW',
            "project_id": "poc-plazi-raw",
            "dataset": "apis",
            "table_name": "tb_diostats_publications_stats",
            "delta_fields": [{
                    "name": "DocUploadDate",
                    "value": '"{{ prev_ds }}"-"{{ ds }}"'
                }]
        }
    },
    'vm_instance': {
        'name': 'instance-1',
        'zone': 'us-central1-a',
        'project_id': 'data-tools-prod-336318',
        'command': ('sudo python3 ~/tb_stats/run.py'
                    ' -t DocUploadDate'
                    ' -e dioStats'
                    ' -b basic_stats'
                    ' -s {{ prev_execution_date.strftime("%Y-%m-%d") }}'
                    ' -f {{ execution_date.strftime("%Y-%m-%d") }}'
                    ' -y {{ execution_date.year }}'
                    ' -m {{ execution_date.month }}'
                    ' -o normal'),
    },
    'data_quality': {
        'project_id': 'poc-plazi-raw',
        'dataset': 'apis',
        'table': 'tb_diostats_publications_stats',
        'config_empty_check': True,
        'config_duplicates_check': False,
        'duplicates_table_key': ['DocDocId', 'DocSubjectId']
    }
}

##################
# PYTHON FUNCTION
##################
def make_authorized_get_request(**op_kwargs):
    """
    make_authorized_get_request makes a GET request to the specified HTTP endpoint
    in service_url (must be a complete URL) by authenticating with the
    ID token obtained from the google-auth client library.
    """
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, op_kwargs["service_url"])

    return id_token


def pass_status(response):

    return False if response.status_code != 200 else True


##########
# TASKS
##########

auth_token_function_api_treatment_bank_stats = PythonOperator(
  task_id="auth_token_function_api_{}".format(params.get('name')),
  python_callable=make_authorized_get_request,
  op_kwargs={"service_url": (params.get('app_engine').get('base_url')
                                .format(name=params.get('app_engine').get('name')))},
  provide_context=True,
  dag=dag
)

call_function_api = SimpleHttpOperator(
    task_id='call_function_api_{}'.format(params.get('name')),
    http_conn_id=params.get('conn_id').get('http'),
    method='POST',
    endpoint=params.get('app_engine').get('name'),
    data=json.dumps({
            "endpoint": params.get('app_engine').get('config').get('endpoint'),
            "table": params.get('app_engine').get('config').get('table'),
            "load_mode": params.get('app_engine').get('config').get('load_mode'),
            "zone": params.get('app_engine').get('config').get('zone'),
            "project_id": params.get('app_engine').get('config').get('project_id'),
            "dataset": params.get('app_engine').get('config').get('dataset'),
            "table_name": params.get('app_engine').get('config').get('table_name'),
            "delta_fields": params.get('app_engine').get('config').get('delta_fields')
        }),
    response_check=lambda response: response.status_code == 200,
    response_filter=pass_status,
    headers={"Authorization": "Bearer {}".format("{{ task_instance.xcom_pull(task_ids='auth_token_function_api_" + params.get('name') + "', key='return_value') }}"),
             "Content-Type": "application/json"},
    do_xcom_push=True,
    dag=dag)

gce_start = ComputeEngineStartInstanceOperator(
    task_id='gce_start_{}'.format(params.get('name')),
    gcp_conn_id=params.get('conn_id').get('gcp'),
    project_id=params.get('vm_instance').get('project_id'),
    zone=params.get('vm_instance').get('zone'),
    resource_id=params.get('vm_instance').get('name'),
    trigger_rule='one_failed',
    dag=dag
)

run_script_gce = SSHOperator(
    task_id='run_script_gce_{}'.format(params.get('name')),
    ssh_hook=ComputeEngineSSHHook(
        gcp_conn_id=params.get('conn_id').get('gcp'),
        instance_name=params.get('vm_instance').get('name'),
        zone=params.get('vm_instance').get('zone'),
        project_id=params.get('vm_instance').get('project_id'),
        use_oslogin=True,
        use_iap_tunnel=False,
    ),
    do_xcom_push=False,
    command=params.get('vm_instance').get('command'),
    dag=dag
)

gce_stop = ComputeEngineStopInstanceOperator(
    task_id='gce_stop_{}'.format(params.get('name')),
    gcp_conn_id=params.get('conn_id').get('gcp'),
    project_id=params.get('vm_instance').get('project_id'),
    zone=params.get('vm_instance').get('zone'),
    resource_id=params.get('vm_instance').get('name'),
    dag=dag
)

data_quality = BigQueryOperator(
    task_id='data_quality_{}'.format(params.get('name')),
    sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
        project_id=params.get('data_quality').get('project_id'),
        dataset=params.get('data_quality').get('dataset'),
        table=params.get('data_quality').get('table'),
        config_empty_check=params.get('data_quality').get('config_empty_check'),
        config_duplicates_check=params.get('data_quality').get('config_duplicates_check'),
        duplicates_table_key=params.get('data_quality').get('duplicates_table_key')
    ),
    use_legacy_sql=False,
    priority="BATCH",
    do_xcom_push=False,
    trigger_rule='one_success',
    dag=dag
)

#############
# SCHEDULING
#############

auth_token_function_api_treatment_bank_stats >> call_function_api
call_function_api >> [data_quality, gce_start]
gce_start >> run_script_gce
run_script_gce >> gce_stop
run_script_gce >> data_quality