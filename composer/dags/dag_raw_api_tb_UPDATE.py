from airflow import DAG

from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (ComputeEngineStartInstanceOperator,
                                                              ComputeEngineStopInstanceOperator)

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import urllib
import json

import google.auth.transport.requests
import google.oauth2.id_token

from libs.log import log


#############
# PARAMETERS
#############
dag_id = 'dag_raw_api_tb_UPDATE'

vm_config = {
    'conn_id': 'compute_engine_conn',
    'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318'
        },
    }

list_of_params = [
    {
        'name': 'tb_srsstats_treatments_stats',
        'primary_key': 'DocUuid',
        'conn_id': {
            'http': 'data-tools-prd_functions',
            'gcp': 'compute_engine_conn',
        },
        'app_engine': {
            'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
            'name': 'api_treatment_bank_stats',
            'config': {
                'endpoint': 'srsStats',
                'table': 'basic_stats',
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_srsstats_treatments_stats_aux",
                "delta_fields": [{
                        "name": "DocUpdateDate",
                        "value": '"{{ prev_ds }}"-"{{ ds }}"'
                    }]
            }
        },
        'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318',
            'command': ('sudo python3 ~/tb_stats/run.py'
                        ' -t DocUpdateDate'
                        ' -e srsStats'
                        ' -b basic_stats'
                        ' -s {{ prev_execution_date.strftime("%Y-%m-%d") }}'
                        ' -f {{ execution_date.strftime("%Y-%m-%d") }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_srsstats_treatments_stats',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['DocUuid']
        }
    },
    {
        'name': 'tb_srsstats_materials_citation',
        'primary_key': 'MatCitId',
        'conn_id': {
            'http': 'data-tools-prd_functions',
            'gcp': 'compute_engine_conn',
        },
        'app_engine': {
            'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
            'name': 'api_treatment_bank_stats',
            'config': {
                'endpoint': 'srsStats',
                'table': 'mat_citation',
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_srsstats_materials_citation_aux",
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
                        ' -e srsStats'
                        ' -b mat_citation'
                        ' -s {{ prev_execution_date.strftime("%Y-%m-%d") }}'
                        ' -f {{ execution_date.strftime("%Y-%m-%d") }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_srsstats_materials_citation',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['MatCitId']
        }
    },
    {
        'name': 'tb_srsstats_treatment_citation',
        'primary_key': 'TreatCitCitId',
        'conn_id': {
            'http': 'data-tools-prd_functions',
            'gcp': 'compute_engine_conn',
        },
        'app_engine': {
            'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
            'name': 'api_treatment_bank_stats',
            'config': {
                'endpoint': 'srsStats',
                'table': 'treat_citation',
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_srsstats_treatment_citation_aux",
                "delta_fields": [{
                        "name": "DocUpdateDate",
                        "value": '"{{ prev_ds }}"-"{{ ds }}"'
                    }]
            }
        },
        'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318',
            'command': ('sudo python3 ~/tb_stats/run.py'
                        ' -t DocUpdateDate'
                        ' -e srsStats'
                        ' -b treat_citation'
                        ' -s {{ prev_execution_date.strftime("%Y-%m-%d") }}'
                        ' -f {{ execution_date.strftime("%Y-%m-%d") }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_srsstats_treatment_citation',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['TreatCitCitId']
        }
    },
    {
        'name': 'tb_diostats_publications_stats',
        'primary_key': 'DocArticleUuid',
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
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_diostats_publications_stats_aux",
                "delta_fields": [{
                        "name": "DocUpdateDate",
                        "value": '"{{ prev_ds }}"-"{{ ds }}"'
                    }]
            }
        },
        'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318',
            'command': ('sudo python3 ~/tb_stats/run.py'
                        ' -t DocUpdateDate'
                        ' -e dioStats'
                        ' -b basic_stats'
                        ' -s {{ prev_ds }}'
                        ' -f {{ ds }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_diostats_publications_stats',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['DocArticleUuid']
        }
    },
    {
        'name': 'tb_diostats_publications_authors',
        'primary_key': 'DocArticleUuid',
        'conn_id': {
            'http': 'data-tools-prd_functions',
            'gcp': 'compute_engine_conn',
        },
        'app_engine': {
            'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
            'name': 'api_treatment_bank_stats',
            'config': {
                'endpoint': 'dioStats',
                'table': 'authors',
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_diostats_publications_authors_aux",
                "delta_fields": [{
                        "name": "DocUpdateDate",
                        "value": '"{{ prev_ds }}"-"{{ ds }}"'
                    }]
            }
        },
        'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318',
            'command': ('sudo python3 ~/tb_stats/run.py'
                        ' -t DocUpdateDate'
                        ' -e dioStats'
                        ' -b authors'
                        ' -s {{ prev_ds }}'
                        ' -f {{ ds }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_diostats_publications_authors',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['DocArticleUuid, AuthName']
        }
    },
    {
        'name': 'tb_ephstats_stats',
        'primary_key': 'DocDocId',
        'conn_id': {
            'http': 'data-tools-prd_functions',
            'gcp': 'compute_engine_conn',
        },
        'app_engine': {
            'base_url': 'https://us-central1-data-tools-prod-336318.cloudfunctions.net/{name}',
            'name': 'api_treatment_bank_stats',
            'config': {
                'endpoint': 'ephStats',
                'table': 'basic_stats',
                'load_mode': 'UPDATE',
                'zone': 'RAW',
                "project_id": "poc-plazi-raw",
                "dataset": "aux",
                "table_name": "tb_ephstats_stats_aux",
                "delta_fields": [{
                        "name": "DocUpdateDate",
                        "value": '"{{ prev_ds }}"-"{{ ds }}"'
                    }]
            }
        },
        'vm_instance': {
            'name': 'instance-1',
            'zone': 'us-central1-a',
            'project_id': 'data-tools-prod-336318',
            'command': ('sudo python3 ~/tb_stats/run.py'
                        ' -t DocUpdateDate'
                        ' -e ephStats'
                        ' -b basic_stats'
                        ' -s {{ prev_ds }}'
                        ' -f {{ ds }}'
                        ' -y {{ execution_date.year }}'
                        ' -m {{ execution_date.month }}'
                        ' -o aux'),
        },
        'data_quality': {
            'project_id': 'poc-plazi-raw',
            'dataset': 'apis',
            'table': 'tb_ephstats_stats',
            'config_empty_check': True,
            'config_duplicates_check': False,
            'duplicates_key': ['DocDocId']
        }
    }
]

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


def check_status_function(**kwargs):

    list_of_tasks = list()

    for params in list_of_params:
        status = kwargs['ti'].xcom_pull(task_ids=['{task_group}.call_function_api_{task_name}'.format(task_group='cloud_function_task_group',
                                                                                                    task_name=params.get('name'))],
                                        key='return_value')[0]

        if status is None:
            if kwargs['option'] == 'scripts':
                list_of_tasks.append('{task_group}.run_script_gce_{task_name}'.format(task_group='cloud_engine_task_group',
                                                                                      task_name=params.get('name')))
            elif kwargs['option'] == 'vm':
                return '{task_group}.gce_start_{dag_id}'.format(task_group='cloud_engine_task_group',
                                                                              dag_id=dag_id)

    if len(list_of_tasks) > 0:
        return list_of_tasks

    else:
        return '{task_group}.dummy_task_{dag_id}'.format(task_group='cloud_engine_task_group',
                                                         dag_id=dag_id)


def define_dataquality(**kwargs):
    return kwargs['dataquality']


##############
# DECLARE DAG
##############

default_args = {
    'owner': "Marcus Guidoti | Plazi's Data Engineer",
    'on_success_callback': log,
    'on_failure_callback': log,
    'on_retry_callback': log,
    'sla_miss_callback': log,
    'start_date': "2021-12-30",
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('dag_raw_api_tb_UPDATE',
        default_args=default_args,
        description='UPDATE load on Treatment Bank tables with daily dagruns.',
        # schedule_interval=None,
        schedule_interval="0 2 * * *",
        tags=['raw', 'api', 'treatment bank', 'UPDATE', 'production', '@daily'],
        max_active_runs=1
        ) as dag:

##########
# TASKS
##########
    with TaskGroup('update_raw_task_group') as tg_update_raw:

        load_processes = dict()

        for params in list_of_params:

            load_process = BigQueryOperator(
                task_id='load_{}'.format(params.get('name')),
                sql='CALL `poc-plazi-raw.apis.update_raws`("{project_id}", {sandbox_id}, "{dataset}", "{table}", "{primary_key}", "{load_mode}")'.format(
                    project_id='poc-plazi-raw',
                    sandbox_id='Null',
                    dataset='apis',
                    table=params.get('name'),
                    primary_key=params.get('primary_key'),
                    load_mode='UPDATE',
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            data_quality = BigQueryOperator(
                task_id='data_quality_{}'.format(params.get('name')),
                sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                    project_id=params.get('data_quality').get('project_id'),
                    dataset=params.get('data_quality').get('dataset'),
                    table=params.get('data_quality').get('table'),
                    config_empty_check=params.get('data_quality').get('config_empty_check'),
                    config_duplicates_check=params.get('data_quality').get('config_duplicates_check'),
                    duplicates_table_key=params.get('data_quality').get('duplicates_key')
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            truncate_aux = BigQueryOperator(
                task_id='truncate_{}_aux'.format(params.get('name')),
                sql='TRUNCATE TABLE `{project_id}.{dataset}.{table}_aux`'.format(
                    project_id='poc-plazi-raw',
                    dataset='aux',
                    table=params.get('name')
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            load_processes[params.get('name')] = load_process

            load_process >> data_quality >> truncate_aux

    with TaskGroup('data_quality_aux_task_group') as tg_aux_dataquality:
        # list_of_dataquality = list()
        aux_dataqualities = dict()

        for params in list_of_params:
            data_quality = BigQueryOperator(
                task_id='data_quality_{}_aux'.format(params.get('name')),
                sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                    project_id=params.get('data_quality').get('project_id'),
                    dataset='aux',
                    table=params.get('data_quality').get('table') + '_aux',
                    # config_empty_check=params.get('data_quality').get('config_empty_check'),
                    config_empty_check=False,
                    # config_duplicates_check=params.get('data_quality').get('config_duplicates_check'),
                    config_duplicates_check=True,
                    duplicates_table_key=params.get('data_quality').get('duplicates_key')
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False,
                trigger_rule='one_success'
            )

            # list_of_dataquality.append(data_quality)
            aux_dataqualities[params.get('name')] = data_quality

            data_quality >> load_processes[params.get('name')]

    with TaskGroup('cloud_function_task_group') as tg_cloud_function:

        list_of_function_calls = list()

        for params in list_of_params:

            auth_token = PythonOperator(
            task_id="auth_token_function_api_{}".format(params.get('name')),
            python_callable=make_authorized_get_request,
            op_kwargs={"service_url": (params.get('app_engine').get('base_url')
                                            .format(name=params.get('app_engine').get('name')))},
            provide_context=True,
            dag=dag
            )

            call_function = SimpleHttpOperator(
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
                headers={"Authorization": "Bearer {}".format("{{ task_instance.xcom_pull(task_ids='cloud_function_task_group.auth_token_function_api_" + params.get('name') + "', key='return_value') }}"),
                        "Content-Type": "application/json"},
                do_xcom_push=True,
                dag=dag)

            list_of_function_calls.append(call_function)

            auth_token >> call_function
            call_function >> aux_dataqualities[params.get('name')]


    with TaskGroup('cloud_engine_task_group') as tg_cloud_engine:

        dummy_task = DummyOperator(
                task_id='dummy_task_{}'.format(dag_id),
                dag=dag
            )

        list_of_script_tasks = [dummy_task]
        list_of_script_tasks_without_dummy = list()

        for params in list_of_params:
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
                pool='avoid_concurrency_gce'
            )

            list_of_script_tasks.append(run_script_gce)
            list_of_script_tasks_without_dummy.append(run_script_gce)

            run_script_gce >> aux_dataqualities[params.get('name')]

        branch_decider = BranchPythonOperator(
                task_id='branch_decider_{}'.format(dag_id),
                python_callable=check_status_function,
                provide_context=True,
                op_kwargs={
                    'option': 'vm'
                },
                trigger_rule='all_done',
                dag=dag
            )

        gce_start = ComputeEngineStartInstanceOperator(
            task_id='gce_start_{}'.format(dag_id),
            gcp_conn_id=vm_config.get('conn_id'),
            project_id=vm_config.get('vm_instance').get('project_id'),
            zone=vm_config.get('vm_instance').get('zone'),
            resource_id=vm_config.get('vm_instance').get('name')
        )

        gce_stop = ComputeEngineStopInstanceOperator(
            task_id='gce_stop_{}'.format(dag_id),
            gcp_conn_id=vm_config.get('conn_id'),
            project_id=vm_config.get('vm_instance').get('project_id'),
            zone=vm_config.get('vm_instance').get('zone'),
            resource_id=vm_config.get('vm_instance').get('name'),
            trigger_rule="one_success"
        )

        branch_task = BranchPythonOperator(
                task_id='branch_task_{}'.format(dag_id),
                python_callable=check_status_function,
                provide_context=True,
                op_kwargs={
                    'option': 'scripts'
                },
            )

        list_of_function_calls >> branch_decider
        branch_decider >> [dummy_task, gce_start]
        gce_start >> branch_task
        gce_start >> gce_stop
        branch_task >> list_of_script_tasks
        list_of_script_tasks_without_dummy >> gce_stop

    # tg_cloud_function >> tg_cloud_engine