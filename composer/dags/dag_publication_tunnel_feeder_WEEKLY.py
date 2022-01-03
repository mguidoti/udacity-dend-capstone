from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (ComputeEngineStartInstanceOperator,
                                                              ComputeEngineStopInstanceOperator)

from libs.operators.custom_bigquery_get_data import CustomBigqueryGetDataOperator

from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from libs.log import log

#############
# PARAMETERS
#############
dag_id = 'dag_publication_tunnel_WEEKLY'

vm_config = {
    'crawlers': {
        'name': 'vm-crawlers',
        'zone': 'us-central1-a',
        'project_id': 'data-tools-prod-336318',
        'conn_id': 'compute_engine_conn',
        'pool': 'avoid_concurrency_crawlers'
        },
    'file_transfer': {
        'name': 'instance-2',
        'zone': 'us-central1-a',
        'project_id': 'data-tools-prod-336318',
        'conn_id': 'compute_engine_conn',
        'pool': 'default_pool',
        'bucket_name': 'poc-plazi-publications-tunnel',
        'bucket_folder': 'pdfs'
    }
}

list_of_crawlers = [
    {
        'name': 'Acarologia',
        'command': 'crawlers/acarologia/run.py'
    }
]

###################
# PYTHON FUNCTIONS
###################
sql_raw_tunnel_check = """SELECT COUNT(*) > 0
                          FROM (
                              SELECT filename,
                                  status,
                                  inserted_at
                              FROM `poc-plazi-raw.platform.publications_tunnel`
                              WHERE DATE(inserted_at) = CURRENT_DATE()
                              AND status = 'SCRAPED'
                              UNION ALL
                              SELECT filename,
                                  status,
                                  inserted_at
                              FROM `poc-plazi-trusted.publications.tunnel`
                              WHERE status IN ('SCRAPED', 'DOWNLOADED')
                          )"""

###################
# PYTHON FUNCTIONS
###################
def check_raw_tunnel_status(**kwargs):
    status = kwargs['ti'].xcom_pull(task_ids=['{task_group}.check_new_data_on_raw_tunnel'.format(task_group='raw_tunnel_checker',)],
                                    key='status')

    print(status)
    print(type(status))
    print(status[0])

    if str(status[0]) == 'True':
        return '{task_group}.{task_id}'.format(task_group='update_trusted_after_crawlers',
                                               task_id='load_tunnel_trusted_after_crawlers')
    else:
        return 'dummy_finish_{}'.format(dag_id)

##############
# DECLARE DAG
##############


default_args = {
    'owner': "Marcus Guidoti | Plazi's Data Engineer",
    'on_success_callback': log,
    'on_failure_callback': log,
    'on_retry_callback': log,
    'sla_miss_callback': log,
    'start_date': "2022-01-01",
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('dag_publication_tunnel_WEEKLY',
        default_args=default_args,
        description='UPDATE load on Treatment Bank tables with daily dagruns.',
        # schedule_interval=None,
        schedule_interval="@weekly",
        tags=['tunnel', 'production', '@weekly'],
        max_active_runs=1
        ) as dag:

##########
# TASKS
##########

    dummy_finish = DummyOperator(
        task_id='dummy_finish_{}'.format(dag_id)
    )

    list_with_dummy = [dummy_finish]

    with TaskGroup('compute_engine_crawlers') as tg_crawlers:

        crawlers_vm_conf = vm_config.get('crawlers')

        gce_start = ComputeEngineStartInstanceOperator(
            task_id='gce_start_vm_crawler_{}'.format(dag_id),
            gcp_conn_id=crawlers_vm_conf.get('conn_id'),
            project_id=crawlers_vm_conf.get('project_id'),
            zone=crawlers_vm_conf.get('zone'),
            resource_id=crawlers_vm_conf.get('name')
        )

        gce_stop = ComputeEngineStopInstanceOperator(
            task_id='gce_stop_vm_crawler_{}'.format(dag_id),
            gcp_conn_id=crawlers_vm_conf.get('conn_id'),
            project_id=crawlers_vm_conf.get('project_id'),
            zone=crawlers_vm_conf.get('zone'),
            resource_id=crawlers_vm_conf.get('name'),
            trigger_rule="all_done"
        )

        data_quality_raw = data_quality = BigQueryOperator(
            task_id='data_quality_tunnel_raw_after_crawlers',
            sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                project_id='poc-plazi-raw',
                dataset='platform',
                table='publications_tunnel',
                config_empty_check=False,
                config_duplicates_check=True,
                duplicates_table_key=['filename', 'status', 'inserted_at']
            ),
            use_legacy_sql=False,
            priority="BATCH",
            do_xcom_push=False,
            trigger_rule='one_success'
        )

        with TaskGroup('crawlers_scripts') as subtg_crawlers_scripts:

            for crawler in list_of_crawlers:
                run_crawler_gce = SSHOperator(
                    task_id='run_crawler_gce_{}'.format(crawler.get('name')),
                    ssh_hook=ComputeEngineSSHHook(
                        gcp_conn_id=crawlers_vm_conf.get('conn_id'),
                        instance_name=crawlers_vm_conf.get('name'),
                        zone=crawlers_vm_conf.get('zone'),
                        project_id=crawlers_vm_conf.get('project_id'),
                        use_oslogin=True,
                        use_iap_tunnel=False,
                        expire_time=82400
                    ),
                    do_xcom_push=False,
                    command='sudo python3 {command}'.format(command=crawler.get('command')),
                    pool=crawlers_vm_conf.get('pool')
                )


        gce_start >> subtg_crawlers_scripts >> [data_quality_raw, gce_stop]


    with TaskGroup('update_trusted_after_crawlers') as tg_update_trusted_after_crawlers:

        load_process = BigQueryOperator(
            task_id='load_tunnel_trusted_after_crawlers',
            sql='CALL `poc-plazi-trusted.publications.load_trd_tunnel`("{project_id}", {sandbox_id}, "{dataset}", "{load_mode}")'.format(
                project_id='poc-plazi-trusted',
                sandbox_id='Null',
                dataset='publications',
                load_mode='FULL',
            ),
            use_legacy_sql=False,
            priority="BATCH",
            do_xcom_push=False
        )

        data_quality_on_destination = BigQueryOperator(
            task_id='data_quality_tunnel_trusted_after_crawlers',
            sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                project_id='poc-plazi-trusted',
                dataset='publications',
                table='tunnel',
                config_empty_check=True,
                config_duplicates_check=True,
                duplicates_table_key=['filename', 'status']
            ),
            use_legacy_sql=False,
            priority="BATCH",
            do_xcom_push=False
        )

        list_with_dummy.append(load_process)
        load_process >> data_quality_on_destination


    with TaskGroup('compute_engine_file_transfer') as tg_file_transfer:

        file_transfer_vm_conf = vm_config.get('file_transfer')

        gce_start = ComputeEngineStartInstanceOperator(
            task_id='gce_start_vm_crawler_{}'.format(dag_id),
            gcp_conn_id=file_transfer_vm_conf.get('conn_id'),
            project_id=file_transfer_vm_conf.get('project_id'),
            zone=file_transfer_vm_conf.get('zone'),
            resource_id=file_transfer_vm_conf.get('name')
        )

        downloader = SSHOperator(
            task_id='run_downloader_gce',
            ssh_hook=ComputeEngineSSHHook(
                gcp_conn_id=file_transfer_vm_conf.get('conn_id'),
                instance_name=file_transfer_vm_conf.get('name'),
                zone=file_transfer_vm_conf.get('zone'),
                project_id=file_transfer_vm_conf.get('project_id'),
                use_oslogin=True,
                use_iap_tunnel=False,
                expire_time=82400
            ),
            do_xcom_push=False,
            command='sudo python3 ~/scripts/downloader/run.py',
            pool=file_transfer_vm_conf.get('pool')
        )

        with TaskGroup('update_trusted_after_downloader') as subtg_update_trusted_after_downloader:
            load_process = BigQueryOperator(
                task_id='load_tunnel_trusted_after_downloader',
                sql='CALL `poc-plazi-trusted.publications.load_trd_tunnel`("{project_id}", {sandbox_id}, "{dataset}", "{load_mode}")'.format(
                    project_id='poc-plazi-trusted',
                    sandbox_id='Null',
                    dataset='publications',
                    load_mode='UPDATE',
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            data_quality_on_destination = BigQueryOperator(
                task_id='data_quality_tunnel_trusted_after_downloader',
                sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                    project_id='poc-plazi-trusted',
                    dataset='publications',
                    table='tunnel',
                    config_empty_check=True,
                    config_duplicates_check=True,
                    duplicates_table_key=['filename', 'status']
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            load_process >> data_quality_on_destination


        uploader = SSHOperator(
            task_id='run_uploader_gce',
            ssh_hook=ComputeEngineSSHHook(
                gcp_conn_id=file_transfer_vm_conf.get('conn_id'),
                instance_name=file_transfer_vm_conf.get('name'),
                zone=file_transfer_vm_conf.get('zone'),
                project_id=file_transfer_vm_conf.get('project_id'),
                use_oslogin=True,
                use_iap_tunnel=False,
                expire_time=82400
            ),
            do_xcom_push=False,
            command='sudo python3 ~/scripts/uploader/run.py',
            pool=file_transfer_vm_conf.get('pool')
        )


        with TaskGroup('update_trusted_after_uploader') as subtg_update_trusted_after_uploader:
            load_process = BigQueryOperator(
                task_id='load_tunnel_trusted_after_uploader',
                sql='CALL `poc-plazi-trusted.publications.load_trd_tunnel`("{project_id}", {sandbox_id}, "{dataset}", "{load_mode}")'.format(
                    project_id='poc-plazi-trusted',
                    sandbox_id='Null',
                    dataset='publications',
                    load_mode='UPDATE',
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

            data_quality_on_destination = BigQueryOperator(
                task_id='data_quality_tunnel_trusted_after_uploader',
                sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
                    project_id='poc-plazi-trusted',
                    dataset='publications',
                    table='tunnel',
                    config_empty_check=True,
                    config_duplicates_check=True,
                    duplicates_table_key=['filename', 'status']
                ),
                use_legacy_sql=False,
                priority="BATCH",
                do_xcom_push=False
            )

        mover = SSHOperator(
            task_id='run_mover_gce',
            ssh_hook=ComputeEngineSSHHook(
                gcp_conn_id=file_transfer_vm_conf.get('conn_id'),
                instance_name=file_transfer_vm_conf.get('name'),
                zone=file_transfer_vm_conf.get('zone'),
                project_id=file_transfer_vm_conf.get('project_id'),
                use_oslogin=True,
                use_iap_tunnel=False,
                expire_time=82400
            ),
            do_xcom_push=False,
            command='gsutil cp ~/scripts/uploader/uploaded/* gs://{bucket_name}/{bucket_folder}/'.format(
                bucket_name=file_transfer_vm_conf.get('bucket_name'),
                bucket_folder=file_transfer_vm_conf.get('bucket_folder')
            ),
            pool=file_transfer_vm_conf.get('pool')
        )


        gce_stop = ComputeEngineStopInstanceOperator(
            task_id='gce_stop_vm_crawler_{}'.format(dag_id),
            gcp_conn_id=file_transfer_vm_conf.get('conn_id'),
            project_id=file_transfer_vm_conf.get('project_id'),
            zone=file_transfer_vm_conf.get('zone'),
            resource_id=file_transfer_vm_conf.get('name'),
            trigger_rule="none_skipped"
        )

        # with TaskGroup('raw_tunnel_checker') as tg_raw_tunnel_checker:

        gce_start >> downloader >> subtg_update_trusted_after_downloader
        subtg_update_trusted_after_downloader >> uploader
        uploader >> [subtg_update_trusted_after_uploader, mover]
        [downloader, uploader, mover] >> gce_stop


    with TaskGroup('raw_tunnel_checker') as tg_raw_tunnel_checker:

        check_raw_tunnel = CustomBigqueryGetDataOperator(
            task_id="check_new_data_on_raw_tunnel",
            sql=sql_raw_tunnel_check,
            # keys=[]
        )

        branch_decider = BranchPythonOperator(
            task_id='branch_decider_{}'.format(dag_id),
            python_callable=check_raw_tunnel_status,
            provide_context=True
        )

        check_raw_tunnel >> branch_decider >> list_with_dummy


    tg_crawlers >> tg_raw_tunnel_checker
    tg_update_trusted_after_crawlers >> tg_file_transfer