from airflow import DAG

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from datetime import datetime, timedelta

from libs.log import log

list_of_pipelines = [
    {
        'name': 'ejt_basic_stats',
        'project_id': 'poc-plazi-dashboards',
        'dataset': 'ejt_v4',
        'table': 'ejt_publication_stats',
        'load_mode': 'UPDATE',
        'data_quality': {
            'on_source': [
                {
                    'name': 'treatment_bank_basic_stats',
                    'dag_id': 'dag_trusted_daily_UPDATE',
                    'task_id': 'data_quality_treatment_bank_basic_stats',
                    'airflow_tolerance_days': '1',
                    'airflow_config_check': True,
                    'airflow_allowed_states': ['success', 'skipped']
                }
            ],
            'on_destination': {
                'config_empty_check': True,
                'config_duplicates_check': True,
                'duplicates_table_key': ['article_uuid']
            }
        }
    }
]

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

dag = DAG('dag_dashboards_daily_UPDATE',
        default_args=default_args,
        description='UPDATE load on treatment_bank_basic_stats with daily dagruns.',
        # schedule_interval=None,
        schedule_interval="30 3 * * *",
        tags=['refined', 'UPDATE', 'production', '@daily'],
        max_active_runs=1
        )

for params in list_of_pipelines:

    list_of_on_source_data_quality = list()

    for source in params.get('data_quality').get('on_source'):

        data_quality_on_source = BigQueryOperator(
            task_id='data_quality_{}'.format(source.get('name')),
            sql='CALL `data-tools-prod-336318.dataquality.test_source`("{dag_id}", "{task_id}", {airflow_tolerance_days}, {config_airflow_check}, {airflow_allowed_states})'.format(
                dag_id=source.get('dag_id'),
                task_id=source.get('task_id'),
                airflow_tolerance_days=source.get('airflow_tolerance_days'),
                config_airflow_check=source.get('airflow_config_check'),
                airflow_allowed_states=source.get('airflow_allowed_states')
            ),
            use_legacy_sql=False,
            priority="BATCH",
            do_xcom_push=False,
            dag=dag
        )

        list_of_on_source_data_quality.append(data_quality_on_source)

    load_process = BigQueryOperator(
        task_id='load_{}'.format(params.get('name')),
        sql='CALL `poc-plazi-dashboards.ejt_v4.load_dsh_ejt_publication_stats`("{project_id}", {sandbox_id}, "{dataset}", "{load_mode}")'.format(
            project_id=params.get('project_id'),
            sandbox_id='Null',
            dataset=params.get('dataset'),
            load_mode=params.get('load_mode'),
        ),
        use_legacy_sql=False,
        priority="BATCH",
        do_xcom_push=False,
        dag=dag
    )

    data_quality_on_destination = BigQueryOperator(
        task_id='data_quality_{}'.format(params.get('name')),
        sql='CALL `data-tools-prod-336318.dataquality.test_destination`("{project_id}", "{dataset}", "{table}", {config_empty_check}, {config_duplicates_check}, {duplicates_table_key})'.format(
            project_id=params.get('project_id'),
            dataset=params.get('dataset'),
            table=params.get('table'),
            config_empty_check=params.get('data_quality').get('on_destination').get('config_empty_check'),
            config_duplicates_check=params.get('data_quality').get('on_destination').get('config_duplicates_check'),
            duplicates_table_key=params.get('data_quality').get('on_destination').get('duplicates_table_key')
        ),
        use_legacy_sql=False,
        priority="BATCH",
        do_xcom_push=False,
        dag=dag
    )

    list_of_on_source_data_quality >> load_process
    load_process >> data_quality_on_destination