from google.cloud import bigquery

import logging
from datetime import datetime

client = bigquery.Client()

service = 'Cloud Function'

log_table_config = {
    'project_id': 'data-tools-prod-336318',
    'dataset': 'logs',
    'table': 'ingestion'
}

def log(log_data):
    """[summary]

    Args:
        log_data ([type]): [description]

    Returns:
        [type]: [description]
    """

    logging.info('LOGGER.py: Arrived at the custom log function.')

    # Getting the data to compose the BigQuery table id
    log_table_id = ("{project_id}.{dataset}.{table}"
                        .format(project_id=log_table_config.get('project_id'),
                                dataset=log_table_config.get('dataset'),
                                table=log_table_config.get('table')))

    ## Get Biq Query Set up
    log_table = client.get_table(log_table_id)

    data = [
        (
            service,
            log_data.get('process'),
            log_data.get('load_mode'),
            log_data.get('zone'),
            log_data.get('project_id'),
            log_data.get('dataset'),
            log_data.get('table'),
            log_data.get('start_process_date'),
            log_data.get('status'),
            log_data.get('response'),
            log_data.get('message'),
            log_data.get('num_rows_inserted'),
            service, # as logged_from,
            log_data.get('ran_at') if log_data.get('ran_at') != '' else str(datetime.now())
        )
    ]

    insert_errors = client.insert_rows(log_table, data)

    if insert_errors == []:
        print("SUCCESS! No errors where found in the process `{process}` "
                    "from service {service} "
                    "on {date}".format(process=log_data.get('process'),
                                       service=service,
                                       date=log_data.get('date')))

    else:
        print(("ERROR: Something happened while logging process `{process}` "
                      "from service {service} on {date}. Error messages "
                      "available: {error}".format(process=log_data.get('process'),
                                                  service=service,
                                                  date=log_data.get('date'),
                                                  error=insert_errors)))