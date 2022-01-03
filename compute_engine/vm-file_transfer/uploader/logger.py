from google.cloud import bigquery

from datetime import datetime
import logging

client = bigquery.Client()

service = 'Compute Engine'

log_table_config = {
    'project_id': 'data-tools-prod-336318',
    'dataset': 'logs',
    'table': 'file_lifespan'
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
            log_data.get('service'),
            log_data.get('process'),
            log_data.get('filepath_service'),
            log_data.get('filepath_project_id'),
            log_data.get('filepath_instance_name'),
            log_data.get('filepath'),
            log_data.get('filename'),
            log_data.get('filesize'),
            log_data.get('nature_of_file'),
            log_data.get('destination'),
            log_data.get('start_process_date'),
            log_data.get('status'),
            log_data.get('response'),
            log_data.get('message'),
            log_data.get('logged_from'), # as logged_from,
            log_data.get('ran_at') if log_data.get('ran_at') != '' else str(datetime.now())
        )
    ]

    insert_errors = client.insert_rows(log_table, data)

    if insert_errors == []:
        logging.info("SUCCESS! No errors where found in the process `{process}` "
                    "from service {service} "
                    "on {date}".format(process=log_data.get('process'),
                                       service=service,
                                       date=log_data.get('date')))

    else:
        logging.info(("ERROR: Something happened while logging process `{process}` "
                      "from service {service} on {date}. Error messages "
                      "available: {error}".format(process=log_data.get('process'),
                                                  service=service,
                                                  date=log_data.get('date'),
                                                  error=insert_errors)))