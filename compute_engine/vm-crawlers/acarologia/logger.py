from google.cloud import bigquery

from datetime import datetime
import logging

from os import environ

# environ["GOOGLE_APPLICATION_CREDENTIALS"]='config/key.json'

client = bigquery.Client()

service = 'Compute Engine'

log_table_config = {
    'project_id': 'data-tools-prod-336318',
    'dataset': 'logs',
    'table': 'crawlers'
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

    insert_errors = client.insert_rows(log_table, log_data)

    if insert_errors == []:
        logging.info("SUCCESS! No errors where found in the process `{process}` "
                    "from service {service} "
                    "on {date}".format(process='ACAROLOGIA',
                                       service=service,
                                       date=datetime.now().strftime('%Y-%m-%d')))

    else:
        logging.info(("ERROR: Something happened while logging process `{process}` "
                      "from service {service} on {date}. Error messages "
                      "available: {error}".format(process='ACAROLOGIA',
                                                  service=service,
                                                  date=datetime.now().strftime('%Y-%m-%d'),
                                                  error=insert_errors)))