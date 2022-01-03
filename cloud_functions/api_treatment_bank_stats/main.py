from google.cloud import bigquery

from datetime import datetime
import logging

# from config import query, delta_fields
from config import config
from helper import prepare_data, querying_data
from logger import log

# Instantiate a BigQuery client
client = bigquery.Client()

def get_tb_data(params, log_data, delta_fields, query, fields):
    """Writes data to BigQuery.

    Args:
        params (dict): Dictionary with the params recovered from the
        HTTP request.
        log_data (dict): Pre-filled dictionary containing data to log
        into BigQuery.

    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # Getting the data to compose the BigQuery table id
    table_id = ("{project_id}.{dataset}.{table}"
                    .format(project_id=params.get('project_id'),
                            dataset=params.get('dataset'),
                            table=params.get('table_name')))

    # Try/Except trying to connect to a BigQuery table
    try:
        logging.info('MAIN.py: Attempting to connect to BigQuery Table.')
        ## Get Biq Query Set up
        table = client.get_table(table_id)
        logging.info(f'MAIN.py: Connecting established succesfully. Table object: {table}')
    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = (f"Error {e.__class__.__name__} tyring to connect"
                                " with the BigQuery table.")
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Returning status
        raise ValueError((f"Error {e.__class__.__name__} tyring to connect"
                            " with the BigQuery table."))

    # Try/Except trying to parse the delta fields provided
    try:
        # Defining the delta and inserting it to the query
        delta_filter_delimiter = '&FP-'
        delta_filter = ''

        for delta_field in delta_fields:
            if params.get('delta_fields') != None:
                if delta_field['name'] in ([field['name'] for field
                                                in params.get('delta_fields')]):
                    delta_filter += (delta_filter_delimiter +
                                    delta_field['code'] +
                                    '=' +
                                    ([field['value'] for field
                                            in params.get('delta_fields')
                                            if field['name'] == delta_field['name']][0]))
            else:
                break

        # delta_filter.replace('"', ).replace('-', )

        url = query.format(delta=delta_filter,
                           format='JSON')
    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = (f"Error {e.__class__.__name__} tyring to parse"
                                " the provided delta fields.")
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Returning status
        raise ValueError((f"Error {e.__class__.__name__} tyring to parse"
                            " the provided delta fields."))

    # Try/Except trying to make the HTTP request to the API
    try:
        logging.info('MAIN.py: Starting the HTTP Request...')
        # Making the HTTP request
        fetched_data = querying_data(log_data, url)
        logging.info('MAIN.py: HTTP Request made, here are the results: rows fetched: {}.'.format(len(fetched_data)))

    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = (f"Error {e.__class__.__name__} tyring to make "
                                "the HTTP request.")
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Returning status
        raise ValueError((f"Error {e.__class__.__name__} tyring to make "
                            "the HTTP request."))

    try:
        logging.info('MAIN.py: Calling the prepare_data() function.')
        data = prepare_data(fetched_data, fields)
        logging.info('MAIN.py: Data has returned! There are {} parsed rows'.format(len(data)))

    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = f"Error trying to parse the data: {str(e)}"
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Returning status
        raise ValueError(f"Error trying to parse the data: {str(e)}")

    # Starting the process to insert fetched and parsed data into BigQuery
    if len(data) == 0:
        logging.info('MAIN.py: Entered the conditional with zero rows in the data dump...')
        # Setting up log message
        log_data['status'] = "Skipped"
        log_data['response'] = ''
        log_data['message'] = 'No data returned'
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        logging.info('MAIN.py: Calling the log function with zero rows in the data dump...')
        log(log_data)

    else:
        # Try/Except trying to make the bulk insert into BigQuery
        try:
            # Make an API request.
            logging.info('MAIN.py: If the data dump has rows, if felt here.')
            insert_errors = client.insert_rows(table, data)

            if insert_errors == []:
                # Setting up log message
                log_data['status'] = "Success"
                log_data['response'] = ''
                log_data['message'] = ("A total of {num} rows have "
                                        "been added".format(num=len(data)))
                log_data['num_rows_inserted'] = len(data)
                log_data['ran_at'] = str(datetime.now())

                # Logging message
                logging.info('MAIN.py: Calling the log function with SOME {} rows in the data dump...'.format(len(data)))
                log(log_data)


            else:
                logging.info('MAIN.py: Error on BigQuery, falling here...')
                # Setting up log message
                log_data['status'] = "Failed"
                log_data['response'] = insert_errors
                log_data['message'] = f"Errors from BQ."
                log_data['ran_at'] = str(datetime.now())

                # Logging message
                logging.info('MAIN.py: Calling the log function because of an error in BigQuery.')
                log(log_data)

                # Returning status
                raise ValueError(f"Errors from BQ: {insert_errors}")

        except Exception as e:
            # Setting up log message
            log_data['status'] = "Failed"
            log_data['response'] = str(e)
            log_data['message'] = (f"Error {e.__class__.__name__} tyring to make "
                                    "the bulk insert.")
            log_data['ran_at'] = str(datetime.now())

            # Logging message
            log(log_data)

            # Returning status
            raise ValueError((f"Error {e.__class__.__name__} tyring to make"
                                " the bulk insert."))


def main(request):
    """Responds to any HTTP request.

    Args:
        request (flask.Request): HTTP request object.

    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    # Getting the data from the request
    params = request.get_json()

    endpoint_and_query = config.get(params.get('endpoint')).get(params.get('table'))
    delta_fields = endpoint_and_query.get('delta_fields')
    query = endpoint_and_query.get('query')
    fields = endpoint_and_query.get('fields')

    config_params = {
        'endpoint': params.get('endpoint'),
        'table': params.get('table')
    }

    # Create log data
    log_data = {
        'process': 'tb_api_treatments_stats',
        'load_mode': params.get('load_mode'),
        'zone': params.get('zone'),
        'project_id': params.get('project_id'),
        'dataset': params.get('dataset'),
        'table': params.get('table_name'),
        'start_process_date': str(datetime.today().strftime('%Y-%m-%d')),
        'status': '',
        'response': '',
        'message': '',
        'num_rows_inserted': 0,
        'ran_at': ''
    }

    try:
        get_tb_data(params, log_data, delta_fields, query, fields)

    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = (f"Error {e.__class__.__name__} tyring to run "
                                "the Cloud Function.")
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Raising exception
        raise ValueError((f"Error {e.__class__.__name__} tyring to run "
                           "the Cloud Function."))