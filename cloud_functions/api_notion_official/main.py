from google.cloud import bigquery

from os import environ
from datetime import datetime
import logging

from config import base_url, dict_of_fields
from helper import prepare_data, querying_data
from logger import log


# Instantiate a BigQuery client
client = bigquery.Client()

token = environ.get('TOKEN', '')


def get_notion_data(params, fields, log_data):
    """Responds to any HTTP request.

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
                            table=params.get('table')))

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


    # Try/Except trying to make the HTTP request to the API
    try:
        logging.info('MAIN.py: Starting the HTTP Request...')
        headers = {
            'Authorization': 'Bearer {token}'.format(token=token),
            'Notion-Version': '2021-08-16'
        }

        url = base_url.format(endpoint=params.get('endpoint'))

        # Attempt to recover the payload from the provided params
        payload = params.get('payload', dict())

        logging.info(f'MAIN.py: Payload defined: {payload}')

        # Making the HTTP request
        (has_more,
        next_cursor,
        fetched_data) = querying_data(log_data=log_data,
                                      url=url,
                                      headers=headers,
                                      payload=payload)

        logging.info('MAIN.py: HTTP Request made, here are the results: has_more: {}; next_cursor: {}; Rows fetched: {}.'.format(has_more, next_cursor, len(fetched_data)))

        while has_more == True:

            logging.info('MAIN.py: Program has entered the while loop...')

            # Delete start_cursor from Payload to guarantee that we won't repeat the query
            payload.pop('start_cursor', None)

            if next_cursor != None:
                payload["start_cursor"] = next_cursor

            logging.info('MAIN.py: Payload redefined to: {}'.format(payload))

            # Making the HTTP request
            (has_more,
            next_cursor,
            fetched_new_data) = querying_data(log_data=log_data,
                                                url=url,
                                                headers=headers,
                                                payload=payload)


            logging.info('MAIN.py: HTTP Request made again, here the results: has_more: {}; next_cursor: {}; Rows fetched: {}.'.format(has_more, next_cursor, len(fetched_data)))

            fetched_data.extend(fetched_new_data)

            logging.info('MAIN.py: New data has been fetched! Rows: {}'.format(len(fetched_new_data)))
            logging.info('MAIN.py: Total of rows in the data dump so far... {}'.format(len(fetched_data)))

            # Perhaps this isn't necessary since the helper
            # function will raise an exception case the status_code is different
            # than a 200
            # has_more = False

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

    # Try/Except clause to attempt to transform data
    try:
        logging.info('MAIN.py: Calling the prepare_data() function.')
        data = prepare_data(fetched_data,
                            fields)
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

    filtered_fields = dict_of_fields.get(params.get('name'))

    # Create log data
    log_data = {
        'process': 'api_notion_official',
        'load_mode': params.get('load_mode'),
        'zone': params.get('zone'),
        'project_id': params.get('project_id'),
        'dataset': params.get('dataset'),
        'table': params.get('table'),
        'start_process_date': str(datetime.today().strftime('%Y-%m-%d')),
        'status': '',
        'response': '',
        'message': '',
        'num_rows_inserted': 0,
        'ran_at': ''
    }

    try:
        get_notion_data(params, filtered_fields, log_data)

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