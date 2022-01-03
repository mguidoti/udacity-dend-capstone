from google.cloud import bigquery

from datetime import datetime
import logging

# from config import query, delta_fields, params
from config import config
from helper import prepare_data, querying_data
from logger import log

import argparse
from os.path import dirname

# Defines the arguments
parser = argparse.ArgumentParser()
parser.add_argument('-t', '--type', help='The type of field (either Upload or Update) in which we will query the API.')
parser.add_argument('-y', '--year', help='The year in which we will query the API.')
parser.add_argument('-m', '--month', help='The month in which we will query the API.')
parser.add_argument('-e', '--endpoint', help='Select the proper endpoint.')
parser.add_argument('-b', '--table', help='Select the query.')
parser.add_argument('-s', '--start', help='Initial date.')
parser.add_argument('-f', '--final', help='Final date.')
parser.add_argument('-o', '--option', help='Option of the table. Could be aux, to indicate the table destination of the auxiliary table.')
args = parser.parse_args()


# Creates a log file
logging.basicConfig(filename='{path}/logs/{today}.log'.format(path=dirname(__file__), today=datetime.now().strftime('%Y-%m-%d-%H%M%S')),
#                     # encoding='utf-8',
                    level=logging.INFO)

logging.getLogger().addHandler(logging.StreamHandler())

vm_instance='instance-1'


# Instantiate a BigQuery client
client = bigquery.Client()


def get_tb_data(params, log_data, delta_fields, fields, query):
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
                            table=params.get('table')))

    # Try/Except trying to connect to a BigQuery table
    try:
        logging.info('{vm_instance}-MAIN.py: Attempting to connect to BigQuery Table.'.format(vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: Attempting to connect to BigQuery Table.'.format(vm_instance=vm_instance))
        ## Get Biq Query Set up
        table = client.get_table(table_id)
        logging.info(f'{vm_instance}-MAIN.py: Connecting established succesfully. Table object: {table}')
        # print(f'{vm_instance}-MAIN.py: Connecting established succesfully. Table object: {table}')
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
        print(delta_filter)
        
        # delta_filter.replace('"', ).replace('-', )

        url = query.format(delta=delta_filter,
                           format='JSON')
        print(url)
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
        logging.info('{vm_instance}-MAIN.py: Starting the HTTP Request...')
        # print('{vm_instance}-MAIN.py: Starting the HTTP Request...')
        # Making the HTTP request
        fetched_data = querying_data(log_data, url)
        logging.info('{vm_instance}-MAIN.py: HTTP Request made, here are the results: rows fetched: {}.'.format(len(fetched_data),
                                                                                                                vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: HTTP Request made, here are the results: rows fetched: {}.'.format(len(fetched_data),
                                                                                                                # vm_instance=vm_instance))

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
        logging.info('{vm_instance}-MAIN.py: Calling the prepare_data() function.'.format(vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: Calling the prepare_data() function.'.format(vm_instance=vm_instance))
        data = prepare_data(fetched_data, fields)
        logging.info('{vm_instance}-MAIN.py: Data has returned! There are {} parsed rows'.format(len(data),
                                                                                                vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: Data has returned! There are {} parsed rows'.format(len(data),
                                                                                                # vm_instance=vm_instance))

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
        logging.info('{vm_instance}-MAIN.py: Entered the conditional with zero rows in the data dump...'.format(vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: Entered the conditional with zero rows in the data dump...'.format(vm_instance=vm_instance))
        # Setting up log message
        log_data['status'] = "Skipped"
        log_data['response'] = ''
        log_data['message'] = 'No data returned'
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        logging.info('{vm_instance}-MAIN.py: Calling the log function with zero rows in the data dump...'.format(vm_instance=vm_instance))
        # print('{vm_instance}-MAIN.py: Calling the log function with zero rows in the data dump...'.format(vm_instance=vm_instance))
        log(log_data)

    else:
        # Try/Except trying to make the bulk insert into BigQuery
        try:
            # Make an API request.
            logging.info('{vm_instance}-MAIN.py: If the data dump has rows, if felt here.'.format(vm_instance=vm_instance))
            # print('{vm_instance}-MAIN.py: If the data dump has rows, if felt here.'.format(vm_instance=vm_instance))

            logging.info('{vm_instance}-MAIN.py: Trying to divide the data in chunks of 1000 records, more or less...'.format(vm_instance=vm_instance))

            chunks = [data[x:x+1000] for x in range(0, len(data), 1000)]

            logging.info('{vm_instance}-MAIN.py: A total of {num_chunks} were created.'.format(vm_instance=vm_instance,
                                                                                                num_chunks=len(chunks)))

            for load_of_data in chunks:
                
                insert_errors = client.insert_rows(table, load_of_data)

            if insert_errors == []:
                # Setting up log message
                log_data['status'] = "Success"
                log_data['response'] = ''
                log_data['message'] = ("A total of {num} rows have "
                                        "been added".format(num=len(data)))
                log_data['num_rows_inserted'] = len(data)
                log_data['ran_at'] = str(datetime.now())

                # Logging message
                logging.info('{vm_instance}-MAIN.py: Calling the log function with SOME {} rows in the data dump...'.format(len(data),
                                                                                                                            vm_instance=vm_instance))
                # print('{vm_instance}-MAIN.py: Calling the log function with SOME {} rows in the data dump...'.format(len(data),
                                                                                                                            # vm_instance=vm_instance))
                log(log_data)


            else:
                logging.info('{vm_instance}-MAIN.py: Error on BigQuery, falling here...'.format(vm_instance=vm_instance))
                # print('{vm_instance}-MAIN.py: Error on BigQuery, falling here...'.format(vm_instance=vm_instance))
                # Setting up log message
                log_data['status'] = "Failed"
                log_data['response'] = insert_errors
                log_data['message'] = f"Errors from BQ."
                log_data['ran_at'] = str(datetime.now())

                # Logging message
                logging.info('{vm_instance}-MAIN.py: Calling the log function because of an error in BigQuery.'.format(vm_instance=vm_instance))
                # print('{vm_instance}-MAIN.py: Calling the log function because of an error in BigQuery.'.format(vm_instance=vm_instance))
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


def main():
    """Responds to any HTTP request.

    Args:
        request (flask.Request): HTTP request object.

    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # Parsing delta argument
    # args = parser.parse_args()
    query_type_field = vars(args).get('type')
    query_year = vars(args).get('year')
    query_month = vars(args).get('month')
    endpoint = vars(args).get('endpoint')
    t = vars(args).get('table')
    start = vars(args).get('start')
    final = vars(args).get('final')
    option = vars(args).get('option')

    endpoint_and_query = config.get(endpoint).get(t)
    delta_fields = endpoint_and_query.get('delta_fields')
    query = endpoint_and_query.get('query')
    fields = endpoint_and_query.get('fields')
    params = endpoint_and_query.get('params')

    if option == 'aux':
        params['dataset'] = 'aux'
        params['table'] = params.get('table') + '_aux'

    # Create log data
    log_data = {
        'process': 'instance-1',
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

    if query_type_field == 'DocUploadDate' or query_type_field == 'DocUpdateDate':
            params['delta_fields'] = [{ "name": query_type_field,
                                        "value": '"{start}"-"{final}"'.format(start=start, final=final) }]
    else:
        params['delta_fields'] = [{
                "name": "DocUploadYear" if query_type_field == 'Upload' else "DocUpdateYear",
                "value": str(int(query_year))
            },
            {
                "name": "DocUploadMonth",
                "value": str(int(query_month))
            }]


    try:
        get_tb_data(params, log_data, delta_fields, fields, query)

    except Exception as e:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = str(e)
        log_data['message'] = (f"Error {e.__class__.__name__} tyring to run "
                                "script in the vm instance.")
        log_data['ran_at'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Raising exception
        raise ValueError((f"Error {e.__class__.__name__} tyring to run "
                           "script in the {vm_instance}."))

if __name__ == "__main__":
    main()