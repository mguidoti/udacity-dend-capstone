import requests

from datetime import datetime
from dateutil import parser
from ast import literal_eval
import logging

from logger import log

def querying_data(log_data, url, headers, payload=None):
    """This helper function deals with the HTTP Request and follow up log in
    in case of success.

    Args:
        log_data ([type]): Dictionary containing the core data to be logged
        url (str): String to be used in the HTTP Request
        headers ([type]): Headers to be used in the HTTP Request
        payload ([type], optional): Payload to be used in the
        HTTP Request. Defaults to `None`.

    Returns:
        [tup]: Returns a tupple with the relevant information from the HTTP
        Request, including status code, text, and the parameters `has_more`,
        `next_cursor`, and the `results`.
    """
    logging.info('HELPER.py: Making the HTTP Request.')
    r = requests.post(
        url,
        headers=headers,
        json=payload
    )

    if r.status_code == 200:
        # Setting up log message
        log_data['status'] = "Success"
        log_data['response'] = r.status_code
        log_data['message'] = ("Successfull HHTP Request.")
        log_data['rows'] = None
        log_data['timestamp'] = str(datetime.now())

        # Logging message
        log(log_data)
    else:
        # Setting up log message
        log_data['status'] = "Failed"
        log_data['response'] = r.status_code
        log_data['message'] = f"Error from the HTTP request: {r.text}"
        log_data['timestamp'] = str(datetime.now())

        # Logging message
        log(log_data)

        # Returning status
        raise ValueError(f"Error from the HTTP request: {r.text}")

    return (r.json().get('has_more'),
            r.json().get('next_cursor'),
            r.json().get('results'))


def prepare_data(data, fields):
    """Parse the fetched data from Notion Official API when Querying Databases.

    Args:
        data (list): List of fetched results from the API.
        fields (list): List of ordered fields provided as param to the
        Cloud Function.
    """
    logging.info('HELPER.py: Reached the prepare_data() function.')
    logging.info('HELPER.py: A total of {} rows arrived here.'.format(len(data)))
    rows_to_append = list()
    # fields = literal_eval(fields)

    for result in data:

        # Filters down to the actual data in the results
        fetched_record_data = result.get('properties')

        # values = list() # Empty list that will hold the values, per record
        tuple_of_values = tuple()

        for key in fields:

            # Gets the field type to correctly point to the actual data
            field_type = fetched_record_data.get(key).get('type')

            # TODO: Perhaps I can refact this and make it less verbose?
            # Kind of a swtich to guide the data to the right path depending on
            # the type of the field. There is a little redundancy here.
            if field_type == 'number':
                field_value = fetched_record_data.get(key).get('number')

            elif field_type == 'select':
                prelim_data = fetched_record_data.get(key).get('select')
                field_value = prelim_data.get('name') if prelim_data != None else None

            elif field_type == 'date':
                prelim_data = fetched_record_data.get(key).get('date')
                field_value = prelim_data.get('start') if prelim_data != None else None

            elif field_type == 'multi_select':
                prelim_data = fetched_record_data.get(key).get('multi_select')

                field_value = [each.get('name') if type(prelim_data) is list and len(prelim_data) > 0 else 'None' for each in prelim_data]

                if type(field_value) is list:
                    field_value = ",".join(field_value)

            elif field_type == 'created_time':
                field_value = fetched_record_data.get(key).get('created_time')
                field_value = parser.parse(field_value).strftime('%Y-%m-%d')

            elif field_type == 'rich_text':
                prelim_data = fetched_record_data.get(key).get('rich_text')

                if type(prelim_data) is list:
                    prelim_data =  prelim_data[0] if len(prelim_data) > 0 else 'None'

                field_value = prelim_data.get('plain_text') if prelim_data != 'None' else None

            elif field_type == 'url':
                field_value = fetched_record_data.get(key).get('url')

            elif field_type == 'title':
                prelim_data = fetched_record_data.get(key).get('title')

                if type(prelim_data) is list:
                    prelim_data =  prelim_data[0] if len(prelim_data) > 0 else 'None'

                field_value = prelim_data.get('plain_text') if prelim_data != 'None' else None

            elif field_type == 'last_edited_time':
                field_value = fetched_record_data.get(key).get('last_edited_time')
                field_value = parser.parse(field_value)

            else:
                field_value = 'None'

            # values.append(field_value)
            tuple_of_values = tuple_of_values + (field_value,)

        # Since the fields dictionary is ordered, and the data is being harvest
        # following that order, a simple zip here does the job
        # data = zip(fields, values)

        rows_to_append.append(tuple_of_values)

    logging.info('HELPER.py: A TOTAL of {} rows were parsed.'.format(len(rows_to_append)))

    return rows_to_append