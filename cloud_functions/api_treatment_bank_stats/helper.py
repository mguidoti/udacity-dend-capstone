import requests

from datetime import datetime

# from config import config
from logger import log

def querying_data(log_data, url):
    """This helper function deals with the HTTP Request and follow up log in
    in case of success.

    Args:
        log_data ([type]): Dictionary containing the core data to be logged
        url (str): String to be used in the HTTP Request

    Returns:
        [tup]: Returns a tupple with the relevant information from the HTTP
        Request, including status code, text, and the parameters `has_more`,
        `next_cursor`, and the `results`.
    """
    r = requests.post(url)

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

    return r.json()['data']

def prepare_data(data, fields):
    """Takes the returning data from TreatmentBank and converts in a
    list of tuples in order to be appended to a table in BigQuery.

    Args:
        data (dict): Dictionary containing the TreatmentBank data.

    Returns:
        rows_to_append (list): List of tuples to be inserted into BigQuery.
    """

    rows_to_append = list()

    for record in data:
        tuple_of_values = tuple()
        for field, value in record.items():

            if field in fields:
                tuple_of_values = tuple_of_values + (value,)
        rows_to_append.append(tuple_of_values)

    return rows_to_append

