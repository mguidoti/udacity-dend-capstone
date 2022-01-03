from google.cloud import bigquery

import requests

from os import path
from datetime import datetime
import shutil

from logger import log
import logging

vm_instance = 'instance-2'

def parse_fetched_data(fetched_data, log_data):
    """[summary]

    Args:
        fetched_data ([type]): [description]

    Returns:
        [type]: [description]
    """
    logging.info(f'{vm_instance}-HELPER.py: Starting to prepare the fetched data')
    results = list()

    for row in fetched_data:
        # print(row)
        results.append({
            'publisher_full_name': row.publisher_full_name,
            'publisher_short_name': row.publisher_short_name,
            'scraper_name': row.scraper_name,
            'year': row.year,
            'volume': row.volume,
            'issue': row.issue,
            'start_page': row.start_page,
            'end_page': row.end_page,
            'publication_type': row.publication_type,
            'doi': row.doi,
            'first_author_surname': row.first_author_surname,
            'link': row.link,
            'filename': row.filename,
            'status': row.status,
            'inserted_at': row.inserted_at
        })

    return results


def upload(publication, log_data):
    """[summary]
    """
    try:
        logging.info(f'{vm_instance}-HELPER.py: Moving the file')
        download_filepath = path.join(path.dirname(__file__), '..', 'downloader', 'downloads')
        upload_filepath = path.join(path.dirname(__file__), 'uploaded')

        shutil.move(path.join(download_filepath, publication.get('filename')),
                    path.join(upload_filepath, publication.get('filename')))

        logging.info(f'{vm_instance}-HELPER.py: Preparing the data to the log function')
        log_data['filepath'] = log_data['filepath'].format(filename=publication.get('filename'))
        log_data['filename'] = publication.get('filename')
        log_data['filesize'] = str(path.getsize(path.join(upload_filepath, publication.get('filename'))))
        log_data['nature_of_file'] = publication.get('publication_type')
        log_data['status'] = 'SUCCESS'
        log_data['response'] = ''
        log_data['message'] = ''
        log_data['logged_from'] = 'Compute Engine'
        log_data['ran_at'] = str(datetime.now())

        logging.info(f'{vm_instance}-HELPER.py: Calling the log function')
        log(log_data)

        return 'UPLOADED'

    except Exception as e:
        logging.info(f'{vm_instance}-HELPER.py: Preparing the data to the log function when the upload crashed')
        log_data['filepath'] = ''
        log_data['filename'] = publication.get('filename')
        log_data['filesize'] = 0
        log_data['nature_of_file'] = publication.get('publication_type')
        log_data['status'] = 'ERROR'
        log_data['response'] = ''
        log_data['message'] = str(e)
        log_data['logged_from'] = 'Compute Engine'
        log_data['ran_at'] = str(datetime.now())

        logging.info(f'{vm_instance}-HELPER.py: Calling the log function')
        log(log_data)

        # Raising exception
        raise ValueError((f"Error {e.__class__.__name__} tyring to run "
                           "the uploader.py."))


# def download(publication, log_data):
#     """Most of this code came from the following link:
#     https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests

#     Args:
#         filename ([type]): [description]
#         link ([type]): [description]
#     """
#     logging.info(f'{vm_instance}-HELPER.py: Making the HTTP Request to download the file')
#     r = requests.get(publication.get('link'), stream=True)

#     try:
#         if r.status_code == 200:
#             # response = r.text
#             with open(path.join(path.dirname(__file__), 'downloads', publication.get('filename')), 'wb') as f:
#                 logging.info(f'{vm_instance}-HELPER.py: Writing file in chunks of 1MB')
#                 for chunk in r.iter_content(chunk_size=1024):
#                     if chunk: # filter out keep-alive new chunks
#                         f.write(chunk)

#         else:

#             logging.info(f'{vm_instance}-HELPER.py: Preparing the data to the log function when status code is different than 200')
#             log_data['filepath'] = log_data['filepath'].format(filename=publication.get('filename'))
#             log_data['filename'] = publication.get('filename')
#             log_data['filesize'] = str(r.headers['Content-length'])
#             log_data['nature_of_file'] = publication.get('publication_type')
#             log_data['status'] = 'SUCCESS' if r.status_code == 200 else 'FAILED'
#             log_data['response'] = ''
#             log_data['message'] = ''
#             log_data['logged_from'] = 'Compute Engine'
#             log_data['ran_at'] = str(datetime.now())

#             logging.info(f'{vm_instance}-HELPER.py: Calling the log function')
#             log(log_data)

#             return None

#         logging.info(f'{vm_instance}-HELPER.py: Preparing the data to the log function')
#         log_data['filepath'] = log_data['filepath'].format(filename=publication.get('filename'))
#         log_data['filename'] = publication.get('filename')
#         log_data['filesize'] = str(r.headers['Content-length'])
#         log_data['nature_of_file'] = publication.get('publication_type')
#         log_data['status'] = 'SUCCESS' if r.status_code == 200 else 'FAILED'
#         log_data['response'] = ''
#         log_data['message'] = ''
#         log_data['logged_from'] = 'Compute Engine'
#         log_data['ran_at'] = str(datetime.now())

#         logging.info(f'{vm_instance}-HELPER.py: Calling the log function')
#         log(log_data)

#         return 'DOWNLOADED'

#     except Exception as e:
#         logging.info(f'{vm_instance}-HELPER.py: Preparing the data to the log function when the downloaded crashed')
#         log_data['filepath'] = ''
#         log_data['filename'] = publication.get('filename')
#         log_data['filesize'] = 0
#         log_data['nature_of_file'] = publication.get('publication_type')
#         log_data['status'] = 'ERROR'
#         log_data['response'] = ''
#         log_data['message'] = str(e)
#         log_data['logged_from'] = 'Compute Engine'
#         log_data['ran_at'] = str(datetime.now())

#         logging.info(f'{vm_instance}-HELPER.py: Calling the log function')
#         log(log_data)

#         # Raising exception
#         raise ValueError((f"Error {e.__class__.__name__} tyring to run "
#                            "the downloader.py."))


def write_to_bq(data, log_data):
    """[summary]

    Args:
        data ([type]): [description]

    Returns:
        [type]: [description]
    """

    # Log Variables
    process = 'downloader.py'
    service = 'Compute Engine'
    vm_instance = 'instance-2'
    date = str(datetime.now().strftime('%Y-%m-%d'))

    logging.info(f'{vm_instance}-HELPER.py: Conecting to BigQuery')
    client = bigquery.Client()

    # Getting the data to compose the BigQuery table id
    table_id = ("{project_id}.{dataset}.{table}"
                        .format(project_id='poc-plazi-raw',
                                dataset='platform',
                                table='publications_tunnel'))

    ## Get Biq Query Set up
    table = client.get_table(table_id)
    logging.info(f'{vm_instance}-HELPER.py: Table retrieved: {table}')

    # Checks if there is any data after all - it might be that there is no paper
    # to upload
    if len(data) > 0:
        insert_errors = client.insert_rows(table, data)

    if insert_errors == []:
        logging.info(f"SUCCESS! No errors where found in the process `{process}` "
                    f"from service {service} run from {vm_instance} on {date}")

    else:
        logging.info(f"ERROR: Something happened while logging process `{process}` "
                      f"from service {service} on {date}. Error messages "
                      f"available: {insert_errors}")