from google.cloud import bigquery

from datetime import datetime
from os.path import dirname

import logging

import sql
from helper import parse_fetched_data, upload, write_to_bq
from logger import log


# Creates a log file
logging.basicConfig(filename='{path}/logs/{today}.log'.format(path=dirname(__file__), today=datetime.now().strftime('%Y-%m-%d-%H%M%S')),
#                     # encoding='utf-8',
                    level=logging.INFO)

logging.getLogger().addHandler(logging.StreamHandler())

vm_instance = 'instance-2'

def main():

    # Create log data
    log_data = {
        'service': 'Compute Engine',
        'process': 'uploader.py',
        'filepath_service': None,
        'filepath_project_id': 'data-tools-prod-336318',
        'filepath_instance_name': vm_instance,
        'filepath': 'uploaded/{filename}',
        'filename': '',
        'filesize': 0,
        'nature_of_file': '',
        'destination': f'Cloud Compute ({vm_instance})',
        'start_process_date': str(datetime.today().strftime('%Y-%m-%d')),
        'status': '',
        'response': '',
        'message': '',
        'logged_from': '',
        'ran_at': ''
    }

    # Try/Catch for data preparation
    try:
        logging.info(f'{vm_instance}-MAIN.py: Connecting to BigQuery')
        client = bigquery.Client()

        logging.info(f'{vm_instance}-MAIN.py: Querying the table through a SQL statement')
        query_job = client.query(sql.select_tunnel.format(status="'DOWNLOADED'"))
        fetched_data = query_job.result()  # Waits for job to complete.

        logging.info(f'{vm_instance}-MAIN.py: Parsing the fetched data')
        data = parse_fetched_data(fetched_data, log_data)

        logging.info(f'{vm_instance}-MAIN.py: For each row/publication, calling the download helper function.')
        for publication in data:
            upload_return = upload(publication, log_data)

            if upload_return != None:
                logging.info('{vm_instance}-MAIN.py: Updating a status of a publication from {old} to {new}'.format(vm_instance=vm_instance,
                                                                                                                    old=publication['status'],
                                                                                                                    new=upload_return))
                publication['status'] = upload_return
                publication['inserted_at'] = str(datetime.now())


        logging.info(f'{vm_instance}-MAIN.py: Appending new rows to the publications_tunnel RAW table.')
        write_to_bq(data, log_data)
    except Exception as e:

        logging.error(f'{vm_instance}-MAIN.py ERROR: Something happened: {str(e)}')

        raise ValueError((f"Something happened: {str(e)}"))


if __name__ == "__main__":
    main()


