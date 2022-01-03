from google.cloud import bigquery
from os import environ
import logging

from datetime import datetime

from logger import log

# environ["GOOGLE_APPLICATION_CREDENTIALS"]='config/key.json'

table_config = {
    'project_id': 'poc-plazi-raw',
    'dataset': 'platform',
    'table': 'publications_tunnel'
}

trusted_config = {
    'project_id': 'poc-plazi-trusted',
    'dataset': 'publications',
    'table': 'tunnel'
}

app_engine = 'acarologia'

sql = """SELECT *
         FROM `poc-plazi-trusted.publications.tunnel`
         WHERE publisher_full_name = "{journal}"
         ORDER BY year DESC,
                  volume DESC,
                  issue DESC,
                  start_page DESC
        LIMIT 1"""

# Getting the data to compose the BigQuery table id
table_id = ("{project_id}.{dataset}.{table}"
                .format(project_id=table_config.get('project_id'),
                        dataset=table_config.get('dataset'),
                        table=table_config.get('table')))

trusted_id = ("{project_id}.{dataset}.{table}"
                .format(project_id=trusted_config.get('project_id'),
                        dataset=trusted_config.get('dataset'),
                        table=trusted_config.get('table')))

def query():
    client = bigquery.Client()
    results= list()

    logging.info(f'{app_engine}-HELPER.py: Querying the table through a SQL statement')
    query_job = client.query(sql.format(journal='Acarologia'))
    fetched_data = query_job.result()  # Waits for job to complete.

    logging.info(f'{app_engine}-HELPER.py: Parsing the fetched data')

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


def filter_data(data):
    if query is list and len(query) > 0:
        latest = query()[0]

        results = list()

        for article in data:
            if (
                (int(article.get('year')) > int(latest.get('year'))) or
                (int(article.get('year')) == int(latest.get('year')) 
                    and (int(article.get('volume')) > int(latest.get('volume')))) or
                (int(article.get('year')) == int(latest.get('year')) 
                    and (int(article.get('volume')) == int(latest.get('volume')))
                    and (int(article.get('issue')) > int(latest.get('issue')))) or
                (int(article.get('year')) == int(latest.get('year')) 
                    and (int(article.get('volume')) == int(latest.get('volume')))
                    and (int(article.get('issue')) == int(latest.get('issue')))
                    and (int(article.get('start_page')) > int(latest.get('start_page'))))
                ):

                art = tuple(article.values())
                results.append(art)

        return results

    else:
        results = list()

        for article in data:
            art = tuple(article.values())
            results.append(art)

        return results


def write(data):


    client = bigquery.Client()
    table = client.get_table(table_id)

    errors = client.insert_rows(table, data)

    if errors == []:
        log_data = [
            (
                'Compute Engine',
                'vm-crawlers',
                'ACAROLOGIA',
                'Acarologia Article Page',
                "Journal's Repository",
                'https://www1.montpellier.inra.fr/CBGP/acarologia/',
                len(data),
                str(datetime.now().strftime('%Y-%m-%d')),
                'SUCCESS',
                '',
                'Successfully Scraped the website.',
                'Compute Engine',
                str(datetime.now())
            )
        ]

        log(log_data)

    else:
        log_data = [
            (
                'Compute Engine',
                'vm-crawlers',
                'ACAROLOGIA',
                'Acarologia Article Page',
                "Journal's Repository",
                'https://www1.montpellier.inra.fr/CBGP/acarologia/',
                len(data),
                str(datetime.now().strftime('%Y-%m-%d')),
                'SUCCESS',
                errors,
                'Failed to deposit records from the scraping of the website. Maybe there was no new record?',
                'Compute Engine',
                str(datetime.now())
            )
        ]

        log(log_data)

