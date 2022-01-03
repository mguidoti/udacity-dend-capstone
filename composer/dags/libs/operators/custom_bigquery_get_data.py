# https://stackoverflow.com/questions/53565834/fetch-results-from-bigqueryoperator-in-airflow

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

import json

class CustomBigqueryGetDataOperator(BaseOperator):
    template_fields = ['sql']
    ui_color = '#e2ffa3'

    @apply_defaults
    def __init__(
            self,
            sql,
            # keys,
            bigquery_conn_id=None,
            delegate_to=None,
            *args,
            **kwargs):
        super(CustomBigqueryGetDataOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        # self.keys = keys # A list of keys for the columns in the result set of sql
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to


    def execute(self, context):
        """
        Run query and handle results row by row.
        """
        ti = context.get('task_instance')

        cursor = self._query_bigquery()
        data = cursor.fetchall()

        # for row in data:

            # Convert all elements in row to String, or the json.dumps()
            # will fail
            # row = [str(value) for value in row]

            # Zip keys and row together because the cursor returns a list of
            # list (not list of dicts)
            # row_dict = dict(zip(self.keys,row))

            # ti.xcom_push(key='delta_params', value=row_dict)
        print(data)
        print(len(data))
        if len(data) > 0:
            if str(data[0][0]) == 'True':
                ti.xcom_push(key='status', value=True)


    def _query_bigquery(self):
        """
        Queries BigQuery and returns a cursor to the results.
        """
        bq = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                          use_legacy_sql=False)
        conn = bq.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor