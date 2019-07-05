"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

This is a factory that handles rules generated into a canonical form and sends them to a deid engine for processing
"""
import sqlite3

from google.cloud import bigquery as bq
import pandas as pd


class Meta(object):

    def sqlite(self, **args):
        """
        Will load a csv/json file with the type information of a table provide the following:

        :path   path of the sqlite file
        :table  name of the table
        """
        path = args['path']
        table = args['table']
        conn = sqlite3.connect(path)
        sql = " ".join(["SELECT * FROM ", table, "LIMIT 10"])
        data_frame = pd.read_sql_query(sql, conn)
        return data_frame.columns.tolist()

    def postgresql(self, **args):
        return

    def bigquery(self, **args):
        """
        This function will return the meta data associated with a table in a bigquery dataset (or schema)
        """
        table_name = args.get('table', '')
        schema = args.get('schema', args.get('dataset', ''))
        private_key = args.get('path', '')
        client = bq.Client.from_service_account_json(private_key)
        tables = client.list_tables(bq.dataset(schema))
        tables = [table for table in tables if table.table_id == table_name]
        return tables[0] if tables else None

    def instance(self, **args):
        """
        :path   path of the db (for sqlite), JSON service account file (bigquery)
        :table  name of the table
        :schema optional if the persistance store supports it
        """
        store = args.get('store', '')
        return getattr(Meta, store)(**args)