"""
This class applies rules and meta data to yield a certain output
"""
from datetime import datetime
import json
import os

import numpy as np
import pandas as pd

from rules import Deid

class Press(object):
    def __init__(self, **args):
        """
        :rules_path  to the rule configuration file
        :info_path   path to the configuration of how the rules get applied
        """
        rules_filepath = args.get('rules', '')
        self.deid_rules = json.loads((open(rules_filepath)).read())

        try:
            self.info = json.loads((open(args.get('table', ''))).read())
        except IOError:
            #
            # In case a table name is not provided, we will apply default rules on he table
            #   I.e physical field suppression and row filter
            #   Date Shifting
            self.info = {}

        if isinstance(self.deid_rules, list):
            cache = {}
            for row in self.deid_rules:
                _id = row.get('_id', '')
                cache[_id] = row
            self.deid_rules = cache
        self.idataset = args.get('idataset', '')
        self.tablename = args.get('table', '')
        self.tablename = self.tablename.split(os.sep)[-1].replace('.json', '').strip()
        self.store = args.get('store', 'sqlite')

        if 'suppress' not in self.deid_rules:
            self.deid_rules['suppress'] = {'FILTERS': []}

        if 'FILTERS' not in self.deid_rules['suppress']:
            self.deid_rules['suppress']['FILTERS'] = []

        self.logpath = args.get('logs', 'logs')
        self.action = args.get('action', 'submit')
        self.action = [term.strip() for term in self.action.split(',')]

    def meta(self, df):
        return pd.DataFrame({"names": list(df.dtypes.to_dict().keys()),
                             "types": list(df.dtypes.to_dict().values())})

    def initialize(self, **args):
        #
        # Let us update and see if the default filters apply at all
        dfilters = []
        columns = self.get(limit=1).columns.tolist()
        deid_filters = self.deid_ruls.get('suppress', {}).get('FILTERS', [])
        for row in deid_filters:
            # should this be the boolean 'and' or is the bit-wise '&' actually wanted
            if set(columns) & set(row.get('filter', '').split(' ')):
                dfilters.append(row)
        self.deid_rules.update({'suppress': {'FILTERS': dfilters}})

    def get(self, **args):
        """
        This function will execute an SQL statement and return the meta data for a given table
        """
        return None

    def submit(self):
        """
        Base class submit method.

        Should be overridden by extending classes.
        """
        raise NotImplementedError("Extending classes are expected to override submit()")

    def update_rules(self):
        """
        Base class update rules method.

        Should be overridden by extending classes.
        """
        raise NotImplementedError("Extending classes are expected to override update_rules()")

    def do(self):
        """
        This function actually runs deid and using both rule specifications and application of the rules
        """
        self.update_rules()
        d = Deid()
        d.cache = self.deid_rules

        _info = self.info

        p = d.apply(_info, self.store)
        is_meta = np.sum([1 * ('on' in _item) for _item in p]) != 0
        self.log(module='do', action='table-type', table=self.get_tablename(), is_meta=is_meta)
        if not is_meta:
            sql = self.to_sql(p)
            _rsql = None
        else:
            #
            # Processing meta tables
            sql = []
            relational_cols = [col for col in p if 'on' not in col]
            meta_cols = [col for col in p if 'on' in col]
            _map = {}
            for col in meta_cols:
                on_col = col.get('on')
                try:
                    _map[on_col].append(col)
                except KeyError:
                    _map[on_col] = [col]

            filter_list = []
            # an error here.  should it default to False or an empty list
            filters_exist = self.deid_rules.get('suppress', {}).get('FILTERS', False)
            conjunction = ' AND ' if filters_exist else ' WHERE '
            for filter_id in _map:
                _item = _map.get(filter_id, '')
                filter_list.append(filter_id)

                _sql = self.to_sql(_item + relational_cols) + conjunction + filter_id

                sql.append(_sql)
            #-- end of loop
            if ' AND ' in conjunction:
                _rsql = self.to_sql(relational_cols) + ' AND ' + ' AND '.join(filter_list).replace(' IN ', ' NOT IN ')
            else:
                _rsql = self.to_sql(relational_cols) + ' WHERE ' + ' '.join(filter_list).replace(' IN ', ' NOT IN ')

            #
            # @TODO: filters may need to be adjusted (add a conditional statement)
            #

            sql.append(_rsql)
            sql = "\nUNION ALL\n".join(sql)

        if 'debug' in self.action:
            self.debug(p)
        else:
            if 'submit' in self.action:
                self.submit(sql)
            if 'simulate' in self.action:
                #
                # Make this threaded if there is a submit action that is associated with it
                self.simulate(p)

    def get_tablename(self):
        return self.idataset + "." + self.tablename if self.idataset else self.tablename

    def debug(self, info):
        for row in info:
            print()
            print(row.get('label', ''), not row.get('apply'))
            print()

    def log(self, **args):
        print(args)

    def simulate(self, info):
        """
        This function will attempt to log the various transformations on every field.

        This is to facilitate the testing team's efforts.
        :info   payload defining the transformations applied to a given table as follows
                [{apply, label, name}] where
                    - apply is the SQL to be applied
                    - label is the flag for the operation (generalize, suppress, compute, shift)
                    - name  is the attribute name that on which the rule gets applied
        """
        table_name = self.idataset + "." + self.tablename
        deid_filters = self.deid_rules.get('suppress', {}).get('FILTERS', [])
        out = pd.DataFrame()
        counts = {}
        dirty_date = False
        filters = []
        for item in info:
            labels = item.get('label', '').split('.')
            if labels[0] not in counts:
                counts[labels[0].strip()] = 0
            counts[labels[0].strip()] += 1

            if 'suppress' in labels or item.get('name', '') == 'person_id' or dirty_date is True:
                continue

            field = item.get('name', '')
            alias = 'original_' + field
            applied = item.get('apply', '')
            sql = ["SELECT DISTINCT ", field, 'AS ', alias, ",", applied, " FROM ", table_name]
            if deid_filters:
                sql.append('WHERE')

                for row in deid_filters:
                    sql.append(row.get('filter', ''))

                    if deid_filters.index(row) < len(deid_filters) - 1:
                        sql.append('AND')

            if 'on' in item:
                # This applies to meta tables
                on_values = item.get('on', '')
                filters.append(on_values)

                if deid_filters:
                    sql.append('AND')
                else:
                    sql.append('WHERE ')

                sql.append(on_values)

            if 'shift' in labels:
                df = self.get(sql=" ".join(sql).replace(':idataset', self.idataset), limit=5)
            else:
                df = self.get(sql=" ".join(sql).replace(':idataset', self.idataset))

            if df.shape[0] == 0:
                self.log(module="simulate",
                         table=table_name,
                         attribute=field,
                         type=item.get('label', ''),
                         status='no data-found')
                continue
            df.columns = ['original', 'transformed']
            df['attribute'] = field
            df['task'] = item.get('label', '').upper().replace('.', ' ')
            out = out.append(df)
        #-- Let's evaluate row suppression here
        #
        out.index = range(out.shape[0])
        rdf = pd.DataFrame()
        if deid_filters:
            extra_filters = [item['filter'] for item in deid_filters if 'filter' in item]
            filters.append(extra_filters)
            original_sql = ' (SELECT COUNT(*) as original FROM {table}) AS ORIGINAL_TABLE ,'.format(table=table_name)
            transformed_sql = (
                '(SELECT COUNT(*) AS transformed FROM {table} WHERE {filters}) AS TRANSF_TABLE'
                .format(table=table_name, filters=' OR '.join(filters))
            )
            sql = ['SELECT * FROM ', original_sql, transformed_sql]

            r = self.get(sql=" ".join(sql).replace(":idataset", self.idataset))
            table_name = self.idataset + "." + self.tablename

            rdf = pd.DataFrame({"operation": ["row-suppression"], "count": r.transformed.tolist()})

        now = datetime.now()
        flag = "-".join(np.array([now.year, now.month, now.day, now.hour]).astype(str).tolist())
        root = os.path.join(self.logpath, self.idataset, flag)

        try:
            os.makedirs(root)
        except OSError:
            print('file {} already exists.  moving on...'.format(root))

        stats = pd.DataFrame({"operation": counts.keys(), "count": counts.values()})
        stats = stats.append(rdf)
        stats.index = range(stats.shape[0])
        stats.reset_index()

        _map = {
            os.path.join(root, 'samples-' + self.tablename + '.csv'): out,
            os.path.join(root, 'stats-' + self.tablename + '.csv'): stats,
        }

        for path in _map:
            _df = _map.get(path, '')
            _df.to_csv(path, encoding='utf-8')

        self.log(module='simulation', table=table_name, status='completed', value=root)

    def to_sql(self, info):
        """
        :info   payload with information of the process to be performed
        """

        table_name = self.get_tablename()
        fields = self.get(limit=1).columns.tolist()
        columns = list(fields)
        sql = []
        self.log(module='to_sql', action='generating-sql', table=table_name, fields=fields)
        for rule_id in ['generalize', 'suppress', 'shift', 'compute']:
            for row in info:
                name = row.get('name', '')

                if rule_id not in row.get('label', '') or name not in fields:
                    continue

                index = fields.index(name)
                fields[index] = row.get('apply', '')

        sql = ['SELECT', ",".join(fields), 'FROM ', table_name]

        if 'suppress' in self.deid_rules and 'FILTERS' in self.deid_rules['suppress']:
            deid_filters = self.deid_rules['suppress']['FILTERS']
            if deid_filters:
                sql.append('WHERE')
            for row in deid_filters:
                if not (set(columns) & set(row.get('filter', '').split(' '))):
                    continue
                sql.append(row.get('filter', ''))
                if deid_filters.index(row) < len(deid_filters) - 1:
                    sql.append('AND')
        return '\t'.join(sql).replace(":idataset", self.idataset)

    def to_pandas(self, rules, info):
        pass
