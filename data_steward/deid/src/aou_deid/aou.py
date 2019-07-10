"""
AOU - De-identification Engine
Steve L. Nyemba <steve.l.nyemba@vanderbilt.edu>

This engine will run de-identificataion rules againsts a given table, certain rules are applied to all tables
(if possible).  We have divided rules and application of the rules, in order to have granular visibility into
what is happening.

DESIGN:

    - The solution :
    The Application of most of the rules are handled in the SQL projection, this allows for simpler jobs with no risk
    of limitations around joins imposed by big-query.  By dissecting operations in this manner it is possible to log
    the nature of an operation on a given attribute as well as pull some sample data to illustrate it

    We defined a vocabulary of rule specifications :
        -fields         Attributes upon which a given rule can be applied
        -values         conditional values that determine an out-come of a rule (can be followed by an
                            operation like REGEXP)
                            If followed by "apply":"REGEXP" the values are assumed to be applied
                            using regular expressions
                            If NOT followed by anything the values are assumed to be integral values and
                            the IN operator is used instead
        -into           outcome related to a rule
        -key_field      attribute to be used as a filter that can be specified by value_field or values
        -value_field    value associated with a key_field
        -on             suggests a meta table and will have filter condition when generalization or a field
                        name for row based suppression


    Overall there are rules that suggest what needs to happen on values, and there is a file specifying how
    to apply the rule on a given table.

    - The constraints:

        1. Bigquery is designed to be used as a warehouse not an RDBMS.That being said
            a. it lends itself to uncontrollable information redundancies and proliferation.
            b. The use of relational concepts such as foreign keys are pointless in addition to the fact
            that there is not referential integrity support in bigquery.  As a result thre is no mechanism
            that guarantees data-integrity.


        2. Testing techniques do not currently include some industry standard best practices,
        (e.g. defining equivalence classes, orthogonal arrays, etc.)
        As such we have a method "simulate" that acts as a sampler to provide some visibility into what
        this engine has done given an attribute and the value space of the data.
        This potentially adds to data redundancies.  It *MUST* remain internal.

LIMITATIONS:

    - The engine is not able to validate the rules without having to submit the job

    - The engine can not simulate complex cases, its intent is to help by providing information about basic scenarios.

    - The engine does not resolve issues of consistency with data for instance: if a record has M,F on
    two fields for gender ... this issue is out of the scope of deid.
    Because it relates to data-integrity.

NOTES:

    There is an undocumented feature enabled via a hack i.e Clustering a table.
    The API (SDK) does NOT provide a clean way to perform clustering a table.

    To try to compensate for this, the developed approach looks for redundancies using regular expressions
    and other available information.  Also developed a means by which identifiers can be used in future iterations.

USAGE :

    python aou.py --rules <path.json> --idataset <name> --private_key <file> --table <table.json> \
        --action [submit,simulate|debug] [--cluster] [--log <path>]

    --rule  will point to the JSON file contianing rules
        --idataset  name of the input dataset (an output dataset with suffix _deid will be generated)
        --table     path of that specify how rules are to be applied on a table
        --private_key   service account file location
        --action        what to do:
                        simulate    will generate simulation without creating an output table
                        submit      will create an output table
                        debug       will just print output without simulation or submit (runs alone)
        --cluster     This flag enables clustering on person_id
"""
from __future__ import print_function

# Python imports
import json
import os
import time

# Third party imports
from google.cloud import bigquery as bq
from google.oauth2 import service_account
import numpy as np
import pandas as pd

# Project imports
from parser import Parse as Parser
from press import Press

class AOU(Press):
    def __init__(self, **args):
        args['store'] = 'bigquery'
        Press.__init__(self, **args)
        self.private_key = args.get('private_key', '')
        self.credentials = service_account.Credentials.from_service_account_file(self.private_key)
        self.odataset = self.idataset + '_deid'
        self.partition = 'cluster' in args
        self.priority = 'BATCH' if 'interactive' not in args else 'INTERACTIVE'
        if 'shift' in self.deid_rules:
            #
            # Minor updates that are the result of a limitation as to how rules are specified.
            # @TODO: Improve the rule specification language
            shift_days = " ".join(['SELECT shift from ',
                                   self.idataset,
                                   '.deid_map WHERE deid_map.person_id = ',
                                   self.tablename,
                                   '.person_id'])
            deid_shift = self.deid_rules.get('shift', '')
            self.deid_rules['shift'] = json.loads(json.dumps(deid_shift).replace(":SHIFT", shift_days))

    def initialize(self, **args):
        Press.initialize(self, **args)
        age_limit = args.get('age_limit', 50)
        max_day_shift = args.get('max_day_shift', 365)
        million = 1000000
        map_tablename = self.idataset + ".deid_map"
        sql = " ".join(["SELECT DISTINCT person_id, EXTRACT(YEAR FROM CURRENT_DATE()) - year_of_birth as age FROM ",
                        self.idataset + ".person",
                        "ORDER BY 2"])
        person_table = self.get(sql=sql)
        self.log(module='initialize', subject=self.get_tablename(), action='patient-count', value=person_table.shape[0])
        map_table = pd.DataFrame()
        if person_table.shape[0] > 0:
            person_table = person_table[person_table.age < age_limit]
            map_table = self.get(sql="SELECT * FROM " + map_tablename)
            if map_table.shape[0] > 0 and map_table.shape[0] == person_table.shape[0]:
                #
                # There is nothing to be done here
                # @TODO: This weak post-condition is not enough nor consistent
                #   - consider using a set operation
                #
                pass
            else:
                num_records = person_table.shape[0]
                lower_bound = million
                upper_bound = lower_bound + (10 * num_records)
                map_table = pd.DataFrame({"person_id": person_table.get('person_id', pd.Series()).tolist()})
                map_table['research_id'] = np.random.choice(
                    np.arange(lower_bound, upper_bound), num_records, replace=False
                )
                map_table['shift'] = np.random.choice(np.arange(1, max_day_shift), num_records)
                #
                # We need to write this to bigquery now
                #

                try:
                    map_table.to_gbq(map_tablename, credentials=self.credentials, if_exists='fail')
                except (ImportError, Exception) as exc:
                    print("Unable to write back to bigquery")
                    print(exc)
        else:
            print("This sql didn't produce any results!\n{}".format(sql))

        #
        # @TODO: Make sure that what happened here is logged and made available
        #   - how many people are mapped
        #   - how many people are not mapped
        #   - was the table created or not
        #
        self.log(module='initialize', subject=self.get_tablename(), action='mapped-patients', value=map_table.shape[0])
        return person_table.shape[0] > 0 or map_table.shape[0] > 0

    def update_rules(self):
        """
        This will add rules that are to be applied by default to the current table
        @TODO: Make sure there's a way to specify these in the configuration
        """
        df = self.get(limit=1)
        columns = df.columns.tolist()

        if 'suppress' not in self.info:
            self.info['suppress'] = [{"rules": "@suppress.DEMOGRAPHICS-COLUMNS",
                                      "table": self.get_tablename(),
                                      "fields": df.columns.tolist(),}]
        else:
            for rule in self.info['suppress']:
                rule_info = rule.get('rules', '')
                if rule_info.endswith('DEMOGRAPHICS-COLUMNS'):
                    rule['table'] = self.get_tablename()
                    rule['fields'] = columns

        # why the bitwise '&' instead of the boolean 'and'?
        date_columns = [name for name in columns if set(['date', 'time', 'datetime']) & set(name.split('_'))]

        if date_columns:
            date = {}
            datetime = {}

            for name in date_columns:
                if 'time' in name:
                    try:
                        datetime['fields'].append(name)
                    except KeyError:
                        datetime['fields'] = [name]

                    datetime['rules'] = '@shift.datetime'
                elif 'date' in name:
                    try:
                        date['fields'].append(name)
                    except KeyError:
                        date['fields'] = [name]

                    date['rules'] = '@shift.date'

            if date and datetime:
                _toshift = [date, datetime]
            elif date or datetime:
                _toshift = [date] if date else [datetime]

            self.info['shift'] = _toshift if 'shift' not in self.info else self.info['shift'] + _toshift

    def get(self, **args):
        """
        This function will execute a query to a data-frame (for easy handling)
        """
        sql = args.get('sql', "".join(["SELECT * FROM ", self.idataset, ".", self.tablename]))

        if 'limit' in args:
            sql = sql + " LIMIT " + str(args.get('limit', 10))

        try:
            df = pd.read_gbq(sql, credentials=self.credentials, dialect='standard')
            return df
        except (ImportError, Exception) as exc:
            print(exc)

        return pd.DataFrame()

    def submit(self, sql):
        table_name = self.get_tablename()
        client = bq.Client.from_service_account_json(self.private_key)
        #
        # Let's make sure the out dataset exists
        datasets = list(client.list_datasets())
        found = np.sum([1  for dataset in datasets if dataset.dataset_id == self.odataset])

        if not found:
            dataset = bq.Dataset(client.dataset(self.odataset))
            client.create_dataset(dataset)

        job = bq.QueryJobConfig()
        job.destination = client.dataset(self.odataset).table(self.tablename)
        job.use_query_cache = True
        job.allow_large_results = True

        if self.partition:
            job._properties['load']['timePartitioning'] = {'type': 'DAY', 'field': 'person_id'}

        # if joined_fields not in ["", None]:
        job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)
        job.priority = 'BATCH'
        job.priority = 'INTERACTIVE'
        job.priority = self.priority
        job.dry_run = True
        self.log(module='submit-job',
                 subject=self.get_tablename(),
                 action='dry-run',
                 value={'priority':self.priority, 'parition':self.partition})

        try:
            os.makedirs(self.logpath)
        except OSError:
            print("cannot create logpath: {}".format(self.logpath))

        try:
            log_dataset_path = os.path.join(self.logpath, self.idataset)
            os.makedirs(log_dataset_path)
        except OSError:
            print("cannot create input dataset log path: {}".format(log_dataset_path))

        sql_filepath = os.path.join(self.logpath, self.idataset, self.tablename + ".sql")
        with open(sql_filepath, 'w') as sql_file:
            sql_file.write(sql)

        r = client.query(sql, location='US', job_config=job)
        if r.errors is None and r.state == 'DONE':
            job.dry_run = False
            #
            # NOTE: This is a hack that enables table clustering (hope it works)
            # The hack is based on an analysis of the source code of bigquery
            #

            r = client.query(sql, location='US', job_config=job)
            self.log(module='submit',
                     subject=self.get_tablename(),
                     action='submit-job',
                     table=table_name,
                     status='pending',
                     value=r.job_id,
                     object='bigquery')
            self.wait(client, r.job_id)
            self.finalize(client)
            #
            # At this point we must try to partition the table
        else:
            self.log(module='submit',
                     subject=self.get_tablename(),
                     action='submit-job',
                     table=table_name,
                     status='error',
                     value=r.errors)
            print(r.errors)

    def wait(self, client, job_id):
        self.log(module='wait', subject=self.get_tablename(), action="sleep", value=job_id)
        status = 'NONE'

        iteration = 1
        while True:
            status = client.get_job(job_id).state

            if status == 'DONE':
                break
            else:
                time.sleep(5 * iteration)
                # extend the sleep time each time the job is unfinished
                iteration += 1

        self.log(module='wait', action='awake', status=status)

    def finalize(self, client):
        i_dataset, i_table = self.get_tablename().split('.')
        ischema = client.get_table(client.dataset(i_dataset).table(i_table)).schema
        table = client.get_table(client.dataset(i_dataset + '_deid').table(i_table))
        fields = [field.name for field in table.schema]

        newfields = [bq.SchemaField(name=field.name,
                                    field_type=field.field_type,
                                    description=field.description) for field in ischema if field.name in fields]
        table.schema = newfields
        client.update_table(table, ['schema'])


if __name__ == '__main__':
    PARAM_KEYS = ['idataset', 'private_key', 'rules', 'input-folder']
    SYS_ARGS = Parser.sys_args()
    HANDLE = AOU(**SYS_ARGS)
    if HANDLE.initialize(age_limit=50, max_day_shift=365):
        HANDLE.do()
    else:
        print("Unable to initialize process ")
        print("\tInsure that the parameters are correct")
