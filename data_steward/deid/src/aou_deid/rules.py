"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    The de-identification code applies to both the projection and filters (relational algebra terminology).
    The code will apply to both relational tables or meta-tables (alternative to relational modeling).

    Rules are expressed in JSON format with limited vocabulary and largely based
    on templates for every persistent data-store.
    We have rules stored in one-place and the application of rules in another.
    This will allow for rules to be able to be shared.

    Shifting:
        We shift dates given a random function or given a another table that has the number of days for the shift
    Generalization
        Rules for generalization are applied on a projected set of tuples and can have an conditions
    Suppression:
        Suppression rules apply to tuples provided a relational table and/or rows
    Compute :
        We have added this feature as a result of uterly poor designs we have
        encountered that has computed fields stored
"""
from __future__ import print_function

import numpy as np

from parser import Parse

APPLY = 'apply'
NAME = 'name'
LABEL = 'label'
RULE_TYPES = ['generalize', 'compute', 'suppress', 'shift']

class Rules(object):
    COMPUTE = {
        "year": "EXTRACT (YEAR FROM :FIELD) AS :FIELD",
        "month": "EXTRACT (MONTH FROM :FIELD) AS :FIELD",
        "day": "EXTRACT (DAY FROM :FIELD) AS :FIELD",
        "id": "SELECT :FIELD FROM :table where :key_field = :key_value",
    }

    def __init__(self):
        self.cache = {}
        self.store_syntax = {
            "sqlite": {
                APPLY: {
                    "REGEXP": "LOWER(:FIELD) REGEXP LOWER(':VAR')",
                    "COUNT": "SELECT COUNT(DISTINCT :FIELD) FROM :TABLE WHERE :KEY=:VALUE"
                },
                "cond_syntax": {
                    "IF":"CASE WHEN",
                    "OPEN":"",
                    "THEN":"THEN",
                    "ELSE":"ELSE",
                    "CLOSE":"END",
                },
                "random": "random() % 365 "
            },
            "bigquery": {
                APPLY: {
                    "REGEXP": "REGEXP_CONTAINS (LOWER(:FIELD), LOWER(':VAR'))",
                    "COUNT": "SELECT COUNT (DISTINCT :KEY) FROM :TABLE WHERE :KEY=:VALUE",
                },
                "cond_syntax": {
                    "IF": "IF",
                    "OPEN": "(",
                    "THEN": ", ",
                    "ELSE": ", ",
                    "CLOSE": ")",
                },
                "random": "CAST( (RAND() * 364) + 1 AS INT64)"
            },
            "postgresql": {
                "cond_syntax": {
                    "IF": "CASE WHEN",
                    "OPEN": "",
                    "THEN": "THEN",
                    "ELSE": "ELSE",
                    "CLOSE": "END",
                },
                "shift": {
                    "date": "FIELD INTERVAL 'SHIFT DAY' ",
                    "datetime": "FIELD INTERVAL 'SHIFT DAY'",
                },
                "random": "(random() * 364) + 1 :: int"
            }
        }
        self.cache['compute'] = Rules.COMPUTE

    def set(self, key, rule_id, **args):
        if key not in RULE_TYPES:
            raise (key + " is Unknown, [suppress, generalize, compute, shift] are allowed")

        if key not in self.cache:
            self.cache[key] = {}

        if rule_id not in self.cache[key]:
            self.cache[key][rule_id] = []

        self.cache[key][rule_id].append(args)

    def get(self, key, rule_id):
        return self.cache[key][rule_id]

    def validate(self, rule_id, entry):
        """
        Validating if the application of a rule relative to a table is valid
        """
        p = rule_id in self.cache

        q = []

        for row in entry:

            if 'rules' in row:
                row_rules = row.get('rules', '')
                if not isinstance(row_rules, list) and row_rules.startswith('@')  and "into" in row:
                    #
                    # Making sure the expression is {apply, into} suggesting a
                    # rule applied relative to an attribute
                    # finding the rules that need to be applied to a given attribute
                    _id, _key = row_rules.replace('@', '').split('.')
                    q.append((_id == rule_id) and (_key in self.cache[rule_id]))
                else:
                    q.append(1)
            elif isinstance(row, list) or not p:
                #
                # assuming we are dealing with a list of strings applied
                # and dealign with self contained rules
                q.append(1)

        q = sum(q) == len(q)

        return (p and q) or (not p and q)

class Deid(Rules):
    """
    This class is designed to apply rules to structured data. For this to work we consider the following:
        - a rule can be applied to many fields and tables (broadest sense)
        - a field can have several rules applied to it:

    """
    def __init__(self):
        Rules.__init__(self)

    def validate(self, rule_id, entry):
        payload = None

        if super(Deid, self).validate(rule_id, entry):

            payload = {}
            payload = {"args": []}
            payload["pointer"] = getattr(self, rule_id)
            for row in entry:

                #
                # @TODO: Ensure that an error is thrown if the rule associated is not found

                p = getattr(Parse, rule_id)(row, self.cache)
                payload['args'] += [p]

        return payload

    def aggregate(self, sql, **args):
        pass

    def log(self, **args):
        print(args)

    def generalize(self, **args):
        """
        This function will apply generalization given a set of rules provided.

        The rules apply to both meta tables and relational tables

        :fields list of target fields
        :rules  list of rules to be applied
        :label  context of what is being generalized
        """
        fields = args.get('fields', [args.get('value_field', '')])
        label = args.get('label', '')
        rules = args.get('rules', '')


        store_id = args.get('store', 'sqlite')
        syntax = self.store_syntax.get(store_id, 'sqlite').get('cond_syntax', {})
        out = []
        label = args.get('label', '')
        for name in fields:
            cond = []
            for rule in rules:
                qualifier = rule.get('qualifier', '')
                if APPLY in rule:
                    #
                    # This will call a built-in SQL function (non-aggregate)'
                    # qualifier = rule.get('qualifier', '')
                    fillter = args.get('filter', name)
                    self.log(module='generalize', label=label.split('.')[1], on=name, type=rule.get(APPLY, ''))

                    if APPLY not in self.store_syntax[store_id]:
                        regex = [rule.get(APPLY, ''),
                                 "(",
                                 fillter,
                                 " , '",
                                 "|".join(rule.get('values', [])),
                                 "') ",
                                 qualifier]
                    else:
                        application_rule = rule.get(APPLY, '')
                        template = self.store_syntax.get(store_id, {})
                        template = template.get(APPLY, {})
                        template = template.get(application_rule, '')
                        regex = template.replace(':FIELD', fillter)
                        regex = regex.replace(':FN', rule.get(APPLY, ''))
                        if ':VAR' in template:
                            regex = regex.replace(":VAR", "|".join(rule.get('values', [])))
                        if rule.get(APPLY, '') in ['COUNT', 'AVG', 'SUM']:
                            #
                            # We are dealing with an aggregate expression.
                            # At this point it is important to know what we are counting
                            # count(:field) from :table [where fillter]
                            #
                            regex = regex.replace(':TABLE', args.get('table', ''))
                            regex = regex.replace(':KEY', args.get('key_field', ''))
                            regex = regex.replace(':VALUE', args.get('value_field', ''))

                            if 'on' in rule and 'key_row' in args:
                                regex += ' AND ' if 'qualifier' in rule else ' WHERE '
                                regex += args.get('key_row', '') + " IN ('" + "', '".join(rule.get('on', [])) + "')"

                            regex = ' '.join(['(', regex, ')', qualifier])
                        else:
                            regex = ' '.join([regex, qualifier])
                        #
                        # Is there a filter associated with the aggregate function or not
                        #

                    _into = args.get('into', rule.get('into', ''))

                    if not _into.isnumeric():
                        _into = "'" + _into + "'"

                    regex = "".join(regex)
                    cond += [" ".join([syntax['IF'], syntax['OPEN'], regex, syntax['THEN'], _into])]

                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [syntax['ELSE']]

                else:
                    #
                    # We are just processing a generalization given a list of
                    # values with no overhead of an aggregate function
                    # @TODO: Document what is going on here
                    #   - We are attempting to have an if or else type of
                    # generalization given a list of values or function
                    #   - IF <fillter> IN <values>, THEN <generalized-value> else <attribute>
                    self.log(module='generalize', label=label.split('.')[1], on=name, type='inline')
                    fillter = args.get('filter', name)
                    qualifier = rule.get('qualifier', '')
                    values = "('" + "','".join(rule.get('values', [])) + "')"
                    _into = args.get('into', rule.get('into', ''))

                    regex = " ".join([fillter, qualifier, values])

                    if not _into.isnumeric():
                        _into = "'" + _into + "'"

                    cond += [" ".join([syntax['IF'], syntax['OPEN'], regex, syntax['THEN'], _into])]

                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [syntax['ELSE']]

            # Let's build the syntax here to make it sound for any persistence storage
            cond.append(name)
            cond_counts = sum([1 for xchar in cond if syntax['IF'] in xchar])
            cond.extend(np.repeat(syntax['CLOSE'], cond_counts).tolist())
            cond.extend(['AS', name])
            result = {"name":name, APPLY:" ".join(cond), "label":label}

            if 'on' in args:
                result['on'] = args.get('on', '')

            out.append(result)
        #
        # This will return the fields that need generalization as specified.
        #

        return out

    def suppress(self, **args):
        """
        We should be able to suppress the columns and/or rows provided specification

        NOTE: Non-sensical specs aren't handled for instance
        """

        rules = args.get('rules', {})
        label = args.get('label', '')
        fields = args.get('fields', [])

        store_id = args.get('store', '')
        apply_fn = self.store_syntax.get(store_id, {}).get(APPLY, {})
        out = []

        if fields and 'on' not in args:
            #
            # This applies on a relational table's columns, it is simple we just nullify the fields
            #
            for name in fields:
                if not rules:
                    #
                    # This scenario, we know the fields upfront and don't have a rule for them
                    # We just need them removed (simple/basic case)
                    #
                    #-- This will prevent accidental type changing from STRING TO INTEGER
                    value = ('NULL AS ' + name) if '_id' in name else ("'' AS " + name)
                    out.append({"name": name, APPLY: value, "label": label})
                    self.log(module='suppression', label=label.split('.')[1], type='columns')
                else:
                    #
                    # If we have alist of fields to be removed, The following code will figure out which ones apply
                    # This will apply to all tables that are passed through this engine
                    #

                    for rule in rules:
                        if APPLY not in rules:
                            if name in rule.get('values', []):
                                #-- This will prevent accidental type changing from STRING TO INTEGER
                                value = ('NULL AS ' + name) if '_id' in name else ("'' AS " + name)
                                out.append({"name": name, APPLY: value, "label": label})

            self.log(module='suppress', label=label.split('.')[1], on=fields, type='columns')

        else:
            #
            # In this case we are just removing the entire row we will be expecting :
            #   - filter    as the key field to match the filter
            #   - The values of the filter are provided by the rule
            #

            for rule in rules:
                qualifier = args.get('qualifier', '')
                apply_filter = {
                    'IN': 'NOT IN',
                    '=': '<>',
                    'NOT IN': 'IN',
                    '<>': '=',
                    '': 'IS FALSE',
                    'TRUE': 'IS FALSE',
                }

                application_rule = rule.get(APPLY, '')

                if application_rule in apply_fn:
                    template = self.store_syntax.get(store_id, {}).get(APPLY, {}).get(application_rule, '')
                    key_field = args.get('filter', args.get('on', ''))
                    expression = template.replace(':VAR', "|".join(rule.get('values', [])))
                    expression = expression.replace(':FN', rule.get(APPLY, ''))
                    expression = expression.replace(':FIELD', key_field)
                    suppression_exp = {"filter": expression + ' ' + qualifier, "label": label}
                elif 'on' in args:
                    #
                    # If we have no application of a function, we will assume an
                    # expression of type <attribute> IN <list>
                    # we have a basic SQL statement here
                    qualifier = 'IN' if qualifier == '' else qualifier
                    qualifier = apply_filter[qualifier]
                    expression = " ".join([args.get('on', ''), qualifier, "('" +  "', '".join(rule.get('values', [])) + "')"])
                    suppression_exp = {"filter": expression, "label": label}

                try:
                    self.cache['suppress']['FILTERS'].append(suppression_exp)
                except AttributeError:
                    current = self.cache.get('suppress', {}).get('FILTERS', [])
                    self.cache['suppress']['FILTERS'] = [current, suppression_exp]
                except KeyError:
                    self.cache['suppress'] = {'FILTERS': [suppression_exp]}

        return out


    def shift(self, **args):
        """
        Shifting will always occur on a column.

        Either the column is specified or is given a field with conditional values
           - simply secified are physical date/datetime columns (nothing special here)
           - But if we are dealing with a meta table, a condition must be provided. {key, value}
        """
        label = args.get('label', '')
        #
        # we can not shift dates on records where filters don't apply
        #
        if not self.cache.get('suppress', {}).get('FILTERS', []):
            return []

        out = []

        fields = args.get('fields', '')

        for name in fields:
            rules = args.get('rules', '')
            result = {APPLY: rules.replace(':FIELD', name), "label": label, "name": name}
            if 'on' in args:
                result_apply = result.get(APPLY, '')
                result['on'] = args.get('on', '')
                xchar = ' AS ' if ' AS ' in result_apply else ' as '
                suffix = xchar + result_apply.split(xchar)[-1]

                result[APPLY] = ' '.join(['CAST(', result_apply.replace(suffix, ''), 'AS STRING ) ', suffix])
            out.append(result)

            break

        return out

    def compute(self, **args):
        """
        Compute functions are simple applications of aggregate functions on a field (SQL Standard)

        Additionally we add a value from another table thus an embedded query
        with a table{key_field, key_value, table, field} as follows:
            field       projection of the embedded query
            key_field   field used in the join/filter
            key_value   external field that will hold the value of key_field
            table       external table
        """
        fields = args.get('fields', [args.get('key_field', '')])
        value_field = args.get('value_field', '')
        label = args.get('label', '')
        out = []

        statement = args.get('rules', '')
        statement = statement.replace(':FIELD', fields[0])
        statement = statement.replace(':value_field', value_field)
        statement = statement.replace(':key_field', args.get('key_field', ''))
        statement = statement.replace(':table', args.get('table', ''))

        out.append({APPLY: statement, "name": fields[0], "label": label})

        return out

    def apply(self, info, store_id=None):
        """
        :info    is a specification of a table and the rules associated
        """
        if store_id is None:
            store_id = 'sqlite'

        out = []
        r = {}
        ismeta = info.get('info', {}).get('type', False)
        for rule_id in RULE_TYPES:

            if rule_id in info:
                r = self.validate(rule_id, info[rule_id])
                if r:
                    r = dict(r, **{'ismeta': ismeta})
                    pointer = r.get('pointer', {})
                    tmp = [pointer(** dict(args, **{"store": store_id})) for args in r.get('args', [])]

                    if tmp:
                        for _item in tmp:
                            if _item:
                                if isinstance(_item, dict):
                                    out.append(_item)
                                else:
                                    out += _item

        #
        # we should try to consolidate here

        return out
