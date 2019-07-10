"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    This file is designed to put rules into a canonical form.
    The framework distinguishes rules from their applications so as to allow
    simulation and have visibility into how the rules are applied given a variety
    of contexts

"""
from argparse import ArgumentParser
import sys


class Parse(object):
    """
    This utility class implements the various ways in which rules and their
    applications are put into a canonical form

    Having rules in a canonical form makes it easy for an engine to apply them
    in batch
    """
    @staticmethod
    def init(rule_id, row, cache):
        try:
            row_rules = row.get('rules', '')
            _id, _key = row_rules.replace('@', '').split('.')
        except ValueError:
            _id, _key = None, None

        label = ".".join([_id, _key]) if _key else rule_id

        p = {'label': label}

        if _id and _key  and _id in cache and _key in cache[_id]:
            p['rules'] = cache[_id][_key]

        for key in row:
            p[key] = p.get(key, row.get(key, ''))

        return p

    @staticmethod
    def shift(row, cache):
        return Parse.init('shift', row, cache)

    @staticmethod
    def generalize(row, cache):
        """
        parsing generalization and translating the features into a canonical form of stores
        """
        p = Parse.init('generalize', row, cache)

        return p

    @staticmethod
    def suppress(row, cache):
        """
        setup suppression rules to be applied the the given 'row' i.e entry
        """
        return Parse.init('suppress', row, cache)

    @staticmethod
    def compute(row, cache):
        return Parse.init('compute', row, cache)

    @staticmethod
    def sys_args():
        sys_args = {}
        if len(sys.argv) > 1:

            len_args = len(sys.argv)
            for i in range(1, len_args):
                value = None
                if sys.argv[i].startswith('--'):
                    key = sys.argv[i].replace('-', '')
                    sys_args[key] = 1

                    if i + 1 < len_args and sys.argv[i + 1].startswith('--') is False:
                        value = sys.argv[i + 1] = sys.argv[i + 1].strip()

                    if key and value:
                        sys_args[key] = value

                i += 2
        return sys_args


def parse_args():
    """
    Standard python command line argument parsing.

    This is preferable to defining your own command line parser because it
    already contains checks and other type information. This one will work, but
    has not yet been integrated.
    """
    parser = ArgumentParser(description='Parse deid command line arguments')
    parser.add_argument('--rules',
                        action='store', dest='rules',
                        help='Filepath to the JSON file containing rules',
                        required=True)
    parser.add_argument('--idataset',
                        action='store', dest='idataset',
                        help=('Name of the input dataset (an output dataset '
                              'with suffix _deid will be generated)'),
                        required=True)
    parser.add_argument('--private_key', dest='private_key', action='store',
                        required=True,
                        help='Service account file location')
    parser.add_argument('--table', dest='table', action='store', required=True,
                        help='Path that specifies how rules are applied on a table')
    parser.add_argument('--action', dest='action', action='store', required=True,
                        choices=['submit', 'simulate', 'debug'],
                        help=('simulate: generate simulation without creating an '
                              'output table\nsubmit: create an output table\n'
                              'debug: print output without simulation or submit '
                              '(runs alone)')
                       )
    parser.add_argument('--cluster', dest='cluster', action='store_true',
                        help='Enable clustering on person_id')
    parser.add_argument('--log', dest='log', action='store',
                        help='Filepath for the log file')
    return parser.parse_args()
