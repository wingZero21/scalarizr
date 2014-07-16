
import prettytable
import itertools
import os
import sys

from scalarizr.node import __node__
from scalarizr.node import base_dir as scalr_base_dir
from scalarizr.queryenv import QueryEnvService


def make_table(data_rows, header=None):
    """Returns PrettyTable object applicable to print"""

    def normalize_multiline(x):
        if sys.version_info[0:2] < (2, 7) and type(x) == str:
            return x.replace('\n', '')
        return x

    if not data_rows:
        data_rows = [[]]
    max_row_length = len(header) if header else max(map(len, data_rows))
    table = prettytable.PrettyTable(header if header else range(max_row_length))
    table.header = bool(header)

    for row in data_rows:
        if not row:
            row = []
        row = [normalize_multiline(x) for x in row]
        row_length = len(row)
        if row_length > 0:
            if row_length != max_row_length:
                row = (row + [None]*max_row_length)[:max_row_length]
            table.add_row(row)

    return table


def encode(obj, encoding='ascii'):
    if isinstance(obj, basestring):
        try:
            return obj.encode(encoding)
        except UnicodeEncodeError:
            return obj
    elif isinstance(obj, list):
        return [encode(item) for item in obj]
    elif isinstance(obj, dict):
        return dict((encode(k), encode(v)) for k, v in obj.items())
    else:
        return obj


def new_queryenv():
    queryenv_creds = (__node__['queryenv_url'],
                      __node__['server_id'],
                      os.path.join(scalr_base_dir, __node__['crypto_key_path']))
    queryenv = QueryEnvService(*queryenv_creds)
    api_version = queryenv.get_latest_version()
    queryenv = QueryEnvService(*queryenv_creds, api_version=api_version) 
    return queryenv
