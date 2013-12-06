
import prettytable


def make_table(data_rows, header=None):
    """Returns PrettyTable object applicable to print"""
    table = prettytable.PrettyTable()
    table.header = bool(header)
    max_row_length = len(header) if header else max(map(len, data_rows))
    table.field_names = header if header else xrange(max_row_length)

    for row in data_rows:
        row_length = len(row)
        if row_length != max_row_length:
            row = (row + [None]*max_row_length)[:max_row_length]
        table.add_row(row)

    return table


def encode(obj, encoding='ascii'):
    if isinstance(obj, basestring):
        return obj.encode(encoding)
    elif isinstance(obj, list):
        return [encode(item) for item in obj]
    elif isinstance(obj, dict):
        return {encode(k): encode(v) for k, v in obj.items()}
    else:
        return obj
