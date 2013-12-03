import inspect

import prettytable

from scalarizr.adm.command import Command
from scalarizr.util import system2


class ListMessages(Command):
    """
    Display list of messages.

    Usage:
      list-messages [--name=<name>]

    Options:
      -n, --name
    """
    aliases = ['lm']

    def table(self, data_rows, header=False):
        """Returns PrettyTable object applicable to print"""
        max_row_length = len(data_rows[0]) if header else max(map(len, data_rows))
        table = prettytable.PrettyTable()
        table.header = header
        table.field_names = data_rows.pop(0) if header else xrange(max_row_length)

        for row in data_rows:
            row_length = len(row)
            if row_length != max_row_length:
                row = row.append([None] * max_row_length)[0:max_row_length]
            table.add_row(row)

        return table


    def __call__(self, name=None):
        try:
            conn = self.get_db_conn()
            cur = conn.cursor()
            query = "SELECT `message_id`,`message_name`,\
                `out_last_attempt_time`,`is_ingoing`,`in_is_handled`\
                FROM p2p_message"
            if name:
                query += " WHERE `message_name`='%s'" % name
            cur.execute(query)

            res = []

            for row in cur.fetchall():
                res.append([row[0],row[1], row[2],'in' if row[3] else 'out',
                        'yes' if row[4] else 'no'])
            self.display(res)
        except Exception,e:
            LOG.warn('Error connecting to db or not correct request look '
                    'at in sradm>ListMessagesCommand>method `run`. Details: %s'% e)
        finally:
            cur.close()