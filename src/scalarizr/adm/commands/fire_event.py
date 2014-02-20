from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.node import __node__


class FireEvent(Command):
    """
    Fires event with given name and parameters on Scalr. Parameters should be
    passed in <key>=<value> form.

    Usage:
      fire-event <name> [<kv>...]
    """

    def __call__(self, name=None, kv=None):
        if not kv:
            kv = {}
        msg_service = bus.messaging_service
        producer = msg_service.get_producer()

        producer.endpoint = __node__['producer_url']

        params = {}
        for pair in kv:
            if '=' in pair:
                k, v = pair.split('=')
                params[k] = v

        msg = msg_service.new_message('FireEvent', body={'event_name': name,
                                                         'params': params})
        print 'Sending %s' % name
        producer.send('control', msg)

        print "Done"


commands = [FireEvent]
