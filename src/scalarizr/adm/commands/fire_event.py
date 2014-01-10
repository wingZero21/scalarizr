from scalarizr.bus import bus
from scalarizr.adm.command import Command


class FireEvent(Command):
    """
    Usage:
      fire-event <name> [<kv>...]
    """

    def __call__(self, name=None, kv=None):
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
