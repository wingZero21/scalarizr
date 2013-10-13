
import uuid
import sys
import threading
import time
import logging

from scalarizr import rpc
from scalarizr.bus import bus
from scalarizr.messaging import Queues, Messages


LOG = logging.getLogger(__name__)

class OperationAPI(object):

    _instance = None

    def __new__(cls, *args, **kwds):
        if not cls._instance:
            cls._instance = super(OperationAPI, cls).__new__(cls, *args, **kwds)
        return cls._instance

    def __init__(self):
        self._ops = {}
        self.rotate_thread = threading.Thread(
            name='Rotate finished operations', 
            target=OperationAPI.rotate_runnable
        )
        self.rotate_thread.setDaemon(True)
        self.rotate_thread.start()

    @rpc.service_method
    def result(self, operation_id=None):
        return self.get(operation_id).serialize()

    def create(self, name, func, **kwds):
        op = Operation(name, func, **kwds)
        self._ops[op.operation_id] = op
        return op

    def get(self, operation_id):
        # TODO: wrap KeyError
        return self._ops[operation_id]

    def remove(self, operation_id):
        del self._ops[operation_id]

    def find(self, name=None, finished_older_then=None):
        if name:
            ret = [op for op in self._ops.values() if op.name == name]
        else:
            ret = self._ops.values()
        if finished_older_then:
            now = time.time()
            ret = [op for op in ret 
                    if op.finished_at and now - op.finished_at > finished_older_then]
        return ret

    def go_with(self, name, func, async=False, **kwds):
        op = self.create(name, func, **kwds)
        if async:
            op.start()
            return op.operation_id
        else:
            return op.execute()

    @classmethod
    def rotate_runnable(cls):
        api = cls()
        one_day = 86400
        two_days = one_day * 2
        while True:
            time.sleep(one_day)
            LOG.debug('Rotating operations finished older then 2 days')
            for op in api.find(finished_older_then=two_days):
                api.remove(op.operation_id)



class _LogHandler(logging.Handler):
    def __init__(self, op):
        self.op = op
        super(_LogHandler, self).__init__(logging.INFO)

    def emit(self, record):
        self.op.logs.append(record)


class Operation(object):

    def __init__(self, name, func, **kwds):
        self.operation_id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.status = kwds.get('status', 'new')
        self.result = kwds.get('result')
        self.logs = kwds.get('logs', [])
        self.error = kwds.get('error')
        self.started_at = None
        self.finished_at = None
        self.async = False
        self.thread = None
        self.logger = None
        self._init_log()

    def _init_log(self):
        self.logger = logging.getLogger('scalarizr.ops.{0}'.format(self.name))
        hdlr = _LogHandler(self)
        hdlr.setLevel(logging.INFO)
        self.logger.addHandler(hdlr)    

    def _run(self):
        self.status = 'in-progress'
        self.started_at = time.time() 
        try:
            self.complete(self.func(self))
        except:
            self.fail()
        finally:
            self.finished_at = time.time()
            svs = bus.messaging_service
            msg = svs.new_message(Messages.OPERATION_RESULT, None, self.serialize())
            svs.get_producer().send(Queues.CONTROL, msg)

    def execute(self):
        self._run()
        return self.result

    def start(self):
        self.thread = threading.Thread(
            name='Task {0}'.format(self.name), 
            target=self._run
        )
        self.async = True
        self.thread.start()

    def fail(self, *exc_info):
        self.error = exc_info or sys.exc_info()
        self.status = 'failed'
        self.logger.exception('Operation "%s" (id: %s) failed', 
                self.name, self.operation_id, exc_info=self.error)

    def complete(self, result=None):
        self.result = result
        self.status = 'completed'

    def serialize(self):
        ret = {
            'id': self.operation_id,
            'status': self.status,
            'result': None,
            'error': None,
            'logs': 'todo: serialize {0} log entries'.format(len(self.logs))
        }
        if self.status == 'completed':
            ret['result'] = self.result
        else:
            ret['error'] =  'todo: serialize exception',
        return ret
