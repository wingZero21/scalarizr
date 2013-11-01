
import uuid
import sys
import threading
import time
import logging
import traceback

from scalarizr import rpc
from scalarizr.node import __node__
from scalarizr.util import Singleton
from scalarizr.messaging import Queues, Messages


LOG = logging.getLogger(__name__)

class OperationAPI(object):

    __metaclass__ = Singleton

    def __init__(self):
        self._ops = {}
        self.rotate_thread = threading.Thread(
            name='Rotate finished operations', 
            target=OperationAPI.rotate_runnable
        )
        self.rotate_thread.setDaemon(True)
        self.rotate_thread.start()

    @rpc.query_method
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

    def find(self, name=None, finished_before=None):
        if name:
            ret = [op for op in self._ops.values() if op.name == name]
        else:
            ret = self._ops.values()
        if finished_before:
            now = time.time()
            ret = [op for op in ret 
                    if op.finished_at and now - op.finished_at > finished_before]
        return ret

    def run(self, name, func, async=False, **kwds):
        op = self.create(name, func, **kwds)
        if async:
            return op.run_async()
        else:
            return op.run()

    @classmethod
    def rotate_runnable(cls):
        api = cls()
        one_day = 86400
        two_days = one_day * 2
        while True:
            time.sleep(one_day)
            LOG.debug('Rotating operations finished older then 2 days')
            for op in api.find(finished_before=two_days):
                api.remove(op.operation_id)



class _LogHandler(logging.Handler):
    def __init__(self, op):
        self.op = op
        logging.Handler.__init__(self, logging.INFO)

    def emit(self, record):
        trace_marker = 'Traceback (most recent call last)'
        msg = self.format(record)
        if trace_marker in msg:
            msg = msg[0:msg.index(trace_marker)].strip()
        self.op.logs.append(msg)


class Operation(object):

    def __init__(self, name, func, **kwds):
        self.operation_id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.cancel_func = kwds.get('cancel_func')
        self.status = kwds.get('status', 'new')
        self.result = kwds.get('result')
        self.logs = kwds.get('logs', [])
        self.error = kwds.get('error')
        self.started_at = None
        self.finished_at = None
        self.async = False
        self.canceled = False
        self.thread = None
        self.logger = None
        self._init_log()

    def _init_log(self):
        self.logger = logging.getLogger('scalarizr.ops.{0}'.format(self.name))
        hdlr = _LogHandler(self)
        hdlr.setLevel(logging.INFO)
        self.logger.addHandler(hdlr)    

    def _in_progress(self):
        self.status = 'in-progress'
        self.started_at = time.time() 
        try:
            self._completed(self.func(self))
            if self.canceled:
                raise Exception('User canceled')
        except:
            self._failed()
        finally:
            self.finished_at = time.time()
            __node__['messaging'].send('OperationResult', body=self.serialize())

    def run(self):
        self._in_progress()
        return self.result

    def run_async(self):
        self.thread = threading.Thread(
            name='Task {0}'.format(self.name), 
            target=self._in_progress
        )
        self.async = True
        self.thread.start()
        return self.operation_id

    def cancel(self):
        self.canceled = True
        if self.cancel_func:
            try:
                self.cancel_func(self)
            except:
                msg = ('Cancelation function failed for '
                        'operation {0}').format(self.operation_id)
                self.logger.exception(msg)

    def _failed(self, *exc_info):
        self.error = exc_info or sys.exc_info()
        self.status = 'failed' if not self.canceled else 'canceled'
        self.logger.error('Operation "%s" (id: %s) failed. Reason: %s', 
                self.name, self.operation_id, self.error[1], exc_info=self.error)

    def _completed(self, result=None):
        self.result = result
        self.status = 'completed'

    def serialize(self):
        ret = {
            'id': self.operation_id,
            'name': self.name,
            'status': self.status,
            'result': self.result,
            'error': None,
            'trace': None,
            'logs': self.logs
        }
        if self.error:
            ret['error'] = str(self.error[1])
            ret['trace'] = '\n'.join(traceback.format_tb(self.error[2]))
        return ret
