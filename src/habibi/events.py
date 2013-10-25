__author__ = 'spike'

import sys
import logging
import threading
import Queue

LOG = logging.getLogger(__name__)

class NotificationCenter(object):

    breakpoints = dict()
    waitlist = dict()

    def add_breakpoint(self, breakpoint, fn):
        if not breakpoint in NotificationCenter.breakpoints:
            NotificationCenter.breakpoints[breakpoint] = list()
        NotificationCenter.breakpoints[breakpoint].append(fn)

    def wait_for_breakpoint(self, breakpoint, timeout=None, fn=None):
        event = threading.Event()
        queue = fn and Queue.Queue() or None
        if not isinstance(breakpoint, Breakpoint):
            breakpoint = Breakpoint(breakpoint)
        LOG.debug('Wait for breakpoint: %s' % breakpoint.cond)

        self.waitlist[breakpoint] = (event, queue, fn)
        try:
            event.wait(timeout)
            if not event.isSet():
                raise Exception('Timeout occured while waiting for breakpoint')

            if fn:
                res = queue.get()
                if res['status'] == 'error':
                    e_t, e_v, e_tb = res['error']
                    raise e_t, e_v, e_tb
                else:
                    return res['result']
        finally:
            del self.waitlist[breakpoint]

    def apply_breakpoints(self, *args, **kwargs):
        if len(args) == 1 and not kwargs:
            if isinstance(args[0], Breakpoint):
                bp_to_apply = args[0]
            elif isinstance(args[0], dict):
                bp_to_apply = Breakpoint(args[0])
            else:
                raise Exception("Can't apply breakpoint. Args: %s Kwargs: %s" % (args, kwargs))
        else:
            bp_to_apply = Breakpoint(kwargs)

        # TODO: fix race condition
        for bp, event_and_queue_and_maybe_fn in self.waitlist.iteritems():
            if bp in bp_to_apply:
                event, queue, fn = event_and_queue_and_maybe_fn
                if fn is not None:
                    try:
                        queue.put(dict(status='ok', result=fn(bp_to_apply)))
                    except:
                        queue.put(dict(status='error', error=sys.exc_info()))
                event.set()

        to_notify = list()
        for bp, fn in self.breakpoints.iteritems():
            if bp in bp_to_apply:
                to_notify.append(fn)

        for fn_list in to_notify:
            for fn in fn_list:
                fn(bp_to_apply)


class Breakpoint(object):

    def __init__(self, cond):
        bp = cond.copy()
        for key in ('source', 'target'):
            if bp.get(key):
                if '.' in bp[key]:
                    bp[key + '_behaviour'], bp[key + '_index'] = bp[key].split('.')
                else:
                    bp[key + '_behaviour'] = bp[key]
                del bp[key]
        self.cond = bp


    def __getitem__(self, item):
        return self.cond[item]


    def __contains__(self, test_cond):
        test_bp = test_cond if isinstance(test_cond, Breakpoint) else Breakpoint(test_cond)
        for k, v in test_bp.cond.iteritems():
            k, attr = k.split('.', 1) if '.' in k else (k, None)
            # TODO: use wildcards for strings, add lambdas support
            if not k in self.cond:
                return False
            self_value = self.cond[k] if not attr else getattr(self.cond[k], attr)
            if self_value != v:
                return False
        return True


    def __getattr__(self, item):
        try:
            return self.cond[item]
        except KeyError:
            raise AttributeError(item)


def breakpoint(**kwds):
    def wrapper(fn):
        bp = Breakpoint(kwds)
        fn._breakpoint = bp
        return fn
    return wrapper
