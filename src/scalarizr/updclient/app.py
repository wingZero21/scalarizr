'''
Created on Jan 19, 2012

@author: marat
'''

import SocketServer
import logging
import optparse
import os
import select
import signal
import socket
import sys
import threading
import time
import wsgiref.simple_server

from scalarizr import linux, util, rpc
from scalarizr.api.binding import jsonrpc_http
from scalarizr.updclient import api as update_api


LOG = logging.getLogger('upd.client')


if linux.os.windows_family:
    import servicemanager
    import win32api
    import win32service
    import win32serviceutil


    class WindowsService(win32serviceutil.ServiceFramework):
        _svc_name_ = "ScalrUpdClient"
        _svc_display_name_ = "Scalr Update Client"
        _upd = None

        def __init__(self, args=None):
            if args != None:
                win32serviceutil.ServiceFramework.__init__(self, args)
            self._upd = UpdClient()

            def handler(*args):
                return True
            win32api.SetConsoleCtrlHandler(handler, True)


        def SvcDoRun(self):
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                  servicemanager.PYS_SERVICE_STARTED,
                                  (self._svc_name_, ''))
            self._upd.serve_forever()


        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            try:
                self._upd.stop()
            except:
                LOG.warning('Caught error during service termination', exc_info=sys.exc_info())


class UpdClient(util.Server):
    daemonize = False
    verbose = True
    if linux.os.windows:
        base = r'C:\Program Files\Scalarizr'
        pid_file = os.path.join(base, r'var\run\scalr-upd-client.pid')
        log_file = os.path.join(base, r'var\log\scalarizr_update.log')
        del base
    else:
        pid_file = '/var/run/scalr-upd-client.pid'
        log_file = sys.stderr

    api = api_server = api_thread = None

    def __init__(self):
        super(UpdClient, self).__init__()
        self.optparser = optparse.OptionParser(option_list=(
            optparse.Option('-d', '--daemonize', action='store_true', help='daemonize process'),
            optparse.Option('-P', '--pid-file', default=self.pid_file, help='file to store PID in'),
            optparse.Option('-l', '--log-file', default=self.log_file, help='log file'),
            optparse.Option('-v', '--verbose', action='store_true', default=self.verbose, 
                            help='verbose logging'),
            optparse.Option('--get-system-id', action='store_true', default=False, 
                            help='print system-id and exit'),
            optparse.Option('--make-status-file', action='store_true', default=False,
                            help='make status file with current state and exit')
        ))
        self.api = update_api.UpdClientAPI()       


    if not linux.os.windows_family:
        def serve_forever(self):
            self.start()
            try:
                # Register API server
                poller = select.epoll()
                poller.register(self.api_server.fileno(), select.POLLIN)
            
                while self.running:
                    try:
                        events = poller.poll(60)
                        for _ in range(0, len(events)):
                            # epoll already notified that socket is readable
                            self.api_server._handle_request_noblock()
                        
                    except KeyboardInterrupt:
                        self.onSIGINT()
                    except IOError, e:
                        if e.errno == 4:
                            # Interrupted syscall
                            continue
                        raise
                    except:
                        LOG.exception('Caught exception')           
            except:
                self.stop()
                raise



    def do_start(self):
        opts = self.optparser.parse_args()[0]
        self.__dict__.update(vars(opts))

        util.init_logging(self.log_file, self.verbose)
        self._wait_network()

        if self.__dict__.get('get_system_id'):
            try:
                print self.api.get_system_id()
                sys.exit()
            except update_api.NoSystemUUID:
                print "system-id not detected"
                sys.exit(1)
        elif self.__dict__.get('make_status_file'):
            if os.path.exists(self.api.status_file):
                os.unlink(self.api.status_file)
            self.api.bootstrap(dry_run=True)
            self.api.store()
            print 'saved status file: {0}'.format(self.api.status_file)
            sys.exit() 

        if self.daemonize:
            util.daemonize()
        if not linux.os.windows_family:
            signal.signal(signal.SIGHUP, self.onSIGHUP)
            signal.signal(signal.SIGTERM, self.onSIGTERM)

        LOG.info('Starting updclient (pid: %s)', os.getpid())
        self._check_singleton()
        try:
            self._start_api()
            self._write_pid_file()

            self.running = True  
            # It should be here, cause self.api.bootstrap() on Windows
            # leads to updclient restart and self.stop(), that is called for this  
            # checks for self.running is True to perform graceful shutdown

            self.api.bootstrap()
        except:
            self.do_stop()
            LOG.exception('Detailed exception information below:')
            sys.exit(1)


    def do_stop(self):
        LOG.info('Stopping updclient')

        LOG.debug('Kill child processes')
        util.kill_childs(os.getpid())
        time.sleep(.05)  # Interrupt main thread

        if self.api_thread:
            LOG.debug('Stopping API') 
            self.api.shutdown()          
            self.api_server.shutdown()
            self.api_thread.join()

        if os.path.exists(self.pid_file):
            os.unlink(self.pid_file)
        
        LOG.info('Stopped')
        
    def onSIGHUP(self, *args):
        LOG.info('Reloading configuration')
        self.api = update_api.UpdClientAPI()
        self.api.bootstrap()


    def onSIGTERM(self, *args):
        self.stop()

    onSIGINT = onSIGTERM

    
    def _start_api(self):
        LOG.debug('Starting API on port %s', self.api.api_port)
        try:
            wsgi_app = jsonrpc_http.WsgiApplication(
                        rpc.RequestHandler(self.api), 
                        self.api.crypto_file)
            class ThreadingWSGIServer(SocketServer.ThreadingMixIn, 
                                    wsgiref.simple_server.WSGIServer):
                pass
            self.api_server = wsgiref.simple_server.make_server(
                                '0.0.0.0', int(self.api.api_port), wsgi_app,
                                server_class=ThreadingWSGIServer)
        except socket.error:
            LOG.error('Cannot create API server on port %s', self.api.api_port)
            raise
        
        if linux.os.windows_family:
            def serve():
                try:
                    self.api_server.serve_forever()
                except:
                    LOG.exception('API thread died unexpectedly')           

            self.api_thread = threading.Thread(target=serve)
            self.api_thread.start()  


    def _wait_network(self):
        timeout = 60
        for _ in range(0, timeout):
            try:
                sock = socket.create_connection(('win.scalr.net', 80), timeout=5)
            except:
                time.sleep(1)
            else:
                sock.close()
                return
        msg = 'Network is not available. timeout {0} seconds was reached'.format(60)
        raise Exception(msg)


    def _write_pid_file(self):
        with open(self.pid_file, 'w+') as fp:
            fp.write(str(os.getpid()))


    def _check_singleton(self):
        if linux.os.windows or not os.path.exists(self.pid_file):
            return
        with open(self.pid_file) as fp:
            pid = fp.read().strip()
            if not pid:
                return
        with open('/proc/{0}/cmdline'.format(pid)) as fp:
            cmdline = fp.read().split('\x00')
            if 'python' in cmdline[0] and 'scalr-upd-client' in cmdline[1]:
                msg = 'Another updclient instance is already running (pid: {0})'.format(pid)
                LOG.warn(msg)
                sys.exit(1)                


def main():

    if linux.os.windows_family:
        win32serviceutil.HandleCommandLine(WindowsService)
    else:
        svs = UpdClient()
        try:
            svs.serve_forever()
        except KeyboardInterrupt:
            pass



if __name__ == '__main__':
    main()
