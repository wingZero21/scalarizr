import socket
import threading
import logging
import time
import errno

###from lettuce import step, world, before, after

from scalarizr.libs.bases import Task
from scalarizr.services.haproxy import HAProxyInitScript


LOG = logging.getLogger(__name__)


class SocketServer(Task):

    def __init__(self, port):
        super(SocketServer, self).__init__()

        self._address = ('', port)

        self.on(start=lambda: LOG.info("[%s] Starting up", self))
        self.on(complete=lambda r: LOG.info("[%s] Went down", self))
        self.on(error=lambda exc_info: LOG.info("[%s] Crashed", self,
            exc_info=exc_info))

    def _run(self):
        self._killed = False

        sock = socket.socket()
        # allow restarting right after a crash
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self._address)
        sock.listen(5)

        while not self._killed:
            self._handle(*sock.accept())

    def __str__(self):
        return str(self._address)

    def _handle(self, client_sock, address):
        if not self._killed:
            LOG.info("[%s] Got connection from %s", self, address)
            client_sock.send(str(self))
        else:
            # we expect client_sock to be the one created by self._kill()
            LOG.debug("[%s] Recieved kill connection from %s", self, address)
            client_sock.send("debug: being killed")

    def _kill(self):
        self._killed = True
        # make sock.accept() return
        try:
            socket.socket().connect(self._address)
        except socket.error, e:
            if e.errno == errno.ECONNREFUSED:
                pass

    def communicate(self):
        """ Make a connection to this server and read the response """
        return communicate(self._address)


def communicate(address):
    # error: [Errno 111] Connection refused - random port
    # error: [Errno 104] Connection reset by pier - dying server
    sock = socket.socket()
    sock.connect(address)

    response = sock.recv(1024)  # FIXME: indefinite block if _handle has 
                                # crashed; using communicate from main thread
                                # allows to CTRL+C this
    LOG.info("[Communicate] %s replied: %s", address, response)
    return response


class Server(SocketServer, threading.Thread):

    port0 = 27000
    servers = []

    def __init__(self):
        super(Server, self).__init__(self.port0 + len(self.servers))
        threading.Thread.__init__(self)  # bases.Task breaks super() chain

        self.servers.append(self)

    @classmethod
    def setup(cls):
        cls.teardown()
        cls.servers = []

    @classmethod
    def teardown(cls):
        map(lambda x: x.kill(), cls.servers)
        map(lambda x: x.join(), cls.servers)


def minimal_haproxy_conf(path="/etc/haproxy/haproxy.cfg"):
    contents = \
    """
defaults
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms

listen init
    bind *:26998

    """

    with open(path, 'w') as f:
        f.write(contents)



###@before.each_scenario
def setup(scenario):
    Server.setup()

    minimal_haproxy_conf()
    HAProxyInitScript.start()


###@after.each_scenario
def teardown(scenario):
    Server.teardown()

    HAProxyInitScript.stop()


###@step("i have a server")
def i_have_a_server(step):
    Server().start()


###@step("i add proxy")
def i_add_proxy(step):
    pass


###@step("i expect proxying")
def i_expect_proxying(step):
    pass

