import socket
import threading
import logging
import time

from lettuce import step, world, before, after

from scalarizr.libs.bases import Task


LOG = logging.getLogger(__name__)


class Server(Task):

    def __init__(self, port):
        super(Server, self).__init__()

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
        LOG.info("[%s] Got connection from %s", self, address)
        client_sock.send(str(self))

    def _kill(self):
        self._killed = True


def communicate(port):
    address = ('', port)

    sock = socket.socket()
    sock.connect(address)

    response = sock.recv(1024)  # FIXME: indefinite block if the server has 
                                # crashed; using communicate from main thread
                                # allows to CTRL+C this
    LOG.info("[Communicate] %s replied: %s", address, response)
    return response


@step("i have a server")
def i_have_a_server(step):
    
    server = Server(27015)

    thread = threading.Thread(target=server.run)
    thread.start()

    time.sleep(1)
    server.kill()
    communicate(27015)



@step("i add proxy")
def i_add_proxy(step):
    pass


@step("i expect proxying")
def i_expect_proxying(step):
    pass

