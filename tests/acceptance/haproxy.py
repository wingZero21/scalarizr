import socket
import threading
import logging
import time
import errno
import sys

from lettuce import step, world, before, after
import mock

from scalarizr.libs.bases import Task
from scalarizr.api import haproxy as haproxy_api


haproxy_api.iptables = mock.MagicMock()


LOG = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


class SocketServer(Task):

    def __init__(self, port):
        super(SocketServer, self).__init__()

        self.address = ('127.0.0.1', port)

        self.on(start=lambda: LOG.info("[%s] Starting up", self))
        self.on(complete=lambda r: LOG.info("[%s] Went down", self))
        self.on(error=lambda exc_info: LOG.info("[%s] Crashed", self,
            exc_info=exc_info))

    def _run(self):
        self._killed = False

        sock = socket.socket()
        # allow restarting right after a crash
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.address)
        sock.listen(5)

        while not self._killed:
            self._handle(*sock.accept())

    def __str__(self):
        return ':'.join(map(str, self.address))

    def _handle(self, client_sock, address):
        """ The server sends back it's own address """
        address_str = ':'.join(map(str, address))

        if not self._killed:
            LOG.info("[%s] Got connection from %s", self, address_str)
            client_sock.send(str(self))
        else:
            # we expect client_sock to be the one created by self._kill()
            LOG.debug("[%s] Recieved kill connection from %s", self, 
                address_str)
            client_sock.send("debug: being killed")

    def _kill(self):
        self._killed = True
        # make sock.accept() return
        try:
            socket.socket().connect(self.address)
        except socket.error, e:
            if e.errno == errno.ECONNREFUSED:
                pass


class Server(SocketServer, threading.Thread):
    """
    SocketServer + Thread + server registration for test setup and teardown
    """

    _port = 27000
    _servers = []

    def __init__(self):
        cls = self.__class__

        cls._port += 1
        super(Server, self).__init__(cls._port)
        threading.Thread.__init__(self)  # bases.Task breaks super() chain

        cls._servers.append(self)

    @classmethod
    def setup(cls):
        cls.teardown()
        cls._servers = []

    @classmethod
    def teardown(cls):
        map(lambda server: server.kill(), cls._servers)
        for server in cls._servers:
            try:
                server.join()
            except RuntimeError:
                pass


def communicate(target):
    """
    Get response from a running :class:`Server` instance.

    :param target: port(int), netloc(str) or address for socket as tuple

    """

    if isinstance(target, int):
        address_str = "127.0.0.1:%s" % target
        address = ("127.0.0.1", target)
    elif isinstance(target, str):
        address_str = target
        ip, port = target.split(':')
        address = (ip, int(port))
    elif isinstance(target, tuple):
        address_str = ':'.join(map(str, self.address))
        address = target

    sock = socket.socket()
    try:
        sock.connect(address)
    except socket.error, e:
            if e.errno == errno.ECONNREFUSED:
                response = 'debug: connection refused'
            else:
                raise
    else:
        response = sock.recv(1024)  # FIXME: indefinite block if _handle has
                                    # crashed; using communicate from main 
                                    # thread allows to CTRL+C this
    LOG.info("[%s] %s replied: \"%s\"", "Communicate".ljust(15), address_str,
        response)
    return response


class Roles(object):

    _id = 10
    _role_servers = {}

    @classmethod
    def setup(cls):
        cls._role_servers = {}

    @classmethod
    def create(cls, servers=None):
        servers = servers or []

        cls._id += 1
        cls._role_servers[cls._id] = servers
        return cls._id

    @classmethod
    def get_servers(cls, role_id):
        return cls._role_servers[role_id]


haproxy_api.get_servers = Roles.get_servers






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


def dont_fail(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            # after.each_scenario doesn't execute for some reason if
            # we let this exception pop out
            LOG.info("STEP FAILED", exc_info=sys.exc_info())
    return wrapper











def acceptable_responses():
    # TODO: doc
    # TODO: ignore down servers

    responses = map(lambda server: server if isinstance(server, str) else \
                                   server["address"],
                    world.servers)

    role_ids = map(lambda role: role if isinstance(role, int) else \
                                role["id"],
                   world.roles)
    [responses.extend(Roles.get_servers(role_id)) for role_id in role_ids]

    LOG.info("Acceptable responses: %s", ', '.join(['"' + response + '"'
                                                    for response in responses]))
    return responses


@before.each_scenario
def setup(scenario):
    Server.setup()
    Roles.setup()

    world.servers = []
    world.roles = []
    world.proxy_port = 27000
    world.acceptable_responses = []

    minimal_haproxy_conf()
    world.api = haproxy_api.HAProxyAPI()
    world.api.svc.start()
    LOG.info("Started haproxy service")


@after.each_scenario
def teardown(scenario):
    Server.teardown()

    world.api.svc.stop()
    LOG.info("Stopped haproxy service")

    LOG.info("-" * 17)


@step("i have a server")
def i_have_a_server(step):
    server = Server()
    server.start()
    world.servers.append(str(server))


@step("i have a role")
def i_have_a_role(step):
    servers = [Server() for i in range(2)]
    [server.start() for server in servers]
    role = Roles.create(map(str, servers))
    world.roles.append(role)


@step("i add proxy")
@dont_fail
def i_add_proxy(step):
    world.api.add_proxy(port=world.proxy_port,
                        servers=world.servers,
                        roles=world.roles)
    world.acceptable_responses = acceptable_responses()


@step("i expect proxying")
def i_expect_proxying(step):
    assert communicate(world.proxy_port) in world.acceptable_responses

