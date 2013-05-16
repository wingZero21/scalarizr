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
logging.getLogger().setLevel(logging.DEBUG)


class IdDict(dict):
    """
    Allows adding without a key.
    Try to avoid using ints as keys.
    """

    def __init__(self, *args, **kwargs):
        super(IdDict, self).__init__(*args, **kwargs)

        self._id = 0

    def add(self, item):
        self._id += 1

        self[self._id] = item


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
            LOG.debug("[%s] Got connection from %s", self, address_str)
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


class Role(object):

    _id = 10
    _instances = {}

    def __init__(self, servers=None):
        cls = self.__class__
        cls._id += 1

        self.id = cls._id
        self.servers = servers or []

        cls._instances[self.id] = self

    @classmethod
    def setup(cls):
        cls._instances = {}

    @classmethod
    def get_servers(cls, role_id):
        #? should the ports be stripped?
        return map(str, cls._instances[role_id].servers)


haproxy_api.get_servers = Role.get_servers


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

    @classmethod
    def add_servers(cls, role_id, servers=None):
        servers = servers or []
        cls._role_servers[role_id].extend(servers)


#haproxy_api.get_servers = Roles.get_servers






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

    responses = map(str, world.servers.values())

    # role_ids = map(lambda role: role if isinstance(role, int) else \
    #                             role["id"],
    #                world.roles)
    # [responses.extend(Roles.get_servers(role_id)) for role_id in role_ids]

    LOG.info("Acceptable responses: %s", ', '.join(['"' + response + '"'
                                                    for response in responses]))
    return responses


@before.each_scenario
def setup(scenario):
    Server.setup()
    Role.setup()

    world.servers = IdDict()
    world.roles = IdDict()
    world.proxy_port = 27000
    world.acceptable_responses = []

    # for 'server goes down' scenario
    world.terminated = []

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


@step("i have a ?(\w*) server ?(\w*)")
def i_have_a_server(step, desc, name):
    # desc == ''
    # name == ''
    server = Server()
    server.start()
    world.servers.add(server)


@step("i have a ?(\w*) role ?(\w*)")
def i_have_a_role(step, desc, name):
    servers = [Server() for i in range(2)]
    [server.start() for server in servers]

    role = Role(servers)
    world.roles.add(role)

@step("i have a backup role")
def i_have_a_backup_role(step):
    role_id = make_role()
    world.roles.append({"id": role_id, "backup": True})


@step("i add proxy")
@dont_fail
def i_add_proxy(step):
    servers = map(str, world.servers.values())
    roles = map(str, world.roles.values())

    world.api.add_proxy(port=world.proxy_port,
                        servers=servers,
                        roles=roles)
    world.acceptable_responses = acceptable_responses()


@step("i have a proxy to a role")
def i_have_a_proxy_to_a_role(step):
    step.given("i have a role")
    step.given("i add proxy")


@step("i expect proxying")
def i_expect_proxying(step):
    for i in range(10):
        assert communicate(world.proxy_port) in world.acceptable_responses


@step("i launch new server of this role")
def i_launch_new_server_of_this_role(step):
    step.given("i have a server")
    server = world.servers[-1]

    role_id = world.roles[-1]
    Roles.add_servers(role_id, [server])

    #? how do we associate role with a backend?
    world.api.add_server(server, "tcp:27000")


@step("i terminate one server of this role")
def i_terminate_one_server_of_this_role(step):
    role_id = world.roles[-1]
    role_servers = Roles.get_servers(role_id)

    server = role_servers.pop()
    world.api.remove_server(server)

    world.terminated.append(server)
    world.acceptable_responses.remove(server)


@step("server appears in the backend")
def server_appears_in_the_backend(step):
    server = world.servers[-1]
    assert server.replace('.', '-') in world.api.cfg.backends["scalr:backend:tcp:27000"]['server']


@step("server is removed from the backend")
def server_is_removed_from_the_backend(step):
    server = world.terminated[-1]
    assert not server.replace('.', '-') in world.api.cfg.backends["scalr:backend:tcp:27000"]['server']


@step("i have a proxy to two roles: master and backup")
def i_have_a_proxy_to_two_roles(step):
    step.given("i have a role")
    step.given("i have a backup role")
    step.given("i add proxy")


@step("i terminate master servers")
def i_terminate_master_servers(step):
    pass

