from __future__ import with_statement
'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr import exceptions
from scalarizr.libs import validate
from scalarizr.services import haproxy
from scalarizr.linux import iptables
from scalarizr import rpc

import logging
LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
    'timeout': {'check': '3s'},
    'default-server': {'inter': '30s', 'fall': 2, 'rise': 10}
}

_rule_protocol = validate.rule(choises=['tcp', 'http', 'TCP', 'HTTP'])
_rule_backend = validate.rule(re=r'^role:\d+$')
_rule_hc_target = validate.rule(re='^[tcp|http]+:\d+$')


# for testing; TODO: import
def get_servers(*args):
    return []


class HAProxyAPI(object):
    """
    Placeholder
    """

    def __init__(self, path=None):
        self.path_cfg = path
        self.cfg = haproxy.HAProxyCfg(path)
        self.svc = haproxy.HAProxyInitScript(path)

    def _server_name(self, ipaddr):
        '''@rtype: str'''
        # if ':' in ipaddr:
        #    ipaddr = ipaddr.strip().split(':')[0]
        return ipaddr.replace('.', '-')


    def add_proxy(self, port, backend_port=None, roles=None, servers=None,
                check_timeout=None, maxconn=None, **default_server_params):
        """
        Add proxy yo.

        :param port: listener port
        :type port: int
        :param backend_port: port for backend server to listen on?
        :type backend_port: int
        :param roles: role ids (ints) or dicts with "id" key
        :type roles: list
        :param servers: server ips
        :type servers: list
        :param check_timeout: ``timeout check`` - additional read timeout,
                              e.g. "3s"
        :type check_timeout: str
        :param maxconn: set ``maxconn`` of the frontend
        :type maxconn: str
        :param **default_server_params: following kwargs will be applied to
                                        the ``default-server`` key of the
                                        backend
        :param check_interval: value for ``inter``, e.g. "3s"
        :type check_interval: str
        :param fall_threshold: value for ``fall``
        :type fall_threshold: int
        :param rise_threshold: value for ``rise``
        :type rise_threshold: int
        :param server_maxconn: value for ``maxconn``, not to confuse with
                               the frontend's ``maxconn``
        :type server_maxconn: str
        :param down: value for #?
        :type down: bool
        :param backup: value for ``backup``
        :type backup: bool

        :returns: ?

        .. note:: official documentation on the global parameters and server \
        options can be found at \
        http://cbonte.github.com/haproxy-dconv/configuration-1.4.html

        """
        # TODO: handle address-host-port mess
        # -> {'check': True, 'port': '27001', 'address': '127.0.0.1'}

        # default values
        if not backend_port:
            backend_port = port
        if not roles:
            roles = []
        if not servers:
            servers = []

        # translate param names to config param names
        server_param_names_map = {
            "check_interval": "inter",
            "fall_threshold": "fall",
            "rise_threshold": "rise",
            "server_maxconn": "maxconn",
        }
        def rename(params):
            return dict([
                (server_param_names_map.setdefault(key, key), val)
                    for key, val in params.items()
            ])

        # allowing short servers & roles specification
        # creating new lists here also protects from side effects
        roles = map(lambda x: {"id": x} if isinstance(x, int) else dict(x), roles)
        servers = map(lambda x: {"address": x} if isinstance(x, str) else dict(x), servers)

        #
        listener_name = haproxy.naming('listen', "tcp", port)
        backend_name = haproxy.naming('backend', "tcp", port)

        #? check for duplicate listener?

        listener = {
            'mode': "tcp",
            'balance': 'roundrobin',
            'bind': '*:%s' % port,
            'default_backend': backend_name,
        }
        if maxconn:
            listener["maxconn"] = maxconn

        backend = {
            "mode": "tcp",
            "server": {},
        }
        backend.update(HEALTHCHECK_DEFAULTS)
        if check_timeout:
            backend["timeout"]["check"] = check_timeout
        backend["default-server"].update(rename(default_server_params))
        backend["default-server"]["port"] = backend_port

        # roles to server ips & their params
        roles_servers = []
        for role in roles:
            role_id, role_params = role.pop("id"), role

            # get_servers(role_id) -> [ip] ?
            role_servers = map(lambda ip: {"address": ip}, get_servers(role_id))
            [server.update(role_params) for server in role_servers]

            roles_servers.extend(role_servers)

        # get all servers together & enable healthchecks
        servers.extend(roles_servers)
        [server.setdefault("check", True) for server in servers]

        # update the backend
        for server in servers:
            backend['server'][server["address"].replace('.', '-')] = rename(server)

        # save to cfg
        self.cfg['listen'][listener_name] = listener
        # backends?
        if not self.cfg.backend or not backend_name in self.cfg.backend:
            self.cfg['backend'][backend_name] = backend

        try:
            iptables.FIREWALL.ensure(
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port}
            )
        except Exception, e:
            raise exceptions.Duplicate(e)

        self.cfg.save()
        self.svc.reload()


    @rpc.service_method
    #@validate.param('ipaddr', type='ipv4')
    @validate.param('backend', optional=_rule_backend)
    def add_server(self, server=None, backend=None):
        '''Add server with ipaddr in backend section'''
        self.cfg.reload()

        if backend:
            backend = backend.strip()

        LOG.debug('HAProxyAPI.add_server')
        LOG.debug('     %s' % haproxy.naming('backend', backend=backend))


        bnds = self.cfg.sections(haproxy.naming('backend', backend=backend))
        if not bnds:
            if backend:
                raise exceptions.NotFound('Backend not found: %s' % (backend, ))
            else:
                raise exceptions.Empty('No listeners to add server to')

        #with self.svc.trans(exit='running'):
            #with self.cfg.trans(exit='working'):

        server.setdefault("check", True)
        server_name = ':'.join([server["address"], str(server["port"])]).replace('.', '-')
        
        for bnd in bnds:
            self.cfg.backends[bnd]['server'][server_name] = server

        self.cfg.save()
        self.svc.reload()


    @rpc.service_method
    @validate.param('ipaddr', type='ipv4')
    @validate.param('backend', optional=_rule_backend)
    def remove_server(self, ipaddr, backend=None):
        '''Remove server from backend section with ipaddr'''
        if ipaddr: 
            ipaddr = ipaddr.strip()
        if backend: 
            backend = backend.strip()

        srv_name = self._server_name(ipaddr)
        for bd in self.cfg.sections(haproxy.naming('backend', backend=backend)):
            if ipaddr and srv_name in self.cfg.backends[bd]['server']:
                del self.cfg.backends[bd]['server'][srv_name]

        self.cfg.save()
        self.svc.reload()


    def health():
        try:
            if self.cfg.defaults['stats'][''] == 'enable' and \
                    self.cfg.globals['stats']['socket'] == '/var/run/haproxy-stats.sock':
                pass
        except:
            self.cfg.globals['stats']['socket'] = '/var/run/haproxy-stats.sock'
            self.cfg.defaults['stats'][''] = 'enable'
            self.cfg.save()
            self.svc.reload()

        stats = haproxy.StatSocket().show_stat()

        print stats


    @rpc.service_method
    @validate.param('port', 'server_port', type=int)
    @validate.param('protocol', required=_rule_protocol)
    @validate.param('server_port', optional=True, type=int)
    @validate.param('backend', optional=_rule_backend)
    def create_listener(self, port=None, protocol=None, server_port=None,
                                    server_protocol=None, backend=None):
        ''' '''
        LOG.debug('create_listener: %s, %s, %s, %s, %s, %s', self, port, protocol, server_port, server_protocol, backend)
        if protocol:
            protocol = protocol.lower()
        ln = haproxy.naming('listen', protocol, port)
        bnd = haproxy.naming('backend', server_protocol or protocol, server_port or port, backend=backend)
        return ln, bnd
        listener = backend = None
        LOG.debug('HAProxyAPI.create_listener: listener = `%s`, backend = `%s`', ln, bnd)

        try:
            if self.cfg.listener[ln]:
                raise 'Duplicate'
        except Exception, e:
            if 'Duplicate' in e:
                raise exceptions.Duplicate('Listener %s:%s already exists' % (protocol, port))
        if protocol == 'tcp':
            listener = {'balance': 'roundrobin'}
        elif protocol == 'http':
            listener = {'option': {'forwardfor': True}}
        else:
            raise ValueError('Unexpected protocol: %s' % (protocol, ))
            #TODO: not correct for https or ssl...

        # listen config:
        listener.update({
                'bind': '*:%s' % port,
                'mode': protocol,
                'default_backend': bnd
        })

        backend_protocol = server_protocol or protocol
        if backend_protocol == 'tcp':
            backend = {}
        elif backend_protocol == 'http':
            backend = {'option': {'httpchk': True}}
        else:
            raise ValueError('Unexpected protocol: %s' % (backend_protocol, ))
            #TODO: not correct for https or ssl...

        # backend config:
        backend.update({'mode': backend_protocol})
        backend.update(HEALTHCHECK_DEFAULTS)

        # apply changes
        #with self.svc.trans(exit='running'):
        #       with self.cfg.trans(enter='reload', exit='working'):
        #TODO: change save() and reload(),`if True` condition to `with...` enter, exit
        if True:
            self.cfg['listen'][ln] = listener
            if not self.cfg.backend or not bnd in self.cfg.backend:
                self.cfg['backend'][bnd] = backend
            try:
                iptables.FIREWALL.ensure(
                        {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port}
                )
            except Exception, e:
                raise exceptions.Duplicate(e)

            self.cfg.save()
            self.svc.reload()

            return listener


    @rpc.service_method
    @validate.param('unhealthy_threshold', 'healthy_threshold', type=int)
    @validate.param('target', optional=_rule_hc_target)
    @validate.param('interval', 'timeout', re=r'(^\d+[smhd]$)|^\d')
    def configure_healthcheck(self, target=None, interval=None, timeout=None,
                                                    unhealthy_threshold=None, healthy_threshold=None):
        ''' '''
        try:
            if interval == 'None': interval=None
            int(interval)
            interval = '%ss' % interval
        except:
            pass
        try:
            if timeout == 'None': timeout=None
            int(timeout)
            timeout = '%ss' % timeout
        except:
            pass

        bnds = haproxy.naming('backend', backend=target)
        if not self.cfg.sections(bnds):
            raise exceptions.NotFound('Backend `%s` not found' % bnds)

        for bnd in self.cfg.sections(bnds):
            if timeout:
                if isinstance(timeout, dict):
                    self.cfg['backend'][bnd]['timeout'] = timeout
                else:
                    self.cfg['backend'][bnd]['timeout'] = {'check': str(timeout)}
            default_server = {
                    'inter': interval,
                    'fall': unhealthy_threshold,
                    'rise': healthy_threshold
            }
            self.cfg['backend'][bnd]['default-server'] = default_server
            for srv in self.cfg['backend'][bnd]['server']:
                server = self.cfg['backend'][bnd]['server'][srv]
                server.update({'check' : True})
                self.cfg['backend'][bnd]['server'][srv] = server
        #with self.svc.trans(exit='running'):
            #       with self.cfg.trans(enter='reload', exit='working'):
        self.cfg.save()
        self.svc.reload()


    


    @rpc.service_method
    @validate.param('ipaddr', type='ipv4', optional=True)
    def get_servers_health(self, ipaddr=None):
        try:
            if self.cfg.defaults['stats'][''] == 'enable' and \
                            self.cfg.globals['stats']['socket'] == '/var/run/haproxy-stats.sock':
                pass
        except:
            self.cfg.globals['stats']['socket'] = '/var/run/haproxy-stats.sock'
            self.cfg.defaults['stats'][''] = 'enable'
            self.cfg.save()
            self.svc.reload()

        #TODO: select parameters what we need with filter by ipaddr
        stats = haproxy.StatSocket().show_stat()
        return stats


    @rpc.service_method
    @validate.param('port', type=int)
    @validate.param('protocol', required=_rule_protocol)
    def delete_listener(self, port=None, protocol=None):
        ''' Delete listen section(s) by port (and)or protocol '''

        ln = haproxy.naming('listen', protocol, port)
        if not self.cfg.sections(ln):
            raise exceptions.NotFound('Listen `%s` not found can`t remove it' % ln)
        try:
            default_backend = self.cfg.listener[ln]['default_backend']
        except:
            default_backend = None

        for path in self.cfg.sections(ln):
            del self.cfg['listen'][ln]
            LOG.debug('HAProxyAPI.delete_listener: removed listener `%s`' % ln)

        if default_backend:
            has_ref = False
            for ln in self.cfg.listener:
                try:
                    if self.cfg.listener[ln]['default_backend'] == default_backend:
                        has_ref = True
                        break
                except:
                    pass
            if not has_ref:
                #it not used in other section, so will be deleting
                del self.cfg.backends[default_backend]

        try:
            iptables.FIREWALL.remove({
                    "jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port
            })
        except Exception, e:
            raise exceptions.NotFound(e)

        self.cfg.save()
        self.svc.reload()


    @rpc.service_method
    @validate.param('target', required=_rule_hc_target)
    def reset_healthcheck(self, target):
        '''Return to defaults for `tartget` backend sections'''
        target = target.strip()
        bnds = haproxy.naming('backend', backend=target)
        if not self.cfg.sections(bnds):
            raise exceptions.NotFound('Backend `%s` not found' % target)
        for bnd in self.cfg.sections(bnds):
            backend = self.cfg['backend'][bnd]
            backend.update(HEALTHCHECK_DEFAULTS)
            self.cfg['backend'][bnd] = backend

        #with self.svc.trans(exit='running'):
            #       with self.cfg.trans(enter='reload', exit='working'):
            #TODO: with...
        self.cfg.save()
        self.svc.reload()


    @rpc.service_method
    def list_listeners(self):
        '''
        @return: Listeners list
        @rtype: [{
                <port>,
                <protocol>,
                <server_port>,
                <server_protocol>,
                <backend>,
                <servers>: [<ipaddr>, ...]
        }, ...]'''
        self.cfg.reload()
        res = []
        for ln in self.cfg.sections(haproxy.naming('listen')):
            listener = self.cfg.listener[ln]
            bnd_name = listener['default_backend']
            bnd_role = ':'.join(bnd_name.split(':')[2:4]) #example`role:1234`
            bnd = self.cfg.backends[bnd_name]

            res.append({
                            'port': listener['bind'].replace('*:',''),
                            'protocol': listener['mode'],
                            'server_port': bnd_name.split(':')[-1],
                            'server_protocol': bnd['mode'],
                            'backend': bnd_role,
                    })
        return res


    @rpc.service_method
    @validate.param('backend', optional=_rule_backend)
    def list_servers(self, backend=None):
        '''
        List all servers, or servers from particular backend
        @rtype: [<ipaddr>, ...]
        '''
        if backend: backend = backend.strip()
        list_section = self.cfg.sections(haproxy.naming('backend', backend=backend))

        res = []
        for bnd in list_section:
            for srv_name in self.cfg.backends[bnd]['server']:
                res.append(self.cfg.backends[bnd]['server'][srv_name]['address'])
        res = list(set(res))
        return res
