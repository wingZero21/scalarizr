import os


from scalarizr import handlers, linux
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr
from scalarizr.messaging import Messages
from scalarizr.util import initdv2
from scalarizr.node import __node__


def get_handlers():
    return [TomcatHandler()]

class TomcatHandler(handlers.Handler, handlers.FarmSecurityMixin):

    def __init__(self):
        handlers.Handler.__init__(self)
        handlers.FarmSecurityMixin.__init__(self, [8080, 8443])
        bus.on(
            init=self.on_init, 
            start=self.on_start
        )
        self.service = initdv2.ParametrizedInitScript('tomcat', '/etc/init.d/tomcat')

    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )

    def on_start(self):
        if __node__['state'] == 'running':
            self.service.start()

    def accept(self, message, queue, behaviour=None, **kwds):
        return message.name in (
                Messages.HOST_INIT, 
                Messages.HOST_DOWN) and 'tomcat' in behaviour

    def on_host_init_response(self, hir_message):
        if not os.path.exists('/etc/init.d/tomcat'):
            pkgs = []
            if linux.os.debian_family:
                if (linux.os['name'] == 'Ubuntu' and linux.os['version'] >= (12, 4)) or \
                    (linux.os['name'] == 'Debian' and linux.os['version']  >= (7, 0)):
                    tomcat = 'tomcat7'
                else:
                    tomcat = 'tomcat6'
                pkgs = [tomcat, '{0}-admin'.format(tomcat)]
            elif linux.os.redhat_family or linux.os.oracle_family:
                tomcat = 'tomcat6'
                pkgs = [tomcat, '{0}-admin-webapps'.format(tomcat)]
            for pkg in pkgs:
                pkgmgr.installed(pkg)

    def on_before_host_up(self, message):
        self.service.start()

