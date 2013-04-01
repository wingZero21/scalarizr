
import logging


from scalarizr import handlers, linux
from scalarizr.node import __node__
from scalarizr.linux import pkgmgr, iptables
from scalarizr.bus import bus


LOG = logging.getLogger(__name__)


def get_handlers():
	return [NatHandler()]


class NatHandler(handlers.Handler):

	def __init__(self):
		self._data = None
		bus.on(init=self.on_init, start=self.on_start)

	def on_init(self):
		bus.on(
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up
		)

	def on_host_init_response(self, hir):
		"""
		Accept HostInitResponse configuration

		.. code-block:: xml

			<vcp>
				<cidr>10.0.0.0/16</cidr>
				<subnets>
					<item>10.0.0.0/24</item>
				</subnets>
			<vpc>

		"""
		if not hir.body.get('vpc'):
			msg = "HostInitResponse message for VPC behavior " \
					"must have 'vpc' property"
			raise HandlerError(msg)

		self._data = hir.vpc

	def on_start(self):
		if __node__['state'] == 'running':
			self._configure()

	def on_before_host_up(self, hostup):
		self._configure()

	def _configure(self):
		pkgmgr.installed('augeas-tools')
		linux.system2('augtool -s set /files/etc/sysctl.conf/net.ipv4.ip_forward 1', shell=True)
		linux.system2('sysctl -p', shell=True)

		rules = []
		for subnet_cidr in self._data['subnets']:
			rules.append({
				'table': 'nat', 
				'protocol': 'all', 
				'source': subnet_cidr, 
				'!destination': self._data['cidr'],  # TODO: implement [!] in iptables module
				'jump': 'MASQUERADE'})
		iptables.ensure({'POSTROUTING': rules})

		# git clone cookbooks

		# chef-solo
