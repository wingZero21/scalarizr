__author__ = 'vladimir'

import mock

from scalarizr.linux import iptables


IPTABLES_LINUX = iptables.linux


class TestIptables(object):

	def setup(self):
		iptables.linux = mock.MagicMock()

	def teardown(self):
		iptables.linux = IPTABLES_LINUX

	def test_iptables(self):
		kwargs = {
			'append': 'INPUT',
			'protocol': 'tcp',
			'table': 'filter',
			'dport': 80,
		}
		iptables.iptables(**kwargs)

		iptables.linux.build_cmd_args.assert_called_once_with(executable='/sbin/iptables', long=kwargs)
		# TODO: linux.system

	def test_iptables_save(self):
		iptables.iptables_save()

		iptables.linux.system.assert_called_once_with(['iptables-save'])


