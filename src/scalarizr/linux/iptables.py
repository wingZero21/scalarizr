# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING

from scalarizr import linux

IPTABLES_EXEC = '/sbin/iptables'

class Chains(object):
	#? singleton
	# FIXME: use long args
	# replace /sbin/iptables with IPTABLES_EXEC 

	container = {}

	def __getitem__(self, name):
		try:
			return self.container[name]
		except KeyError:
			self.container[name] = Chain(name)
			return self.container[name]

	def add(self, name):
		print linux.build_cmd_args(executable='/sbin/iptables', short=['-N', name])

	def remove(self, name, force=False):
		if force:
			print linux.build_cmd_args(executable='/sbin/iptables', short=['-X', name])
			#? delete references
		print linux.build_cmd_args(executable='/sbin/iptables', short=['-X', name])
		# del container[name]


class Chain(object):
	# FIXME: use long args

	def __init__(self, chain):
		self.name = chain

	def append(self, rule, persistent=False):
		short = ['-A', self.name]
		long = {}
		for key, val in rule.items():
			if key in ('protocol', 'match'):
				short.extend(['--' + key, val])
			else:
				long[key] = val
		print linux.build_cmd_args(executable='/sbin/iptables', short=short, long=long)

	def insert(self, index, rule, persistent=False):
		pass

	def replace(self, index, rule, persistent=False):
		pass

	def remove(self, arg):
		if isinstance(arg, int):
			self._remove_by_index(arg)
		elif isinstance(arg, dict):
			self._remove_by_rule(arg)

	def _remove_by_index(self, index):
		pass

	def _remove_by_rule(self, rule):
		pass


chains = Chains()


chains['TEST'].append({
	'protocol': 'tcp',
	'syn': True,
	'dport': 23,
	'match': 'connlimit',
	'connlimit_above': 2,
	'jump': 'REJECT'
})



raise Exception("OK")
#################################################################################
INPUT = chains['INPUT']

iptables.INPUT.append([
	{'protocol': 'tcp', 'dport': 3306, 'jump': 'ACCEPT'}
])
iptables.chains['RH-Input-1'].append(
	{'protocol': 'udp', 'dport': 8014, 'jump': 'ACCEPT'}
)

# allow 2 telnet connections per client host
iptables.INPUT.append({
	'protocol': 'tcp',
	'syn': True,
	'dport': 23,
	'match': 'connlimit',
	'connlimit_above': 2,
	'jump': 'REJECT'
})

#iptables -A PREROUTING -t mangle -i eth1 -m cluster --cluster-total-nodes 2 --cluster-local-node 1 --cluster-hash-seed 0xdeadbeef -j
#              MARK --set-mark 0xffff
iptables.PREROUTING.append({
	'table': 'mangle',
	'in_interface': 'eth1',
	'match': 'cluster',
	'cluster_total_nodes': 2,
	'cluster_local_node': 1,
	'cluster_hash_seed': '0xdeadbeef',
	'jump': 'MARK',
	'set_mask': '0xffff'
})

'''
# negative match [!]
# iptables -A INPUT -p tcp --syn --dport 23 -m connlimit ! --connlimit-above 2 -j ACCEPT
iptables.INPUT.append({
	'protocol': 'tcp',
	'syn': True,
	'dport': 23,
	'match': 'connlimit',
	'!connlimit_above': 2,
	'jump': 'ACCEPT'
})
'''

# insert rule at the head
iptables.PREROUTING.insert(None, {
	'table': 'nat',
	'protocol': 'tcp',
	'dport': 80,
	'match': 'cpu',
	'cpu': 0,
	'jump': 'REDIRECT',
	'to_port': 8080
})

# delete by rule num
iptables.INPUT.remove(1)

# delete by rulespec
iptables.INPUT.remove({'protocol': 'tcp', 'dport': 8013, 'jump': 'ACCEPT'})

# Replace command
iptables.INPUT.replace(2, rulespec)

# List INPUT rules:
iptables.INPUT.list(numeric=True)
# Another way
iptables.list('INPUT', table='nat', numeric=True)

# List all chains with rules
'''
iptables.list_all()
'''

# Add new chain
iptables.chains.add('RH-Input-2')

# Delete user-defined chain
iptables.chains.remove('RH-Input-2')
# Delete non-empty user-defined chain
iptables.chains.remove('RH-Input-2', force=True)


# There is a way to create persistent rules
# On RHEL they will be stored in /etc/sysconfig/iptables
# On Ubuntu in iptables.rules
iptables.INPUT.insert(1, rulespec, persistent=True)
iptables.INPUT.replace(2, rulespec, persistent=True)

# You can enable auto persistence. by default it's False
iptables.auto_persistence = True


# wrappers over binaries
def iptables(**long_kwds):
	pass

def iptables_save(filename=None, *short_args, **long_kwds):
	# file name is a path string or file-like object
	# if filename is None return output
	pass

def iptables_restore(filename, *short_args, **long_kwds):
	pass

'''
# TODO: State function
def ensure(
	
)
'''
