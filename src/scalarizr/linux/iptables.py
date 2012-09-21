# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING

from collections import OrderedDict

from scalarizr import linux


IPTABLES_BIN = '/sbin/iptables'


def iptables(**long_kwds):

	ordered_long = OrderedDict()
	for key in ("protocol", "match"):
		if key in long_kwds:
			ordered_long[key] = long_kwds.pop(key)
	ordered_long.update(long_kwds)

	return linux.build_cmd_args(executable=IPTABLES_BIN, long=ordered_long)


def iptables_save(filename=None, *short_args, **long_kwds):
	# file name is a path string or file-like object
	# if filename is None return output
	kwds = {}
	if isinstance(filename, basestring):
		filename = open(filename, 'w+')
	if hasattr(filename, 'write'):
		kwds['stdout'] = filename
	out = linux.system(linux.build_cmd_args(executable='iptables-save',
		short=short_args, long=long_kwds), **kwds)[0]
	return out


def iptables_restore(filename, *short_args, **long_kwds):
	if isinstance(filename, basestring):
		filename = open(filename)
	linux.system(linux.build_cmd_args(executable='iptables-restore',
		short=short_args, long=long_kwds), stdin=filename)


def save():
	'''
	on RHEL call 'service iptables save'
	on Ubuntu:
		- touch or create /etc/network/if-pre-up.d/iptables.sh
			$ cat /etc/network/if-pre-up.d/iptables.sh
			#!/bin/bash
			iptables-restore < /etc/iptables.rules
		- iptables-save > /etc/iptables.rules
	'''
	if linux.os["family"] in ("RedHat", "Oracle"):
		linux.system(linux.build_cmd_args(executable="service", short=['iptables',
																	   'save']))
	elif linux.os["family"] == "Debian":
		with open('/etc/network/if-pre-up.d/iptables.sh', 'w') as fp:
			fp.write('#!/bin/bash\n'
					 'iptables-restore < /etc/iptables.rules')

		iptables_save('/etc/iptables.rules')


class _Chain(object):

	def __init__(self, chain):
		self.name = chain

	def append(self, rule):
		return iptables(append=self.name, **rule)

	def insert(self, index, rule):
		if index:
			insert = (self.name, index)
		else:
			insert = self.name
		return iptables(insert=insert, **rule)

	def replace(self, index, rule):
		return iptables(replace=(self.name, index), **rule)

	def remove(self, arg):
		if isinstance(arg, int):
			delete = (self.name, arg)
			rule = {}
		elif isinstance(arg, dict):
			delete = self.name
			rule = arg
		return iptables(delete=delete, **rule)

	def list(self, table=None):
		kwargs = {}
		if table:
			kwargs = {'table': table}
		out = iptables(list=self.name, **kwargs)[0]
		# 'Chain INPUT (policy ACCEPT)\ntarget     prot opt source               destination         \n\nChain FORWARD (policy ACCEPT)\ntarget     prot opt source               destination         \n\nChain OUTPUT (policy ACCEPT)\ntarget     prot opt source               destination         \n'
		#TODO:parse
		out.split('\n\n')


class _Chains(object):

	_predefined = (
		"INPUT",
		"FORWARD",
		"OUTPUT",
		"PREROUTING",
		"POSTROUTING",
	)
	_container = dict([(name, _Chain(name)) for name in _predefined])

	@classmethod
	def __getitem__(cls, name):
		return cls._container[name]

	@classmethod
	def add(cls, name):
		assert name not in cls._predefined
		cls._container.setdefault(name, _Chain(name))

		# create
		try:
			iptables(**{"new-chain": name})
		except linux.LinuxError, e:
			if not e.err == "iptables: Chain already exists.":
				raise

	@classmethod
	def remove(cls, name, force=False):
		assert name not in cls._predefined

		if force:
			iptables(flush=name)
			# TODO: delete references
		iptables(**{"delete-chain": name})

		cls._container.pop(name)


chains = _Chains()

INPUT = chains["INPUT"]
FORWARD = chains["FORWARD"]
OUTPUT = chains["OUTPUT"]
PREROUTING = chains["PREROUTING"]
POSTROUTING = chains["POSTROUTING"]


def list(chain, table=None):
	return chains[chain].list(table)




if __name__ == "__main__":

	print POSTROUTING.insert(8, {
		'table': 'nat',
		'protocol': 'tcp',
		'dport': 80,
		'match': 'cpu',
		'cpu': 0,
		'jump': 'REDIRECT',
		'to_port': 8080
	})



"""
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
"""