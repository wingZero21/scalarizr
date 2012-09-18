# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING

from collections import OrderedDict
from copy import copy
import re
from contextlib import contextmanager

from scalarizr import linux


AUTO_PERSISTENCE = False
IPTABLES_BIN = '/sbin/iptables'
IPTABLES_CONF = '/home/vladimir/test2/dump'


class Chains(object):
	#? singleton
	# FIXME: use long args
	# replace /sbin/iptables with IPTABLES_EXEC 

	_container = {}

	def __getitem__(self, name):
		return self._container[name]

	def add(self, name):
		print linux.build_cmd_args(executable=IPTABLES_BIN, short=['-N', name])
		self._container[name] = Chain(name)

	def remove(self, name, force=False):
		if force:
			print linux.build_cmd_args(executable=IPTABLES_BIN, short=['-F', name])
			#? delete references
		print linux.build_cmd_args(executable=IPTABLES_BIN, short=['-X', name])
		del self._container[name]


class Chain(object):
	# FIXME: use long args

	def __init__(self, chain):
		self.name = chain

	def _execute(self, short, long):
		return linux.build_cmd_args(executable=IPTABLES_BIN, short=short, long=long)

	def _rule_to_kwargs(self, rule):
		_rule = copy(rule)

		kwargs = OrderedDict()
		for key in ("protocol", "match"):
			if key in _rule:
				kwargs[key] = _rule.pop(key)
		kwargs.update(_rule)

		return kwargs

	def append(self, rule, persistent=False):
		short = ['-A', self.name]
		long = self._rule_to_kwargs(rule)
		print self._execute(short, long)

		if AUTO_PERSISTENCE or persistent:
			Persistent.append(short, long)

	def insert(self, index, rule, persistent=False):
		short = ['-I', self.name]
		if index:
			short.append(index)
		long = self._rule_to_kwargs(rule)
		print self._execute(short, long)

		if AUTO_PERSISTENCE or persistent:
			Persistent.insert(short, long)

	def replace(self, index, rule, persistent=False):
		short = ['-R', self.name, index]
		long = self._rule_to_kwargs(rule)
		print self._execute(short, long)

		if AUTO_PERSISTENCE or persistent:
			Persistent.replace(short, long)

	def remove(self, arg):
		if isinstance(arg, int):
			short = ['-D', self.name, arg]
			long = {}
		elif isinstance(arg, dict):
			short = ['-D', self.name]
			long = self._rule_to_kwargs(arg)
		print self._execute(short, long)

	def list(self, numeric=False, table=None):
		short = ['-L', self.name]
		if numeric:
			short.append('-n')
		if table:
			short.extend(['-t', table])
		print self._execute(short, long={})


def splitlist(lst, sep):
	"""
	splitlist(["word1", "word2", "and", "word3", "and", "word4"], "and")
	->
	[
		["word1", "word2"],
		["word3"],
		["word4"],
	]
	"""
	result = [[]]
	for element in lst:
		if element == sep:
			result.append([])
		else:
			result[-1].append(element)
	return result


def joinlists(lst, sep):
	"""
	splitlist reverse
	"""
	result = []
	for l in lst:
		result.extend(l)
		result.append(sep)
	del result[-1]
	return result


class Persistent(object):

	# TODO: persistent rule for non-persistent chain

	@staticmethod
	@contextmanager
	def _modify_config(table_name="filter"):
		"""
		Yields list of strings from config's table section, excluding 'COMMIT'.
		"""
		## parse
		with open(IPTABLES_CONF) as fd:
			datalist = fd.read().splitlines()

		# delete comments and empty/whitespace lines
		datalist = filter(lambda x: not re.match(r'^(#|\s*$)', x), datalist)

		# split datalist into list of tables + []
		datalist = splitlist(datalist, 'COMMIT')

		del datalist[-1]

		## yield
		for table in datalist:
			if table[0][1:] == table_name:
				yield table
				break
		else:
			raise Exception("No such table")  # TODO: handle

		## dump
		datalist.append([])

		datalist = joinlists(datalist, 'COMMIT')

		datastring = '\n'.join(datalist) + '\n'

		with open(IPTABLES_CONF, 'w') as fd:
			fd.write(datastring)

	@staticmethod
	def append():
		with Persistent._modify_config() as data:
			print data

	@staticmethod
	def insert():
		pass

	@staticmethod
	def replace():
		pass



Persistent.append()


raise Exception("OK")

chains = Chains()


Chain('TEST').append({
	'protocol': 'tcp',
	'syn': True,
	'dport': 23,
	'match': 'connlimit',
	'connlimit_above': 2,
	'jump': 'REJECT'
})


Chain('TEST').insert(None, {
	'table': 'nat',
	'protocol': 'tcp',
	'dport': 80,
	'match': 'cpu',
	'cpu': 0,
	'jump': 'REDIRECT',
	'to_port': 8080
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
