# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING

from __future__ import with_statement

import sys
if sys.version_info[0:2] >= (2, 7):
	from collections import OrderedDict
else:
	from scalarizr.externals.collections import OrderedDict
import shlex
import os
import re
from copy import copy

from scalarizr import linux
from scalarizr.linux import coreutils
from scalarizr.linux import redhat

import logging
LOG = logging.getLogger(__name__)


IPTABLES_BIN = '/sbin/iptables'
IPTABLES_SAVE = '/sbin/iptables-save'
IPTABLES_RESTORE = '/sbin/iptables-restore'

# from iptables --help, must cover all short options
_OPTIONS = {
	"-A": "--append",
	"-D": "--delete",
	"-I": "--insert",
	"-R": "--replace",
	"-L": "--list",
	"-S": "--list-rules",
	"-F": "--flush",
	"-Z": "--zero",
	"-N": "--new",
	"-X": "--delete-chain",
	"-P": "--policy",
	"-E": "--rename-chain",
	"-p": "--protocol",
	"-s": "--source",
	"-d": "--destination",
	"-i": "--in-interface",
	"-j": "--jump",
	"-g": "--goto",
	"-m": "--match",
	"-n": "--numeric",
	"-o": "--out-interface",
	"-t": "--table",
	"-v": "--verbose",
	"-x": "--exact",
	"-f": "--fragment",
	"-V": "--version",
}

coreutils.modprobe('ip_tables')


def iptables(**long_kwds):

	ordered_long = OrderedDict()
	for key in ("protocol", "match"):
		if key in long_kwds:
			ordered_long[key] = long_kwds.pop(key)
	ordered_long.update(long_kwds)

	return linux.system(linux.build_cmd_args(executable=IPTABLES_BIN,
		long=ordered_long))


def iptables_save(filename=None, *short_args, **long_kwds):
	# file name is a path string or file-like object
	# if filename is None return output
	kwds = {}
	if isinstance(filename, basestring):
		filename = open(filename, 'w+')
	if hasattr(filename, 'write'):
		kwds['stdout'] = filename
	out = linux.system(linux.build_cmd_args(executable=IPTABLES_SAVE,
		short=short_args, long=long_kwds), **kwds)[0]
	return out


def iptables_restore(filename, *short_args, **long_kwds):
	if isinstance(filename, basestring):
		filename = open(filename)
	linux.system(linux.build_cmd_args(executable=IPTABLES_RESTORE,
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
		if isinstance(index, int):
			insert = [self.name, index]
		else:
			insert = self.name
		return iptables(insert=insert, **rule)

	def replace(self, index, rule):
		return iptables(replace=[self.name, index], **rule)

	def remove(self, arg):
		if isinstance(arg, int):
			delete = [self.name, arg]
			rule = {}
		elif isinstance(arg, dict):
			delete = self.name
			rule = arg
		return iptables(delete=delete, **rule)

	def list(self, table=None):
		result = []

		def build_ruledict(option, element):
			"""
			For use in reduce(build_ruledict, arglist). This will result in
			filling the last dict from 'result' with {'option': 'value'} pairs
			from ['--option', 'value', ...] arglist. Multiple or no values per
			option is acceptable.
			"""

			ruledict = result[-1]
			key = option[2:]
			val = ruledict.setdefault(key, True)

			if element.startswith('--'):  # element is the new option
				return element
			else:
				if val == True:
					ruledict[key] = element
				elif not hasattr(val, 'append'):
					ruledict[key] = [val, element]
				else:
					ruledict[key].append(element)
				return option

		# get stdout
		kwargs = {"list-rules": self.name}
		if table:
			kwargs['table'] = table
		try:
			out = iptables(**kwargs)[0]
		except linux.LinuxError, e:
			if "Unknown arg `--list-rules'" in e.err:
				LOG.debug("Iptables does not support --list-rules. "
						  "Iptables.list() defaults to [], iptables.ensure() "
						  "degrades to simple insertion or appending that may "
						  "result in rule duplication.")
				return []
			else:
				raise


		# parse
		for rule in out.splitlines():
			args = shlex.split(rule)
			if "-P" in args or "-N" in args:
				continue  # dull rules

			# convert all short options to long
			for i, arg in enumerate(args):
				if len(arg) == 2 and arg.startswith('-'):
					args[i] = _OPTIONS[arg]  # will crash on unknown short opt

			# build a new ruledict in result
			result.append({})
			reduce(build_ruledict, args)

			# postprocess: hide append option
			del result[-1]["append"]  #? except KeyError: pass

		return result


class _Chains(object):

	_predefined = (
		"INPUT",
		"FORWARD",
		"OUTPUT",
		"PREROUTING",
		"POSTROUTING",
	)
	_container = dict([(name, _Chain(name)) for name in _predefined])

	def __getitem__(self, name):
		return self._container[name]

	def __iter__(self):
		return iter(self._container)

	def __contains__(self, value):
		return value in self._container

	def add(self, name):
		assert name not in self._predefined
		self._container.setdefault(name, _Chain(name))

		# create
		try:
			iptables(**{"new-chain": name})
		except linux.LinuxError, e:
			if not e.err == "iptables: Chain already exists.":
				raise

	def remove(self, name, force=False):
		assert name not in self._predefined

		if force:
			iptables(flush=name)
			# TODO: delete references
		iptables(**{"delete-chain": name})

		self._container.pop(name)


chains = _Chains()

INPUT = chains["INPUT"]
FORWARD = chains["FORWARD"]
OUTPUT = chains["OUTPUT"]
PREROUTING = chains["PREROUTING"]
POSTROUTING = chains["POSTROUTING"]


def list(chain, table=None):
	return chains[chain].list(table)


def ensure(chain_rules, append=False):
	# {chain: [rule, ...]}
	# Will simply insert missing rules at the beginning.
	# NOTE: rule comparsion is far from ideal, check _to_inner method
	# note: existing rules don't have table attribute

	LOG.debug("Current iptables %s: " % str(list("INPUT")))
	LOG.debug("Inserting iptables rules: " + str(chain_rules["INPUT"]))

	for chain, rules in chain_rules.iteritems():
		existing = list(chain)
		for rule in reversed(rules):
			rule_repr = _to_inner(rule)
			if rule_repr not in existing:
				if not append:
					chains[chain].insert(None, rule)
					existing.insert(0, rule_repr)
				else:
					chains[chain].append(rule)
					existing.append(rule_repr)


def _to_inner(rule):
	"""
	Converts rule to its inner representation for comparsion.

	1. "source": "192.168.0.1" -> "source": "192.168.0.1/32"
	2. "dport": 22 -> "dport": "22"

	TODO:

	"src": $value -> "source": $value
	"proto": $value -> "protocol": $value
	"syn": True -> "tcp-flags": "FIN,SYN,RST,ACK SYN"
	"protocol": "tcp" -> "protocol": "tcp", "match": "tcp" for all protocols
	format "destination" same way as "source"
	"""
	inner = copy(rule)

	# 1
	if inner.has_key("source") and inner["source"][-3] != '/':
		inner["source"] += "/32"
	# 2
	if inner.has_key("dport") and isinstance(inner["dport"], int):
		inner["dport"] = str(inner["dport"])

	return inner


def enabled():
	if linux.os['family'] in ('RedHat', 'Oracle'):
		out = redhat.chkconfig(list="iptables")[0]
		return bool(re.search(r"iptables.*?\s\d:on", out))
	else:
		return os.access(IPTABLES_BIN, os.X_OK)


def _after_merge():
	raise Exception()
	# replace ip check with this
	def is_ip(s):
		return [n.isdigit() and 0 <= int(n) <= 255 for n in s.split('.')] == \
			   [True] * 4
	# make auto match for tcp and udp



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
# iptables.ensure({'INPUT': [{rule}, {rule}]})
)
'''
"""
