
'''
Usage:
'''

from scalarizr.linux import iptables

# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING


iptables.INPUT.extend([
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
iptables.list_all()


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


# TODO: State function
def ensure(
	
)
