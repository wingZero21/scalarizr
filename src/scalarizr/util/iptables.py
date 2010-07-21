'''
Created on Jul 21, 2010

@author: marat
'''

P_TCP = "tcp"
P_UDP = "udp"
P_UDPLITE = "udplite"
P_ICMP = "icmp"
P_ESP = "esp"
P_AH = "ah"
P_SCTP = "sctp"
P_ALL = "all"


class RuleSpec(object):
	options = None
	rule_num = None
	
	def __init__(self, protocol=None, source=None, destination=None, jump=None, 
			goto=None, inint=None, outint=None, set_counters=None):
		self.options = dict()
		
	def get_protocol (self):
		return self.options["-p"]
	
	def set_protocol (self, p):
		if not hasattr(p, "__iter__"):
			p = (p, True)
		if len(p) != 2:
			raise
		self.options["-p"] = p

	protocol = property(get_protocol, set_protocol)
	
	def __str__(self):
		pass

class IpTables(object):
	executable = None
	
	def __init__(self, executable=None):
		self.executable = executable or "/sbin/iptables"
		
	def append_rule(self, chain, rule_spec):
		pass

	def insert_rule(self, chain, rule_num, rule_spec):
		pass
	
	def delete_rule(self, chain, rule_spec):
		pass
	
	def list_rules(self, chain):
		pass