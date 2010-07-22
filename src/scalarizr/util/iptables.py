'''
Created on Jul 21, 2010

@author: marat
'''

from scalarizr.util import system
import sys

P_TCP = "tcp"
P_UDP = "udp"
P_UDPLITE = "udplite"
P_ICMP = "icmp"
P_ESP = "esp"
P_AH = "ah"
P_SCTP = "sctp"
P_ALL = "all"


class RuleSpec(object):
	specs = None
	
	def __init__(self, protocol=None, source=None, destination=None, 
				inint=None, outint=None, dport = None, jump=None, custom=None):	
		self.specs = {}
		self.specs['-p'] = protocol
		self.specs['-s'] = source
		self.specs['-d'] = destination	
		self.specs['-i'] = inint
		self.specs['-o'] = outint
		self.specs['-j'] = jump
		self.specs['-dport'] = dport
		self.specs['custom'] = custom
		
	def __str__(self):
		rule_spec = ''			
		for key in self.specs:
			if self.specs[key] and key != 'custom':
				rule_spec +=' ! %s %s' % (key,self.specs[key][0]) if is_inverted(self.specs[key]) \
						else ' %s %s' % (key,self.specs[key])
		if self.specs['custom']:
			rule_spec += self.specs['custom']				
		return str(rule_spec)
	
	def __eq__(self, other):
		p = self.specs['-p'] == other.specs['-p'] or \
			(not self.specs['-p'] and other.specs['-p']=='ALL') or \
			(not other.specs['-p'] and self.specs['-p']=='ALL')
		
		s = self.specs['-s'] == other.specs['-s'] or \
			(not self.specs['-s'] and other.specs['-s']=='0.0.0.0/0') or \
			(not other.specs['-s'] and self.specs['-s']=='0.0.0.0/0')
		
		d = (self.specs['-d'] == other.specs['-d']) or \
			(not self.specs['-d'] and other.specs['-d']=='0.0.0.0/0') or \
			(not other.specs['-d'] and self.specs['-d']=='0.0.0.0/0')
			
		i = self.specs['-i'] == other.specs['-i']
		o = self.specs['-o'] == other.specs['-o']
		j = self.specs['-j'] == other.specs['-j']
		dport = self.specs['-dport'] == other.specs['-dport']
		
		if p and s and d and i and o and j and dport:
			return True
		else:
			return False

def is_inverted(param):
	return type(param) == tuple and len(param) > 1 and not param[1]

class IpTables(object):
	executable = None
	
	def __init__(self, executable=None):
		self.executable = executable or "/sbin/iptables"
		
	def append_rule(self, rule_spec, chain='INPUT'):
		rule = "%s -A %s%s" % (self.executable, chain, str(rule_spec))
		system(rule)

	def insert_rule(self, rule_num, rule_spec, chain='INPUT'):
		if not rule_num:
			rule_num = ''
		rule = "%s -I %s %s%s" % (self.executable, chain, str(rule_num), str(rule_spec))
		system(rule)
	
	def delete_rule(self, rule_spec, chain='INPUT'):
		rule = "%s -D %s%s" % (self.executable, chain, str(rule_spec))
		system(rule)

	def list_rules(self, chain='INPUT'):
		table = system('%s --line-numbers -nvL %s' % (self.executable, chain))[0]

		list = table.splitlines()
		rules = []
		for line in list:
			if line.find("destination")==-1 and not line.startswith('Chain') and line.strip():
				row = line.split()
				row.reverse()
				num = row.pop()
				pkts = row.pop()
				bytes = row.pop()
				
				for option in range(1,len(row)):
					if row[option].startswith('!'):
						row[option] = (row[option][1:],False)
					elif row[option] in ('--','*'):
						row[option] = None

				rule = RuleSpec()
				rule.specs['-d'] = row[0]
				rule.specs['-s'] = row[1]
				rule.specs['-o'] = row[2]
				rule.specs['-i'] = row[3]
				rule.specs['-p'] = row[5]
				if len(row)>6:
					rule.specs['-j'] = row[6]
				rules.append((rule, num))			
		return rules
	
	def flush(self, chain='INPUT'):
		rule = '%s -F %s' % (self.executable, chain)
		system(rule)