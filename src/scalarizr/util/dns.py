'''
Created on Sep 28, 2011
'''

from collections import namedtuple

import logging


HostLine=namedtuple('host', ['ipaddr', 'hostname', 'aliases'])

class HostsFile(object):
	FILENAME ='/etc/hosts'

	hosts = []

	def __init__(self, filename=None):
		self.filename = filename or self.FILENAME
	
	def _read(self, hostname=None):
		self.hosts = []
		res=None
		ip=None
		
		f=open(self.filename,'r')
		
		for line in f:
			if not '#' in line:
				line=line.replace('\t',' ')
				while '  ' in line:
					line=line.replace('  ',' ')
				isreadable=False
				#check on elements in line
				try:
					host=line.split()
					leng=len(list(host))
					if leng==2:
						alias=''
						(ip, domain)=host
						isreadable=True
					elif leng==3:
						(ip, domain, alias)=host
						isreadable=True
				except:
					#TODO: if it can't to split line, logging?
					pass
				#add to hosts list
				if isreadable:
					flag=True
					for host in self.hosts:
						if domain==host.hostname:
							if not host.aliases:
								list_alias=['',]
							else:
								list_alias=host.aliases
							self.hosts.remove(host)
							host._replace(aliases=list_alias.append(alias))
							self.hosts.append(host)
							flag=False
							break
					if flag:
						self.hosts.append(HostLine(ip, domain, [alias,]))
		f.close()
		#return result
		for host in self.hosts:
			if hostname==host.hostname:
				flag=True
				return host
		return False
	
	@property
	def _save(self):
		try:
			f=open(self.filename, 'w')
			for host in self.hosts: 
				for alias in host.aliases:
					line=' '.join([host.ipaddr, host.hostname, alias])
					f.write("%s\r\n"%line)
			f.close()
			return True
		except Exception, e:
			logging.warn('Error write hosts in file, method _save():%s'%e)
			return False

	def map(self, ipaddr, hostname, *aliases):
		'''
		Updates hostname -> ipaddr mapping and aliases
		@type hostname: str
		@type ipaddr: str
		'''

		if hostname:
			host=self._read(hostname=hostname)

			if host:
				if not ipaddr:
					ipaddr=host.ipaddr
				temp=host.aliases+list(aliases)
				temp=list(set(temp))
				self.hosts.remove(host)
				self.hosts.append(HostLine(ipaddr, host.hostname, temp))
				return self._save
			else:
				if ipaddr:
					temp=list(set(aliases))
					self.hosts.append(HostLine(ipaddr, hostname, temp))
					return self._save
		else:
			return False

	def remove(self, hostname):
		'''
		Removes hostname mapping and aliases
		@type hostname:str
		'''
		host=self._read(hostname=hostname)

		if host:
				self.hosts.remove(host)
				return self._save
		else:
			return False

	def alias(self, hostname, *aliases):
		'''
		Add hostname alias
		@type hostname: str
		@type *aliases: str or tuple, list
		'''
		if hostname and aliases:
		
			host=self._read(hostname=hostname)

			if host:
				temp=host.aliases+list(aliases)
				temp=list(set(temp))
				self.hosts.remove(host)
				self.hosts.append(HostLine(host.ipaddr, host.hostname, temp))
				return self._save
			else:
				logging.warn('host with that hostname not found')
				return False
		else:
			logging.warn('params not correct')
			return False

	def unalias(self, hostname, *aliases):
		'''
		Removes hostname alias
		@type hostname:str
		@type *aliases: str or tuple, list
		'''
		if hostname and aliases != None:
		
			host=self._read(hostname=hostname)

			if host:
				temp=host.aliases
				delete_list=list(aliases)
				for del_alias in delete_list:
					if del_alias in temp:
						temp.remove(del_alias)
				if not temp:
					temp=['',]
				
				self.hosts.remove(host)
				self.hosts.append(HostLine(host.ipaddr, host.hostname, temp))
				return self._save
			else:
				logging.warn('host with that hostname not found')
				return False
		else:
			logging.warn('params not correct')
			return False

	def resolve(self, hostname):
		'''
		Returns ip address
		@type hostname: str
		'''
		res=self._read(hostname=hostname)
		if res:
			return res.ipaddr
		else:
			return self.hosts

	def get(self, hostname=None):
		'''
		Returns namedtuple(ipaddr, hostname, aliases)
		@type hostname:str
		'''
		res=self._read(hostname=hostname)
		if res:
			return res
		else:
			return self.hosts
