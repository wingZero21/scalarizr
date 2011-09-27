'''
Created on Nov 26, 2010

@author: Dmytro Korsakov
'''
from scalarizr.config import ScalarizrCnf
from scalarizr import queryenv
from scalarizr.queryenv import QueryEnvService
from scalarizr.bus import bus
from scalarizr.util.filetool import read_file
from scalarizr.util.software import system_info, whereis
from scalarizr import init_script
from scalarizr.util import system2

import smtplib
from email.Utils import formatdate
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders

from optparse import OptionParser, _, HelpFormatter
#import optparse

import ConfigParser
import tarfile
import sys
import os
try:
	import json
except ImportError:
	import simplejson as json

try:
	from prettytable import PrettyTable as PTable
except:
	print('prettytable modul not found')

#23.09.11----------------------------------------------------------------------------------------------
class ScalrError(BaseException):
	pass

class IndHelpFormatter(HelpFormatter):
	"""Format help with indented section bodies.
    """
	def __init__(self,
                 indent_increment=0,
                 max_help_position=24,
                 width=None,
                 short_first=1):
		HelpFormatter.__init__(
            self, indent_increment, max_help_position, width, short_first)

	def format_usage(self, usage):
		return _("[+] %s") % usage

	def format_heading(self, heading):
		return "%*s%s:\n" % (self.current_indent, "", heading)

class Command(object):
	name = None
	method = None
	parser = None
	fields = None

	kwds = {}
	
	@property
	def usage(self):
		return self.parser.get_usage()

	@classmethod
	def queryenv(cls):
		if not hasattr(cls, '_queryenv'):
			init_cnf()

			key_path = os.path.join(bus.etc_path, ini.get('general', 'crypto_key_path'))
			server_id = ini.get('general', 'server_id')
			url = ini.get('general','queryenv_url')
			cls._queryenv = QueryEnvService(url, server_id, key_path)		
		return cls._queryenv

	def __init__(self, argv=None):
		if argv:
			self.kwds =self.parser.parse_args(argv)[0].__dict__
		else:
			self.kwds=None


	def run(self):
		try:
			ishelp = self.kwds['help']
		except:
			ishelp = None
		
		if self.kwds and not ishelp:
			result = getattr(self.queryenv(), self.method)(**self.kwds)
		elif ishelp:
			self.parser.format_help()
		else:
			result = getattr(self.queryenv(), self.method)()
		self.output(result)

	def _dict_append_in2_lines(self, _dict, lines=None):
		_list=[]
		if not lines:
			lines=[]
		if self.fields:
			for _param in self.fields:
				_list.append(_dict.get(_param))
		else:
			self.fields=[]
			for _key in _dict.keys():
				_list.append(_dict.get(_key))
				self.fields.append(_key)
		if _list:
			lines.append(_list)
		return lines

	def output(self, result):
		_lines = []
		if result and isinstance(result, list):
				for dict_line in result:
					_dict=self.get_dict_param(dict_line)
					#if we need show some of fields we must set field
					_lines=self._dict_append_in2_lines(_dict, _lines)
		else:
			_dict=self.get_dict_param(result)
			_lines=self._dict_append_in2_lines(_dict)
		_list = []
		out = None
		if self.fields:
			if not _lines:
				for i in self.fields:
					_list.append('-')
				_lines.append(_list)
			out=PTable(self.fields)
			for _list in _lines:
				out.add_row(_list)
		print(out)


class GetlatestVersionCommand(Command):
	name="get-latest-version"
	method="get_latest_version"
	fields =['version']

	parser = OptionParser(usage='get-latest-version [-h --help]'
		'\n\t Display latest versioin\n', formatter= IndHelpFormatter())

	def get_dict_param(self, d):
		'''return: 
		{version: string}'''
		return {'version':d}


class ListEbsMountpointsCommand(Command):
	name = "list-ebs-mountpoints"
	method = "list_ebs_mountpoints"
	fields =['name', 'dir', 'createfs', 'isarray', 'volume-id', 'device']

	parser = OptionParser(usage='list-ebs-mountpoints [-h --help]'
		'\n\t Display ebs mountpoints\n', formatter=IndHelpFormatter())

	'''
	def run(self):
		m1=queryenv.Mountpoint(name='Mountpoint 1', dir='dir1', create_fs=False, is_array=True,
			volumes=[queryenv.Volume(device='dev 1',volume_id='21'),queryenv.Volume(device='dev 2', volume_id='22')])

		m2=queryenv.Mountpoint(name='Mountpoint 2', dir='dir2', create_fs=False, is_array=True,
			volumes=[queryenv.Volume(device='dev 3',volume_id='23'),queryenv.Volume(device='dev 4', volume_id='24')])

		self.output([m1,m2])
	'''

	def get_dict_param(self, d):
		#Mountpoint[] 
		vols=[]
		devs=[]
		for _vol in d.volumes:
			vols.append(_vol.volume_id)
			devs.append(_vol.device)
		vols=', '.join(vols)
		devs=', '.join(devs)
		return {'name':d.name, 'dir':d.dir, 'createfs':d.create_fs, 'isarray':d.is_array, 
			'volume-id':vols, 'device':devs}


class ListRolesCommand(Command):
	name = "list-roles"
	method = "list_roles"
	fields = ['behaviour','name', 'index', 'internal-ip',
		'external-ip', 'replication-master']

	parser = OptionParser(usage='list-roles [-b --behaviour] '
		'[-r --role] [-h --help]\n\t Display roles list\n',
		 formatter= IndHelpFormatter())
	parser.add_option('-b', '--behaviour', dest='behaviour', help='Role behaviour')
	parser.add_option('-r', '--role-name', dest='role_name', help='Role name')

	def get_dict_param(self, d):
		return {'behaviour':', '.join(d.behaviour), 'name':d.name,
			'index':d.hosts[0].index, 'replication-master':d.hosts[0].replication_master,
			'internal-ip':d.hosts[0].internal_ip, 'external-ip':d.hosts[0].external_ip}


class GetHttpsCertificateCommand(Command):
	name = "get-https-certificate"
	method = "get_https_certificate" 
	fields = ['cert', 'pkey', 'cacert']

	parser = OptionParser(usage='get-https-certificate [-h --help]\n\t Display cert,'
		' pkey https certificate\n', formatter=IndHelpFormatter())
	
	def get_dict_param(self, d):
		'''return:
		(cert, pkey, cacert)
		'''
		(cert, pkey, cacert)=d
		return {'cert':cert, 'pkey':pkey, 'cacert':cacert}


class ListRoleParamsCommand(Command):
	name = "list-role-params"
	method = "list_role_params"
	
	parser = OptionParser(usage='list-role-params [-n --name][-h --help]'
		'\n\t Display list role param by name\n', formatter=IndHelpFormatter())
	parser.add_option('-n', '--name', dest='name', help='Show params by role name ')
	
	'''
	def run(self):
		self.output([{'key1':'val1','key2':'val2','key3':'val3','key4':'val4'}])
	'''
	
	def get_dict_param(self, d):
		'''dictionary
		#ERROR:'NoneType' object has no attribute 'nodeValue'
		'''
		return d


class ListVirtualhostsCommand(Command):
	name = "list-virtualhosts"
	method = "list_virtual_hosts"
	fields = ['hostname', 'https', 'type', 'raw']

	parser = OptionParser(usage='list-virtualhosts'
		' [-n --name] [-s --https] [-h --help]'
		'\n\t Display list of virtual hosts\n', formatter=IndHelpFormatter())
	parser.add_option('-n', '--name', dest='name', help='Show virtual host by name')
	parser.add_option('-s', '--https', dest='https', help='Show virtual hosts by https')
	
	'''
	def run(self):
		res=[queryenv.VirtualHost(hostname='194.162.85.4', type='virHost', raw='<![CDATA[ ]]>', https=False),
			queryenv.VirtualHost(hostname='201.1.85.4', type='virHost2', raw='<![CDATA[ ]]>', https=True)]
		self.output(res)'''
	
	def get_dict_param(self, d):
		'''return:
		{hostname : string
		type : string
		raw : string
		https : 0|1}
		'''
		return {'hostname':d.hostname, 'https':d.https, 'type':d.type, 'raw':d.raw}


class ListScriptsCommand(Command):
	name = "list-scripts"
	method = "list_scripts"
	fields = ['asynchronous', 'exec-timeout', 'name', 'body']
	
	parser = OptionParser(usage='list-scripts [-e --event]'
		' [-a --asynchronous] [-n --name] [-h --help]'
		'\n\t Display list of scripts\n', formatter=IndHelpFormatter())
	parser.add_option('-e', '--event', dest='event', help='Show scripts host on event')
	parser.add_option('-a', '--asynchronous', dest='asynchronous', 
		help='Show scripts host by asynchronous')
	parser.add_option('-n', '--name', dest='name', help='Show script(s) with name')
	
	'''
	def run(self):
		self.output([queryenv.Script(asynchronous=False, exec_timeout=126, name='Script1',
			body='<script> ... </script>'), queryenv.Script(asynchronous=True,
			exec_timeout=12006, name='Script2', body='<script> ... </script>')])
	'''
	
	def get_dict_param(self, d):
		'''return:
		asynchronous : 1|0
		exec_timeout : string 
		name : string
		body : string
		'''
		return {'asynchronous':d.asynchronous, 'exec-timeout':d.exec_timeout,
			'name':d.name, 'body':d.body}


class Help(Command):
	name='help'
	com_dict=None
	
	parser = None
	
	def __init__(self,com_d=None):
		if com_d:
			self.com_dict=com_d

	def run(self, com_d=None):
		if com_d:
			self.com_dict=com_d
		if self.com_dict:
			for com_name in self.com_dict.keys():
				try: 
					com_obj=self.com_dict.get(com_name)()
					
					if not isinstance(com_obj, Help):
						print(com_obj.usage)
				except Exception, e:
					print('Cant show usage of command: %s;'
						' Error in szradm.Help.run() function: %s' % (com_name, e))		

#-------------------------------------------------------------------------------------------------


def get_mx_records(email):
	out = system2('%s -t mx %s' % (whereis('host')[0], email.split('@')[-1]), shell=True)[0]
	mxs = [mx.split()[-1][:-1] if mx.endswith('.') else mx for mx in out.split('\n')]
	if '' in mxs: mxs.remove('')
	#from sets import Set
	#return list(Set(mxs))
	temp = {}
	for x in mxs: temp[x] = None
	return list(temp.keys())

ini = None
def init_cnf():
	cnf = ScalarizrCnf(bus.etc_path)
	cnf.bootstrap()
	globals()['ini'] = cnf.rawini
	
def main():
	global ini
	init_script()

#23.09.11-------------------------------------------------------------------------------------------------

	com_dict={'list-roles':ListRolesCommand,
			'get-latest-version':GetlatestVersionCommand,
			'list-ebs-mountpoints':ListEbsMountpointsCommand,
			'get-https-certificate':GetHttpsCertificateCommand,
			'list-role-params':ListRoleParamsCommand,
			'list-virtualhosts':ListVirtualhostsCommand,
			'list-scripts':ListScriptsCommand,
			'help':Help,
			'--help':Help,
			'-h':Help
		}

	str=None
	com=None
	com_find=None

	if len(sys.argv)>1:
		com_find=1
	if com_find and com_dict.has_key(sys.argv[1]):
		if sys.argv[2:]:
			str=sys.argv[2:]
		#select command class in list and create object of this class with param
		try:
			com=com_dict.get(sys.argv[1])(str)
			if com:
				if isinstance(com, Help):
					com.run(com_dict)
				else:
					com.run()
		except Exception, e:
			com=Help(com_dict)
			com.run()
			print('Cant execute command or option. Error:%s' % e)
	else:
#-------------------------------------------------------------------------------------------------	

		parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
		parser.add_option("-q", "--queryenv", dest="queryenv", action="store_true",
			default=False, help="QueryEnv CLI")
		parser.add_option("-m", "--msgsnd", dest="msgsnd", action="store_true",
			default=False, help="Message sender CLI")
		parser.add_option("-r", "--repair", dest="repair", action="store_true",
			default=False,	help="Repair database")
		parser.add_option('--reinit', dest='reinit', action='store_true', default=False,
			help='Reinitialize Scalarizr')
		parser.add_option("-n", "--name", dest="name", default=None, help="Name")
		parser.add_option("-f", "--msgfile", dest="msgfile", default=None, help="File")
		parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Endpoint")
		parser.add_option("-o", "--queue", dest="queue", default=None, help="Queue")
		parser.add_option("-s", "--qa-report", dest="report", action="store_true",
			default=None, help="Build report with logs and system info")

		(options, raw_args) = parser.parse_args()

		if not options.queryenv and not options.msgsnd and not options.repair \
			and not options.report and not options.reinit:
			print parser.format_help()
			sys.exit()

		args = []
		kv = {}
		for pair in raw_args:
			raw = pair.split("=")
			if len(raw)>=2:
				k = raw[0]
				v = raw[1]
				kv[k] = v
			elif len(raw)==1: 
				args.append(pair)

		if options.queryenv:

			if not args:
				print parser.format_help()
				sys.exit()
			init_cnf()

			key_path = os.path.join(bus.etc_path, ini.get('general', 'crypto_key_path'))
			server_id = ini.get('general', 'server_id')
			url = ini.get('general','queryenv_url')

			qe = QueryEnvService(url, server_id, key_path)
			xml = qe.fetch(*args, **kv)
			print xml.toprettyxml()

		if options.msgsnd:

			if not options.queue or (not options.msgfile and not options.name):
				print parser.format_help()
				sys.exit()

			msg_service = bus.messaging_service
			producer = msg_service.get_producer()

			init_cnf()

			producer.endpoint = options.endpoint or ini.get('messaging_p2p', 'producer_url')	
			msg = msg_service.new_message()

			if options.msgfile:
				str = read_file(options.msgfile, error_msg='Cannot open message file %s' %
					options.msgfile)
				if str:
					msg.fromxml(str)
			else:
				msg.body = kv
	
			if options.name:
				msg.name = options.name
	
			producer.send(options.queue, msg)

			print "Done"		

		if options.reinit:
			print 'Call scalarizr to reinitialize role (see /var/log/scalarizr.log for results)'
			db = bus.db
			conn = db.get().get_connection()
			cur = conn.cursor()
			try:
				cur.execute(
						'UPDATE p2p_message SET in_is_handled = ? WHERE message_name = ?', 
					(0, 'HostInitResponse')
				)
				conn.commit()
			finally:
				cur.close()

		if options.report:			#collecting
			hostname = system2(whereis('hostname'), shell=True)[0]
			tar_file = os.path.join(os.getcwd(), 'report-%s.tar.gz' % hostname)
			json_file = os.path.join(os.getcwd(), 'sysinfo-%s.json' % hostname)

			cnf = bus.cnf
			cnf.bootstrap()
			ini = cnf.rawini
			try:
				log_params = ini.get('handler_file', 'args')
				try:
					log_file = log_params(0)
				except IndexError, TypeError:
					raise
			except Exception, BaseException:
				log_file = '/var/log/scalarizr.log'

			file = open(json_file, 'w')
			json.dump(system_info(), file, sort_keys=True, indent=4)
			file.close()

			tar = tarfile.open(tar_file, "w:gz")
			tar.add(json_file)
			if os.path.exists(log_file):
				tar.add(log_file)
			tar.close()

			#cleaning
			if os.path.exists(json_file):
				os.remove(json_file)

			#sending
			fromaddr='root@%s' % hostname
			try:
				email = ini.get('general', 'report_email') 
			except ConfigParser.NoOptionError:
				print "Unable to send email: section 'report_mail' not found in config file."
				print "Although you can send %s to support manually." % tar_file
				sys.exit(1)

			role_name = ini.get('general', 'role_name')
			server_id = ini.get('general', 'server_id')

			toaddrs=[email]
			subject = 'scalarizr report from hostname %s (role: %s , serverid: %s)' % (hostname, role_name, server_id)

			msg = MIMEMultipart()
			msg['From'] = fromaddr
			msg['To'] = email
			msg['Date'] = formatdate(localtime=True)
			msg['Subject'] = subject

			text_msg = MIMEText(subject)
			msg.attach(text_msg)

			part = MIMEBase('application', "octet-stream")
			part.set_payload( open(tar_file,"rb").read())
			Encoders.encode_base64(part)
			part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(tar_file))
			msg.attach(part)

			for server in get_mx_records(email):
				try:
					print 'Sending message to %s through %s' % (email, server)
					smtp = smtplib.SMTP(server)
					smtp.sendmail(fromaddr, toaddrs, msg.as_string())
					break
				except (Exception, BaseException), e:
					print e, '\nTrying next mx entry'
				finally:
					smtp.close()
	
			print "Done."


if __name__ == '__main__':
	main()

'''
* szadm --queryenv list-roles behaviour=app
* szadm --msgsnd -n BlockDeviceAttached devname=/dev/sdo
* szadm --msgsnd --lo -n IntServerReboot
* szadm --msgsnd --lo -f rebundle.xml
* szadm --repair host-up
'''

"""
<?xml version="1.0" ?>
<message id="037b1864-4539-4201-ac0b-5b1609686c80" name="Rebundle">
    <meta>
        <server_id>ab4d8acc-f001-4666-8f87-0748af52f700</server_id>
    </meta>
    <body>
        <platform_access_data>
            <account_id>*account_id*</account_id>
            <key_id>*key_id*</key_id>
            <key>*key*</key>
            <cert>*cert*</cert>
            <pk>*pk*</pk>
        </platform_access_data>
        <role_name>euca-base-1</role_name>
        <bundle_task_id>567</bundle_task_id>
        <excludes><excludes>
    </body>
</message>
"""
