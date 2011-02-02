'''
Created on Nov 26, 2010

@author: Dmytro Korsakov
'''
from scalarizr.config import ScalarizrCnf
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
from optparse import OptionParser
import ConfigParser
import tarfile
import sys
import os
try:
	import json
except ImportError:
	import simplejson as json


def _init():
	init_script()
	
def get_mx_records(email):
	out = system2('%s -t mx %s' % (whereis('host')[0], email.split('@')[-1]), shell=True)[0]
	mxs = [mx.split()[-1][:-1] if mx.endswith('.') else mx for mx in out.split('\n')]
	if '' in mxs: mxs.remove('')
	from sets import Set
	return list(Set(mxs))

def main():
	
	_init()
	
	parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
	parser.add_option("-q", "--queryenv", dest="queryenv", action="store_true", default=False, help="QueryEnv CLI")
	parser.add_option("-m", "--msgsnd", dest="msgsnd", action="store_true", default=False, help="Message sender CLI")
	parser.add_option("-r", "--repair", dest="repair", action="store_true", default=False, help="Repair database")
	parser.add_option('--reinit', dest='reinit', action='store_true', default=False, help='Reinitialize Scalarizr')
	parser.add_option("-n", "--name", dest="name", default=None, help="Name")
	parser.add_option("-f", "--msgfile", dest="msgfile", default=None, help="File")
	parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Endpoint")
	parser.add_option("-o", "--queue", dest="queue", default=None, help="Queue")
	parser.add_option("-s", "--qa-report", dest="report", action="store_true", default=None, help="Build report with logs and system info")
	
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
		
		cnf = ScalarizrCnf(bus.etc_path)
		cnf.bootstrap()
		ini = cnf.rawini
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
		if options.endpoint:
			producer.endpoint = options.endpoint
	
		msg = msg_service.new_message()
	
		if options.msgfile:
			str = read_file(options.msgfile, error_msg='Cannot open message file %s' % options.msgfile)
			if str:
				msg.fromxml(str)
			
		if msg.name:
			msg.name = options.name
			
		msg.body = kv
			
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
	
	if options.report:
		#collecting
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
			
		toaddrs=[email]
		subject = 'scalarizr report from %s' % hostname
		
		msg = MIMEMultipart()
		msg['From'] = fromaddr
		msg['To'] = email
		msg['Date'] = formatdate(localtime=True)
		msg['Subject'] = subject

		part = MIMEBase('application', "octet-stream")
		part.set_payload( open(tar_file,"rb").read() )
		Encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(tar_file))
		msg.attach(part)
		
		for server in get_mx_records(email):
			try:
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
