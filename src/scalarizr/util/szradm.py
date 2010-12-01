'''
Created on Nov 26, 2010

@author: Dmytro Korsakov
'''
from scalarizr.config import ScalarizrCnf
from scalarizr.queryenv import QueryEnvService
from scalarizr.bus import bus
from scalarizr.util.filetool import read_file
from scalarizr import init_script

from optparse import OptionParser
import sys
import os


def _init():
	init_script()

def main():
	
	_init()
	
	parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
	parser.add_option("-q", "--queryenv", dest="queryenv", action="store_true", default=False, help="QueryEnv CLI")
	parser.add_option("-m", "--msgsend", dest="msgsend", action="store_true", default=False, help="Message sender CLI")
	parser.add_option("-r", "--repair", dest="repair", action="store_true", default=False, help="Repair database")
	parser.add_option("-n", "--name", dest="name", default=None, help="Name")
	parser.add_option("-f", "--msgfile", dest="msgfile", default=None, help="File")
	parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Endpoint")
	parser.add_option("-o", "--queue", dest="queue", default=None, help="Queue")
	
	(options, raw_args) = parser.parse_args()
	
	if not options.queryenv and not options.msgsend and not options.repair:
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
		
	if options.msgsend:

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
