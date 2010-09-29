from szr_integtest import get_selenium, config
from szr_integtest_libs import expect
from szr_integtest_libs.scalrctl import FarmUI
from ConfigParser import ConfigParser
import os
import re
import time
import paramiko
from scalarizr.util import system
from szr_integtest_libs.szrctl import TailLogSpawner



role_name = 'mysql-058-u1004'
farm_id = config.get('test-farm', 'farm_id')
farm_key = config.get('test-farm', 'farm_key')

server_id_re = re.compile('\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' % (farm_id, role_name), re.M)

farm = FarmUI(get_selenium())
print "Launching farm"
farm.use(farm_id)
farm.launch()

print "Farm launched"
out = system('php -q /home/spike/workspace/scalr/scalr.net-trunk/app/cron-ng/cron.php --Scaling')[0]

result = re.search(server_id_re, out)
if not result:
	raise Exception('Farm hasn\'t been scaled up')

server_id = result.group('server_id')
print "New server id: %s" % server_id
ip = farm.get_public_ip(server_id)
print "New server's ip: %s" % ip

spawner = TailLogSpawner(ip, farm_key)
channel = spawner.spawn()

sequence = ['HostInitResponse', 'Initializing MySQL master', "Message 'HostUp' delivered"]
system('php -q /home/spike/workspace/scalr/scalr.net-trunk/app/cron-ng/cron.php --ScalarizrMessaging')

for regexp in sequence:
	expect(channel, regexp, 60)
	print "regexp OK"

	


	




