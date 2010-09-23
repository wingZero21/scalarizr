'''
Created on Sep 23, 2010

@author: marat
'''
from scalarizr.integ_test.libs import session


EC2_ROLE_DEFAULT_SETTINGS = {
	'aws.availability_zone' : 'us-east-1a',
	'aws.instance_type' : 't1.micro'
}

EC2_MYSQL_ROLE_DEFAULT_SETTINGS = {
	'mysql.ebs_volume_size' : '1'
}

class FarmUI:
	sel = None
	farm_id = None	
	
	def __init__(self, sel):
		self.sel = sel
	
	def use(self, farm_id):
		self.farm_id = farm_id
		login(self.sel)
		sel.open('/farms_add.php?id=%s' % self.farm_id)
	
	def add_role(self, role_name, min_servers=1, max_servers=2, settings=None):
		pass
	
	def remove_role(self, role_name):
		pass
	
	def save(self):
		pass
	
	def launch(self):
		pass
	
	def terminate(self, keep_ebs=False, remove_from_dns=True):
		pass
	
def login(sel):
	pass

def reset_farm(ssh, farm_id):
	pass

def exec_cronjob(ssh, name):
	pass