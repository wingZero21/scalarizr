'''
Created on Mar 24, 2010

@author: marat
'''


class Behaviours:
	APP = "app"
	MYSQL = "mysql"
	WWW = "www"
	
	
def get_configurator(name):
	if name == Behaviours.APP:
		return AppConfigurator()
	return None

class BehaviourConfigurator:
	def configure(self, interactive, **kwargs):
		pass
	
	
class AppConfigurator(BehaviourConfigurator):
	pass


def get_behaviour_ini_name(name):
	return "behaviour.%.ini" % name

