'''
Created on Jul 20, 2010

@author: marat
'''

from scalarizr import init_script
from scalarizr.util import disttool, system
import logging

def main():
	init_script()
	logger = logging.getLogger("scalarizr.scripts.update")
	logger.info("Starting update script...")
	
	if disttool.is_debian_based():
		logger.info("Updating scalarizr with Apt")
		system("apt-get -y install scalarizr")
	elif disttool.is_redhat_based():
		logger.info("Updating scalarizr with Yum")
		system("yum -y update scalarizr")
	else:
		logger.error("Don't know how to update scalarizr on %s", " ".join(disttool.linux_dist()))
