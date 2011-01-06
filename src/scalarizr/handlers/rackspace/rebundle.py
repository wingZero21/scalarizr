'''
Created on Nov 22, 2010

@author: spike
'''
from cloudservers import CloudServers, ImageManager
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.util import wait_until, software, disttool
import logging
import time

def get_handlers ():
	return [RackspaceRebundleHandler()]

class RackspaceRebundleHandler(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE
	
	def on_Rebundle(self, message):
		try:
			role_name = message.role_name.encode("ascii")
			image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")
			platform = bus.platform
			con = platform.new_rackspace_conn()
			servers = con.servers.list()
			
			public_ip = platform.get_public_ip()
			self._logger.debug('Searching our instance in server list.')
			for server in servers:
				if server.public_ip == public_ip:
					server_id = server.id
					break
			else:
				raise HandlerError("Server is not in server list. Server's public ip: %s" % public_ip)
			
			self._logger.debug('Instance has been successfully found. ID=%s' % server_id)
			cnf = bus.cnf
			old_state = cnf.state
			cnf.state = ScalarizrState.REBUNDLING
			
			try:
				image_manager = ImageManager(con)
				self._logger.debug("Creating instance's image")
				image = image_manager.create(image_name, server_id)
				image_id = image.id
				self._logger.debug("Waiting for image completion. Image id - %s" % image_id)
				wait_until(hasattr, args=(image, 'progress'), sleep=5, logger=self._logger)
				wait_until(lambda: image_manager.get(image_id).progress == 100, sleep=30, logger=self._logger)
			finally:
				cnf.state = old_state
			self._logger.debug("Image has been successfully created.")
			# Creating message
			ret_message = dict(	status = "ok",
								snapshot_id = image_id,
								bundle_task_id = message.bundle_task_id )
			
			# Updating message with OS, software and modules info
			self._logger.debug("Updating message with os and software info.")
			ret_message.update(software.system_info())
			
			self.send_message(Messages.REBUNDLE_RESULT, ret_message)
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			last_error = hasattr(e, "error_message") and e.error_message or str(e)
			# Send message to Scalr
			self.send_message(Messages.REBUNDLE_RESULT, dict(
				status = "error",
				last_error = last_error,
				bundle_task_id = message.bundle_task_id
			))		
			