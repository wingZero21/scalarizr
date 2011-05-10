'''
Created on Nov 22, 2010

@author: spike
'''
from cloudservers import ImageManager
from scalarizr.handlers import Handler, HandlerError, RebundleLogHandler
from scalarizr.messaging import Messages
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.util import wait_until, software, system2
import logging
import time

def get_handlers ():
	return [RackspaceRebundleHandler()]

class RackspaceRebundleHandler(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._log_hdlr = RebundleLogHandler()	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE
	
	def on_Rebundle(self, message):
		try:
			self._log_hdlr.bundle_task_id = message.bundle_task_id
			self._logger.addHandler(self._log_hdlr)	
			role_name = message.role_name.encode("ascii")
			image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")
			pl = bus.platform
			con = pl.new_cloudservers_conn()
			servers = con.servers.list()

			self._logger.info('Lookup server %s on CloudServers', pl.get_public_ip())
			for server in servers:
				if server.public_ip == pl.get_public_ip():
					break
			else:
				raise HandlerError('Server %s not found in servers list' % pl.get_public_ip())
			
			self._logger.debug('Found server %s. server id: %s', pl.get_public_ip(), server.id)
			cnf = bus.cnf
			old_state = cnf.state
			cnf.state = ScalarizrState.REBUNDLING
			
			try:
				image_manager = ImageManager(con)
				system2("sync", shell=True)
				self._logger.info("Creating server image. server id: %s, image name: '%s'", server.id, image_name)
				image = image_manager.create(image_name, server.id)
				self._logger.debug('Image %s created', image.id)
				
				self._logger.info('Checking that image %s is completed', image.id)
				wait_until(hasattr, args=(image, 'progress'), 
						sleep=5, logger=self._logger, 
						error_text="Image %s has no attribute 'progress'" % image.id)
				wait_until(lambda: image_manager.get(image.id).progress == 100, 
						sleep=30, logger=self._logger,
						error_text="Image %s wasn't completed in a reasonable time" % image.id)
				self._logger.info('Image %s completed and available for use!', image.id)
			finally:
				cnf.state = old_state
			
			# Creating message
			msg_data = dict(
				status = "ok",
				snapshot_id = image.id,
				bundle_task_id = message.bundle_task_id
			)
			
			# Updating message with OS, software and modules info
			self._logger.debug("Updating message with OS and software info")
			msg_data.update(software.system_info())
			
			self.send_message(Messages.REBUNDLE_RESULT, msg_data)
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			last_error = str(e)
			
			# Send message to Scalr
			self.send_message(Messages.REBUNDLE_RESULT, dict(
				status = "error",
				last_error = last_error,
				bundle_task_id = message.bundle_task_id
			))
		finally:
			self._log_hdlr.bundle_task_id = None
			self._logger.removeHandler(self._log_hdlr)
			