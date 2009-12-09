'''
Created on Dec 5, 2009

@author: marat
'''
from scalarizr.messaging import MessageConsumer, Message, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, P2pMessage
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from threading import Thread, current_thread
import logging
import timemodule as time

class P2pMessageConsumer(MessageConsumer):
	endpoint = ""
	_server = None
	_logger = None
	_handler_thread = None
	_shutdown_handler = False
	
	def __init__(self, config={}):
		from scalarizr.util import config_apply
		config_apply(self, config)
		self._logger = logging.getLogger(__package__)
		self._handler_thread = Thread(name="MessageHandler", target=self.message_handler)
			
	def start(self):
		if self._server is None:
			from urlparse import urlparse			
			r = urlparse(self.endpoint)
			
			self._server = HTTPServer((r.hostname, r.port), _HttpRequestHanler)
			self._logger.info("Build consumer server on %s:%s", r.hostname, r.port)
			
		self._logger.info("Staring consumer...")
		self._handler_thread.start() 	# start message handler
		self._server.serve_forever() 	# start http server
	
	def stop(self):
		if (not self._server is None):
			self._logger.info("Stopping consumer...")
			
			# stop http server
			self._server.shutdown()
			 
			# stop message handler thread
			self._shutdown_handler = True
			self._handler_thread.join()

	def message_handler (self):
		store = P2pMessageStore()
		while not self._shutdown_handler:
			ids = store.get_unhandled_ids()
			for message_id in ids:
				message = store.load(message_id, True)
				try:
					self._logger.info("Notify message listener (message_id: %s)", message_id);
				except Exception, e:
					self._logger.exception(e)
				finally:
					store.mark_as_handled(message_id)
					
			time.sleep(0.2)
		
	
class _HttpRequestHanler(BaseHTTPRequestHandler):

	def do_POST(self):
		logger = logging.getLogger(__package__)
		
		import os.path
		queue = os.path.basename(self.path)
		xml = self.rfile.read(int(self.headers["Content-length"]))
		logger.info("Received ingoing message. queue: '%s', message: %s" % (queue, xml))
		
		message = P2pMessage()
		try:
			message.fromxml(xml)
		except Exception, e:
			logger.exception(e)
			self.send_response(400, str(e))
			return
		
		try:
			store = P2pMessageStore()
			store.put_ingoing(message, queue)
		except Exception, e: 
			logger.exception(e) 
			self.send_response(500, str(e))
			return
		
		self.send_response(201)
		
		
		
	def log_message(self, format, *args):
		logger = logging.getLogger(__package__)
		logger.info("%s %s\n", self.address_string(), format%args)
