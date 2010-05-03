'''
Created on Dec 5, 2009

@author: marat
'''
from scalarizr.messaging import MessageConsumer
from scalarizr.messaging.p2p import P2pMessageStore, P2pMessage, _P2pBase, P2pConfigOptions
from scalarizr.util import cryptotool, configtool
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from urlparse import urlparse
from threading import Thread
import logging
try:
	import time
except ImportError:
	import timemodule as time
import os.path


class P2pMessageConsumer(MessageConsumer, _P2pBase):
	endpoint = None
	_server = None
	_logger = None
	_handler_thread = None
	_shutdown_handler = False
	
	def __init__(self, **kwargs):
		_P2pBase.__init__(self, **kwargs)
		self.endpoint = kwargs[P2pConfigOptions.CONSUMER_URL]
		self._logger = logging.getLogger(__name__)
		self._handler_thread = Thread(name="MessageHandler", target=self.message_handler)
			
	def start(self):
		if self._server is None:
			r = urlparse(self.endpoint)
			_HttpRequestHanler.consumer = self
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
			
			self._logger.info("Stopped")

	def message_handler (self):
		store = P2pMessageStore()
		while not self._shutdown_handler:
			for unhandled in store.get_unhandled():
				queue = unhandled[0]
				message = unhandled[1]
				try:
					self._logger.info("Notify message listeners (message_id: %s)", message.id)
					for ln in self._listeners:
						ln(message, queue)
				except (BaseException, Exception), e:
					self._logger.exception(e)
				finally:
					self._logger.debug("Mark message (message_id: %s) as handled", message.id)
					store.mark_as_handled(message.id)
					
			time.sleep(0.2)
		
	
class _HttpRequestHanler(BaseHTTPRequestHandler):
	consumer = None

	def do_POST(self):
		logger = logging.getLogger(__name__)
		
		queue = os.path.basename(self.path)
		rawmsg = self.rfile.read(int(self.headers["Content-length"]))
		logger.debug("Received ingoing message. queue: '%s', rawmessage: %s" % (queue, rawmsg))
		
		try:
			logger.debug("Decrypt message")
			crypto_key = configtool.read_key(self.consumer.crypto_key_path)
			xml = cryptotool.decrypt(rawmsg, crypto_key)
			
			logger.debug("Decode message")
			message = P2pMessage()
			message.fromxml(xml)
			
		except (BaseException, Exception), e:
			logger.exception(e)
			self.send_response(400, str(e))
			return
		
		logger.info("Received ingoing message. queue: '%s' message: %s" % (queue, message))
		
		try:
			store = P2pMessageStore()
			store.put_ingoing(message, queue)
			
		except (BaseException, Exception), e: 
			logger.exception(e) 
			self.send_response(500, str(e))
			return
		
		self.send_response(201)
		
		
		
	def log_message(self, format, *args):
		logger = logging.getLogger(__name__)
		logger.info("%s %s\n", self.address_string(), format%args)
