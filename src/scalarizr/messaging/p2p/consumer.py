'''
Created on Dec 5, 2009

@author: marat
'''
from scalarizr.messaging import MessageConsumer, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, P2pMessage
from scalarizr.util import cryptotool, configtool
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from urlparse import urlparse
import threading
import logging
import socket
import sys


try:
	import time
except ImportError:
	import timemodule as time
import os.path


class P2pMessageConsumer(MessageConsumer):
	endpoint = None
	_server = None
	_logger = None
	_handler_thread = None
	_shutdown_handler = False

	create_handler_thread = True
	'''
	FIXME: It's ugly to solve 2 messaging endpoints problem 
	by disabling message handling in one of them
	'''
	
	def __init__(self, endpoint=None):
		MessageConsumer.__init__(self)
		self._logger = logging.getLogger(__name__)		
		self.endpoint = endpoint
		
		if self.create_handler_thread:
			self._handler_thread = threading.Thread(name='MessageHandler', target=self.message_handler)
		self.RequestHandler.consumer = self
	
	def starttest(self):
		self._logger.debug('Create test socket to validate endpoint')
		r = urlparse(self.endpoint)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock.bind((r.hostname, r.port))
		except socket.error, e:
			raise MessagingError('Cannot start message server. socket.error: %s' % e)
		finally:
			sock.close()
			
	def start(self):
		try:
			if self._server is None:
				r = urlparse(self.endpoint)
				server_cls = HTTPServer if sys.version_info >= (2,6) else _HTTPServer25
				self._server = server_cls((r.hostname, r.port),	self.RequestHandler)
				self._logger.info("Build message consumer server on %s:%s", r.hostname, r.port)
		except (BaseException, Exception), e:
			self._logger.error("Cannot build server. %s", e)
			
		self._logger.debug("Starting message consumer")
		
		try:
			if self.create_handler_thread:
				self._handler_thread.start() 	# start message handler
			self._server.serve_forever() 	# start http server
		except (BaseException, Exception), e:
			self._logger.error("Cannot start message consumer. %s", e)
			
	def stop(self):
		if (not self._server is None):
			self._logger.debug("Stopping message consumer...")
		
			# stop http server
			self._logger.debug("Stopping HTTP server")
			self._server.shutdown()
			self._logger.debug("HTTP server stopped")

			# stop message handler thread
			if self.create_handler_thread:
				self._logger.debug("Stopping message handler")
				self._shutdown_handler = True
				self._handler_thread.join()
				self._logger.debug("Message handler stopped")
			
			self._logger.debug("Message consumer stopped")

	def shutdown(self):
		self._logger.debug("Closing HTTP server")
		self._server.server_close()
		self._server = None		
		self._logger.debug("HTTP server closed")

	def message_handler (self):
		store = P2pMessageStore()
		while not self._shutdown_handler:
			try:
				for unhandled in store.get_unhandled():
					queue = unhandled[0]
					message = unhandled[1]
					try:
						self._logger.debug("Notify message listeners (message_id: %s)", message.id)
						for ln in self.listeners:
							ln(message, queue)
					except (BaseException, Exception), e:
						self._logger.exception(e)
					finally:
						self._logger.debug("Mark message (message_id: %s) as handled", message.id)
						store.mark_as_handled(message.id)
			except (BaseException, Exception), e:
				self._logger.exception(e)
					
			time.sleep(0.2)
		
	class RequestHandler(BaseHTTPRequestHandler):
		# FIXME: internal messaging overrides default consumer and ingoing messages doesn't read anymore
		consumer = None
		'''
		@cvar consumer:Message consumer instance
		@type consumer: P2pMessageConsumer 
		'''
		

		def do_POST(self):
			logger = logging.getLogger(__name__)

			queue = os.path.basename(self.path)
			rawmsg = self.rfile.read(int(self.headers["Content-length"]))
			logger.debug("Received ingoing message in queue: '%s'", queue)
			
			try:
				for f in self.consumer.filters['protocol']:
					rawmsg = f(self.consumer, queue, rawmsg)
			except (BaseException, Exception), e:
				err = 'Message consumer protocol filter raises exception: %s' % str(e)
				logger.error(err)
				logger.exception(e)
				self.send_response(400, str(e))
				return
			
			try:
				logger.debug("Decoding message")
				message = P2pMessage()
				message.fromxml(rawmsg)
			except (BaseException, Exception), e:
				err = "Cannot decode message. error: %s; xml message: %s" % (str(e), rawmsg)
				logger.error(err)
				logger.exception(e)
				self.send_response(400, err)
				return
			
			logger.info("Received ingoing message '%s' in queue %s", message.name, queue)
			
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
			logger.debug("%s %s\n", self.address_string(), format%args)		


if sys.version_info < (2,6):
	try:
		import selectmodule as select
	except ImportError:
		import select
	class _HTTPServer25(HTTPServer):
		
		def __init__(self, server_address, RequestHandlerClass):
			HTTPServer.__init__(self, server_address, RequestHandlerClass)
			self.__is_shut_down = threading.Event()
			self.__serving = False
		
		def serve_forever(self, poll_interval=0.5):
			logger = logging.getLogger(__name__)
			logger.debug("_HTTPServer25 serving...")
			self.__serving = True
			self.__is_shut_down.clear()
			while self.__serving:
				# XXX: Consider using another file descriptor or
				# connecting to the socket to wake this up instead of
				# polling. Polling reduces our responsiveness to a
				# shutdown request and wastes cpu at all other times.
				r, w, e = select.select([self], [], [], poll_interval)
				if r:
					self.handle_request()
			
			self.__is_shut_down.set()
		
		def shutdown(self):
			self.__serving = False
			logger = logging.getLogger(__name__)
			logger.debug("Set serving to False")
			self.__is_shut_down.wait()

			