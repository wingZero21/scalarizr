'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.messaging import MessageProducer, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, _P2pBase, P2pConfigOptions
from scalarizr.util import cryptotool, configtool
from urllib import splitnport
from urllib2 import urlopen, Request, URLError, HTTPError
import logging
import uuid
import binascii
import threading
try:
	import timemodule as time
except ImportError:
	import time




class P2pMessageProducer(MessageProducer, _P2pBase):
	endpoint = None
	retries_progression = None
	_store = None
	_logger = None
	
	
	def __init__(self, **kwargs):
		MessageProducer.__init__(self)
		_P2pBase.__init__(self, **kwargs)
		self.endpoint = kwargs[P2pConfigOptions.PRODUCER_URL]
		self.retries_progression = configtool.split_array(kwargs[P2pConfigOptions.PRODUCER_RETRIES_PROGRESSION], ",")		
		self.next_try = 0
		self._logger = logging.getLogger(__name__)
		self._store = P2pMessageStore()
		
		self._send_event = threading.Event()
		self._sender_thread = threading.Thread(target=self._send_undelivered)
		self._sender_thread.daemon = True
		self._sender_thread.start()
	
	def _send_undelivered(self):
		while 1:
			for queue, message in self.get_undelivered():		
				try:
					xml = message.toxml()
					crypto_key = configtool.read_key(self.crypto_key_path)
					data = cryptotool.encrypt(xml, crypto_key)
					req = Request(self.endpoint + "/" + queue, data, {"X-Server-Id": self.server_id})
					urlopen(req)
					self._store.mark_as_delivered(message.id)
					self.fire("send", queue, message)
					self.next_try = 0			
				except IOError, e:
					if isinstance(e, HTTPError) and e.code == 201:
						self._store.mark_as_delivered(message.id)
						self.fire("send", queue, message)
						self.next_try = 0	
					else:
						if self.next_try < len(self.retries_progression):
							self.next_try += 1
						interval = self.next_interval()
						self._send_event.wait(interval)
						self._send_event.clear()
						break
			time.sleep(1)
			
	
	def send(self, queue, message):
		self._logger.info("Sending message '%s' into queue '%s'" % (message.name, queue))
		try:
			if message.id is None:
				message.id = str(uuid.uuid4())
						
			self._send_event.set()
				
			self.fire("before_send", queue, message)				
			self._store.put_outgoing(message, queue)
			
			# Prepare POST body
			xml = message.toxml()
			xml = xml.ljust(len(xml) + 8 - len(xml) % 8, " ")
			crypto_key = binascii.a2b_base64(configtool.read_key(self.crypto_key_path))
			data = cryptotool.encrypt(xml, crypto_key)
			
			signature, timestamp = cryptotool.sign_http_request(data, crypto_key)
			
			# Send request
			headers = {
				"Date": timestamp, 
				"X-Signature": signature, 
				"X-Server-Id": self.server_id
			}
			self._logger.debug("Date: " + timestamp)			
			self._logger.debug("X-Signature: " + signature)
			self._logger.debug("X-Server-Id: " + self.server_id)
			self._logger.debug("Payload: " + data)
			
			req = Request(self.endpoint + "/" + queue, data, headers)
			resp = urlopen(req)
			print resp.read()
			
			self._store.mark_as_delivered(message.id)
			self.fire("send", queue, message)
			
		except IOError, e:

			if isinstance(e, HTTPError) and e.code == 201:
				self._store.mark_as_delivered(message.id)
				self.fire("send", queue, message)
			else:
				self.fire("send_error", e, queue, message)
				self._logger.info("Mark message as undelivered")
				self._store.mark_as_undelivered(message.id)
			
				if isinstance(e, HTTPError):
					#print e
					if e.code == 401:
						raise MessagingError("Cannot authenticate on message server. %s" % e)
					
					elif e.code == 400:
						raise MessagingError("Malformed request. %s" % e)
					
					else:
						raise MessagingError("Request to message server failed. %s" % e)
				elif isinstance(e, URLError):
					host, port = splitnport(req.host, req.port)
					raise MessagingError("Cannot connect to message server on %s:%s. %s" % (host, port, e))
				else:
					raise MessagingError("Cannot read crypto key. %s" % e)		
					

	def get_undelivered(self):
		return self._store.get_undelivered()
	
	def next_interval(self):
		return int(self.retries_progression[self.next_try]) * 60.0
