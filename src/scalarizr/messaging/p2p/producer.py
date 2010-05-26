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
		self.interval = None
		self._logger = logging.getLogger(__name__)
		self._store = P2pMessageStore()
		
		self._send_event = threading.Event()
		self._sender_thread = threading.Thread(target=self._send_undelivered)
		self._sender_thread.daemon = True
		self._sender_thread.start()		
	
	def _send_undelivered(self):

		def message_delivered(message, queue):
			self._store.mark_as_delivered(message.id)
			self.fire("send", queue, message)
			self.next_try = 0
			self.interval = None
		
		def set_next_interval():
			self.interval = self.get_next_interval()
			if self.next_try < len(self.retries_progression):
				self.next_try += 1
				
		while 1:
			try:
				self._send_event.wait(self.interval)
				self._send_event.clear()
				for queue, message in self.get_undelivered():
					self._logger.debug("Fetch undelivered message '%s' (id: %s)", message.name, message.id)		
					try:
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
						
						url = self.endpoint + "/" + queue
						req = Request(url, data, headers)
						resp = urlopen(req)
						
						message_delivered(message, queue)
						self._logger.debug("Mark delivered '%s' (id: %s)", message.name, message.id)
					
					
					except HTTPError, e:
						if e.code == 201:
							message_delivered(message, queue)
						else:
							self._logger.info("Mark message as undelivered")
							self.fire("send_error", e, queue, message)
							resp_body = e.read() if not e.fp is None else ""
							
							if e.code == 401:
								self._logger.error("Cannot authenticate on message server. %s", resp_body)
							elif e.code == 400:
								self._logger.error("Malformed request. %s", resp_body)	
							else:
								self._logger.error("Cannot post message to %s. %s", url, e)
							set_next_interval()
							break
					except URLError,e:
						host, port = splitnport(req.host, req.port)
						self._logger.error("Cannot connect to message server on %s:%s. %s", host, port, e)
						set_next_interval()
						break
					except (Exception, BaseException), e:	
						self._logger.error("Cannot read crypto key. %s",  e)
						set_next_interval()
						break
			except (Exception, BaseException), e:
				self._logger.error("Unable to read from database. %s", e)
	
	def send(self, queue, message):
		self._logger.info("Sending message '%s' into queue '%s'", message.name, queue)

		if message.id is None:
			message.id = str(uuid.uuid4())
		self.fire("before_send", queue, message)				
		self._store.put_outgoing(message, queue)
		self._send_event.set()


	def get_undelivered(self):
		return self._store.get_undelivered()
	
	def get_next_interval(self):
		return int(self.retries_progression[self.next_try]) * 60.0
