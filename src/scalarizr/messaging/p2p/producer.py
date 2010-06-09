'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.messaging import MessageProducer
from scalarizr.messaging.p2p import P2pMessageStore, _P2pBase, P2pConfigOptions,\
	P2pSender
from scalarizr.util import cryptotool, configtool
from urllib import splitnport
from urllib2 import urlopen, Request, URLError, HTTPError
import logging
import uuid
import binascii
import threading


class P2pMessageProducer(MessageProducer, _P2pBase):
	endpoint = None
	retries_progression = None
	sender = None
	_store = None
	_logger = None
	
	def __init__(self, **kwargs):
		MessageProducer.__init__(self)
		_P2pBase.__init__(self, **kwargs)
		
		self.endpoint = kwargs[P2pConfigOptions.PRODUCER_URL]
		self.retries_progression = configtool.split_array(
				kwargs[P2pConfigOptions.PRODUCER_RETRIES_PROGRESSION], ",")
		self.sender = kwargs[P2pConfigOptions.PRODUCER_SENDER]
				
		self._next_retry_index = 0
		self._interval = None
		self._logger = logging.getLogger(__name__)
		self._store = P2pMessageStore()
		
		self._send_event = threading.Event()
		self._delivery_thread = threading.Thread(target=self._send_undelivered)
		self._delivery_thread.daemon = True
		self._delivery_started = False
		self._delivery_lock = threading.Lock()
	
	
	def send(self, queue, message):
		# TODO: Implement blocking. Thread will block until it deliver message
		self._logger.info("Sending message '%s' into queue '%s'", message.name, queue)

		if message.id is None:
			message.id = str(uuid.uuid4())
		self.fire("before_send", queue, message)				
		self._store.put_outgoing(message, queue, self.sender)
		
		"""
		if self.sender == P2pSender.DAEMON:
			self._delivery_lock.acquire()
			if not self._delivery_started:
				self._delivery_started = True
				self._delivery_lock.release()				
				self._delivery_thread.start()
			self._send_event.set()
		else:
			self._send0(queue, message)
		"""	
		self._send0(queue, message)
	
	
	def _send_undelivered(self):
		"""
		Delivery thread target
		"""
		
		def _delivered(queue, message):
			self._next_retry_index = 0
			self._interval = None
		
		def _undelivered(queue, message):
			self._interval = self._get_next_interval()
			if self._next_retry_index < len(self.retries_progression):
				self._next_retry_index += 1
		
		self._logger.info("Starting message delivery thread")		
		while 1:
			try:
				self._logger.debug("Wait %s seconds to continue delivering messages", self._interval)
				self._send_event.wait(self._interval)
				self._send_event.clear()
				for queue, message in self._store.get_undelivered(P2pSender.DAEMON):
					self._logger.debug("Fetch undelivered message '%s' (id: %s)", message.name, message.id)
					self._send0(queue, message, _delivered, _undelivered)

			except (Exception, BaseException), e:
				self._logger.error("Unable to read from database. %s", e)
				self._logger.exception(e)


	def _get_next_interval(self):
		return int(self.retries_progression[self._next_retry_index]) * 60.0


	def _send0(self, queue, message, success_callback=None, fail_callback=None):
		try:
			# Serialize
			xml = message.toxml()
			xml = xml.ljust(len(xml) + 8 - len(xml) % 8, " ")
			self._logger.debug("Delivering message '%s' %s", message.name, xml)
			
			# Crypt
			crypto_key = binascii.a2b_base64(configtool.read_key(self.crypto_key_path))
			data = cryptotool.encrypt(xml, crypto_key)
			
			# Sign
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
			
			self._message_delivered(queue, message, success_callback)
		
		except (Exception, BaseException), e:
			# Python < 2.6 raise exception on 2xx http codes 
			if isinstance(e, HTTPError):
				if e.code == 201:
					self._message_delivered(queue, message, success_callback)
					return

			self._logger.warning("Message '%s' not delivered (message_id: %s)", message.name, message.id)
			self.fire("send_error", e, queue, message)
				
			if isinstance(e, HTTPError):
				if e.code == 401:
					self._logger.error("Cannot authenticate on message server. %s", e.msg)
				elif e.code == 400:
					self._logger.error("Malformed request. %s", e.msg)	
				else:
					self._logger.error("Cannot post message to %s. %s", url, e)
						
			elif isinstance(e, URLError):
				host, port = splitnport(req.host, req.port)
				self._logger.error("Cannot connect to message server on %s:%s. %s", host, port, e)
				
			else:
				self._logger.exception(e)
			
			# Call user code
			if fail_callback:
				fail_callback(queue, message)


	def _message_delivered(self, queue, message, callback=None):
		self._logger.info("Message '%s' delivered (message_id: %s)", message.name, message.id)		
		self._store.mark_as_delivered(message.id)
		self.fire("send", queue, message)
		if callback:
			callback(queue, message)

