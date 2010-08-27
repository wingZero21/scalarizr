'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.messaging import MessageProducer, Queues
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
	sender = None
	_store = None
	_logger = None
	_stop_delivery = None
	
	def __init__(self, **kwargs):
		MessageProducer.__init__(self)
		_P2pBase.__init__(self, **kwargs)
		
		self.endpoint = kwargs[P2pConfigOptions.PRODUCER_URL]
		self.retries_progression = configtool.split_array(
				kwargs[P2pConfigOptions.PRODUCER_RETRIES_PROGRESSION], ",")
		self.sender = kwargs[P2pConfigOptions.PRODUCER_SENDER]
				
		self._logger = logging.getLogger(__name__)
		self._store = P2pMessageStore()
		self._stop_delivery = threading.Event()
		
		self._local = threading.local()
		self._local_defaults = dict(interval=None, next_retry_index=0, delivered=False)

	def shutdown(self):
		self._stop_delivery.set()
	
	def send(self, queue, message):
		self._logger.debug("Sending message '%s' into queue '%s'", message.name, queue)

		if message.id is None:
			message.id = str(uuid.uuid4())
		self.fire("before_send", queue, message)
		self._store.put_outgoing(message, queue, self.sender)
		
		if not hasattr(self._local, "interval"):
			for k, v in self._local_defaults.items():
				setattr(self._local, k, v)
				
		self._local.delivered = False
		#while not self._local.delivered and not self._stop_delivery.isSet():
		while not self._local.delivered:			
			if self._local.interval:
				self._logger.debug("Sleep %d seconds before next attempt", self._local.interval)
				time.sleep(self._local.interval)
				# FIXME: SIGINT hanged
				# strace:
				# --- SIGINT (Interrupt) @ 0 (0) ---
				# rt_sigaction(SIGINT, {0x36b9210, [], 0}, {0x36b9210, [], 0}, 8) = 0
				# sigreturn()                             = ? (mask now [])
				
				# futex(0xa3f5d78, FUTEX_WAIT_PRIVATE, 0, NUL
			#if not self._stop_delivery.isSet():
			#	self._send0(queue, message, self._delivered_cb, self._undelivered_cb)
			self._send0(queue, message, self._delivered_cb, self._undelivered_cb)
				


	def _delivered_cb(self, queue, message):
		self._local.next_retry_index = 0
		self._local.interval = None
		self._local.delivered = True
	
	def _undelivered_cb(self, queue, message):
		self._local.interval = self._get_next_interval()
		if self._local.next_retry_index < len(self.retries_progression):
			self._local.next_retry_index += 1	

	def _get_next_interval(self):
		return int(self.retries_progression[self._local.next_retry_index]) * 60.0

	def _send0(self, queue, message, success_callback=None, fail_callback=None):
		try:
			# Serialize
			xml = message.toxml()
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
			urlopen(req)
			
			self._message_delivered(queue, message, success_callback)
		
		except (Exception, BaseException), e:
			# Python < 2.6 raise exception on 2xx > 200 http codes except
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
				host, port = splitnport(req.host, req.port or 80)
				self._logger.error("Cannot connect to message server on %s:%s. %s", host, port, e)
				
			else:
				self._logger.exception(e)
			
			# Call user code
			if fail_callback:
				fail_callback(queue, message)


	def _message_delivered(self, queue, message, callback=None):
		self._logger.log(queue == Queues.LOG and logging.DEBUG or logging.INFO, 
				"Message '%s' delivered (message_id: %s)", message.name, message.id)
		self._store.mark_as_delivered(message.id)
		self.fire("send", queue, message)
		if callback:
			callback(queue, message)

