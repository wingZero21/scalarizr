'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.messaging import MessageProducer, Message, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, P2pOptions, _P2pBase
from scalarizr.util import CryptoUtil
from urllib2 import *
import logging
import uuid

class P2pMessageProducer(MessageProducer, _P2pBase):
	endpoint = ""
	_store = None
	_logger = None
	
	
	def __init__(self, config):
		MessageProducer.__init__(self)
		_P2pBase.__init__(self, config)
		for pair in config:
			key = pair[0]
			if key == P2pOptions.PRODUCER_ENDPOINT:
				self.endpoint = pair[1]
	
		self._logger = logging.getLogger(__name__)
		self._store = P2pMessageStore()
	
	def send(self, queue, message):
		self._logger.info("Sending message '%s' into queue '%s'" % (message.name, queue))
		try:
			if message.id is None:
				message.id = str(uuid.uuid4())
				
			self.fire("beforesend", queue, message)				
			self._store.put_outgoing(message, queue)
			
			# Prepare POST body
			xml = message.toxml()
			xml = xml.ljust(len(xml) + 8 - len(xml) % 8, " ")
			data = CryptoUtil().encrypt(xml, self._crypto_key)
			
			# Send request
			req = Request(self.endpoint + "/" + queue, data, {"X-Server-Id": self._server_id})
			resp = urlopen(req)
			
			self._store.mark_as_delivered(message.id)
			self.fire("send", queue, message)
			
		except URLError, e:
			self.fire("senderror", e, queue, message)
			self._logger.exception(e)
			self._logger.info("mark as undelivered")
			self._store.mark_as_undelivered(message.id)
			
			if isinstance(e, HTTPError):
				raise MessagingError("Cannot connect to message server. %s" % (str(e)))				
			else:
				print e.__dict__
				if e.code == 401:
					raise MessagingError("Cannot authenticate on message server. %s" % (e.read()))
				elif e.code == 400:
					raise MessagingError("Malformed request. %s" % (e.read()))
				else:
					raise MessagingError("Request to message server failed. %s" % (str(e)))

	def get_undelivered(self):
		return self._store.get_undelivered()
