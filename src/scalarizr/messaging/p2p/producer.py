'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.messaging import MessageProducer, Message, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore
from urllib2 import *
import logging
import uuid

class P2pMessageProducer(MessageProducer):
	endpoint = ""
	_store = None
	_logger = None
	
	
	def __init__(self, config):
		for pair in config:
			key = pair[0]
			if key == "p2p.producer.endpoint":
				self.endpoint = pair[1]
				
		self._logger = logging.getLogger(__package__)
		self._store = P2pMessageStore()
	
	def send(self, queue, message):
		self._logger.info("Sending message '%s' into queue '%s'" % (message.name, queue))
		try:
			if message.id is None:
				message.id = str(uuid.uuid4())
				self._store.put_outgoing(message, queue)
				
			r = urlopen(self.endpoint + "/" + queue, message.toxml())
			response = r.read()
			self._store.mark_as_delivered(message.id)
			
		except HTTPError, e:
			self._logger.exception(e)
			self._store.mark_as_undelivered(message.id)
			
			if isinstance(e, HTTPError):
				raise MessagingError("Cannot connect to message server. %s" % (str(e)))				
			else:
				if e.code == 401:
					raise MessagingError("Cannot authenticate on message server. %s" % (e.read()))
				elif e.code == 400:
					raise MessagingError("Malformed request. %s" % (e.read()))
				else:
					raise MessagingError("Request to message server failed. code: %d, body: %s" \
							% (e.code, e.read()))

	def get_undelivered(self):
		return self._store.get_undelivered()
