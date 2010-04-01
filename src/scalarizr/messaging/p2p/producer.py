'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.messaging import MessageProducer, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, P2pOptions, _P2pBase
from scalarizr.util import CryptoTool
from urllib import splitnport
from urllib2 import urlopen, Request, URLError, HTTPError
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
				
			self.fire("before_send", queue, message)				
			self._store.put_outgoing(message, queue)
			
			# Prepare POST body
			xml = message.toxml()
			#xml = xml.ljust(len(xml) + 8 - len(xml) % 8, " ")
			crypto = CryptoTool()
			data = crypto.encrypt(xml, self._read_key())
			
			# Send request
			req = Request(self.endpoint + "/" + queue, data, {"X-Server-Id": self._server_id})
			urlopen(req)
			
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
					resp_body = e.read() if not e.fp is None else ""
					if e.code == 401:
						raise MessagingError("Cannot authenticate on message server. %s" % (resp_body))
					
					elif e.code == 400:
						raise MessagingError("Malformed request. %s" % (resp_body))
					
					else:
						raise MessagingError("Request to message server failed (code: %d). %s" % (e.code, str(e)))
				elif isinstance(e, URLError):
					host, port = splitnport(req.host, req.port)
					raise MessagingError("Cannot connect to message server on %s:%s. %s" % (host, port, str(e)))
				else:
					raise MessagingError("Cannot read crypto key. %s" % str(e))		
					

	def get_undelivered(self):
		return self._store.get_undelivered()
