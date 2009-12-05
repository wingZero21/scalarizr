'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.messaging import MessageService, Message, MessagingError


class P2pOptions:
	pass

class P2pMessageService(MessageService):
	def __init__(self, config):
		pass
	
	
class _P2pMessageStore:
	def __init__(self):
		self._db = Bus()[BusEntries.DB]
		
	def load_message(self, messageid):
		cur = self._db.cursor()
		cur.execute("SELECT * FROM p2p_message WHERE messageid = ?", messageid)
		if (cur.rowcount > 0):
			row = cur.fetchone()
			message = P2pMessage()
			self._unmarshall_message(message, row)
			return message
		else:
			raise MessagingError("Cannot find message (messageid: %s)" % (messageid))
		
	def save_message(self, message):
		pass
	
	def is_message_delivered(self, messageid):
		cur = self._db.cursor()
		cur.execute("SELECT is_delivered FROM p2p_message WHERE messageid = ?", messageid)
		return cur.rowcount > 0 and cur.fetchone()["is_delivered"] == 1
		
	def is_response_message_received(self, messageid):
		cur = self._db.cursor()
		cur.execute("SELECT response_messageid FROM p2p_message WHERE messageid = ?", messageid)
		return cur.rowcount > 0 and cur.fetchone()["response_messageid"] != ""
		
	def get_response_message(self, messageid):
		cur = self._db.cursor()
		cur.execute("SELECT response_messageid FROM p2p_message WHERE messageid = ?", messageid)
		if cur.rowcount:
			response_messageid = cur.fetchone()["response_messageid"]
			if response_messageid:
				return self.load_message(response_messageid)
		return None
		
	def _unmarshall_message(self, message, row):
		message.fromxml(row["message"])
		
	def _marshall_message(self, message, row={}):
		row["messageid"] = message.id
		row["message_name"] = message.name
		row["message"] = str(message)
		return row

_message_store_instance = None
def P2pMessageStore():
	if _message_store_instance in None:
		_message_store_instance = _P2pMessageStore()
	return _message_store_instance

def P2pMessage(Message):
	def __init__(self):
		self._store = P2pMessageStore()
	
	def is_delivered(self):
		return self._store.is_message_delivered(self.id)
	
	def is_responce_received(self):
		return self._store.is_response_message_received(self.id)
		
	def get_response(self):
		return self._store.get_response_message(self.id)
