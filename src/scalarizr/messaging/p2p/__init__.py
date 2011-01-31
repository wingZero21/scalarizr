'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.messaging import MessageService, Message, MetaOptions, MessagingError
from scalarizr.messaging.p2p.security import P2pMessageSecurity
import logging
import threading


"""
InFilter
OutFilter
"""

class P2pConfigOptions:
	SERVER_ID 						= "server_id"
	CRYPTO_KEY_PATH 				= "crypto_key_path"
	PRODUCER_URL 					= "producer_url"
	PRODUCER_RETRIES_PROGRESSION 	= "producer_retries_progression"
	PRODUCER_SENDER					= "producer_sender"	
	CONSUMER_URL 					= "consumer_url"


class P2pMessageService(MessageService):
	_params = {}
	_default_producer = None
	_default_consumer = None
	
	def __init__(self, **params):
		self._params = params
		self._security = P2pMessageSecurity(
			self._params[P2pConfigOptions.SERVER_ID],
			self._params[P2pConfigOptions.CRYPTO_KEY_PATH]
		)

	def new_message(self, name=None, meta=None, body=None):
		return P2pMessage(name, meta, body)
	
	def get_consumer(self):
		if not self._default_consumer:
			self._default_consumer = self.new_consumer(
				endpoint=self._params[P2pConfigOptions.CONSUMER_URL]
			)
		return self._default_consumer
	
	def new_consumer(self, **params):
		import consumer
		c = consumer.P2pMessageConsumer(**params)
		c.filters['protocol'].append(self._security.in_protocol_filter)
		return c
	
	def get_producer(self):
		if not self._default_producer:
			self._default_producer = self.new_producer(
				endpoint=self._params[P2pConfigOptions.PRODUCER_URL],
				retries_progression=self._params[P2pConfigOptions.PRODUCER_RETRIES_PROGRESSION],
			)
		return self._default_producer
	
	def new_producer(self, **params):
		import producer
		p = producer.P2pMessageProducer(**params)
		p.filters['protocol'].append(self._security.out_protocol_filter)
		return p
		

def new_service(**kwargs):
	return P2pMessageService(**kwargs)

class _P2pMessageStore:
	_logger = None	
	
	TAIL_LENGTH = 50	

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		ex = bus.periodical_executor
		if ex: 
			self._logger.debug('Add rotate messages table task for periodical executor')
			ex.add_task(self.rotate, 3600, 'Rotate messages sqlite table') # execute rotate task each hour

	def _conn(self):
		db = bus.db
		return db.get().get_connection()
		
	def rotate(self):
		conn = self._conn()
		cur = conn.cursor()
		cur.execute('SELECT * FROM p2p_message ORDER BY id DESC LIMIT %d, 1' % self.TAIL_LENGTH)
		row = cur.fetchone()
		if row:
			self._logger.debug('Deleting messages older then messageid: %s', row['message_id'])
			cur.execute('DELETE FROM p2p_message WHERE id <= ?', (row['id'],))
		conn.commit()
		
	def put_ingoing(self, message, queue, consumer_id):
		conn = self._conn()
		cur = conn.cursor()
		try:
			sql = """INSERT INTO p2p_message (id, message, message_id, 
						message_name, queue, is_ingoing, in_is_handled, in_consumer_id)
					VALUES 
						(NULL, ?, ?, ?, ?, ?, ?, ?)"""
			cur.execute(sql, [str(message), message.id, message.name, queue, 1, 0, consumer_id])
			
			if message.meta.has_key(MetaOptions.REQUEST_ID):
				cur.execute("""UPDATE p2p_message 
						SET response_uuid = ? WHERE message_id = ?""", 
						[message.id, message.meta[MetaOptions.REQUEST_ID]])

			self._logger.debug("Commiting put_ingoing")
			conn.commit()
			self._logger.debug("Commited put_ingoing")
		finally:
			cur.close()
			
	def get_unhandled(self, consumer_id):
		"""
		Return list of unhandled messages in obtaining order
		@return: [(queue, message), ...]   
		"""
		cur = self._conn().cursor()
		try:
			sql = """SELECT queue, message_id FROM p2p_message
					WHERE is_ingoing = ? AND in_is_handled = ? AND in_consumer_id = ? 
					ORDER BY id"""
			cur.execute(sql, [1, 0, consumer_id])
			ret = []
			for r in cur.fetchall():
				ret.append((r["queue"], self.load(r["message_id"], True)))
			return ret
		finally:
			cur.close()
	
	def mark_as_handled(self, message_id):
		conn = self._conn()
		cur = conn.cursor()
		try:
			sql = """UPDATE p2p_message SET in_is_handled = ? 
					WHERE message_id = ? AND is_ingoing = ?"""
			cur.execute(sql, [1, message_id, 1])

			self._logger.debug("Commiting mark_as_handled")
			conn.commit()
			self._logger.debug("Commited mark_as_handled")
		finally:
			cur.close()

	def put_outgoing(self, message, queue, sender):
		conn = self._conn()
		cur = conn.cursor()
		try:
			sql = """INSERT INTO p2p_message (id, message, message_id, message_name, queue, 
						is_ingoing, out_is_delivered, out_delivery_attempts, out_sender) 
					VALUES 
						(NULL, ?, ?, ?, ?, ?, ?, ?, ?)"""
			cur.execute(sql, [str(message), message.id, message.name, queue, 0, 0, 0, sender])
			
			self._logger.debug("Commiting put_outgoing")
			conn.commit()
			self._logger.debug("Commited put_outgoing")
		finally:
			cur.close()

			
	def get_undelivered (self, sender):
		"""
		Return list of undelivered messages in outgoing order
		"""
		cur = self._conn().cursor()
		try:
			sql = """SELECT queue, message_id FROM p2p_message
					WHERE is_ingoing = ? AND out_is_delivered = ? AND out_sender = ? ORDER BY id"""
			cur.execute(sql, [0, 0, sender])
			ret = []
			for r in cur.fetchall():
				ret.append((r[0], self.load(r[1], False)))
			return ret
		finally:
			cur.close()
			
	def mark_as_delivered(self, message_id):
		return self._mark_as_delivered(message_id, 1)
	

	def mark_as_undelivered(self, message_id):
		return self._mark_as_delivered(message_id, 0)

	def _mark_as_delivered (self, message_id, delivered):
		conn = self._conn()
		cur = conn.cursor()
		try:
			sql = """UPDATE p2p_message SET out_delivery_attempts = out_delivery_attempts + 1, 
						out_last_attempt_time = datetime('now'), out_is_delivered = ? 
					WHERE 
						message_id = ? AND is_ingoing = ?"""
			cur.execute(sql, [int(bool(delivered)), message_id, 0])
			conn.commit()
		finally:
			cur.close()
			
	def load(self, message_id, is_ingoing):
		cur = self._conn().cursor()
		try:
			cur.execute("""SELECT * FROM p2p_message 
					WHERE message_id = ? AND is_ingoing = ?""", 
					[message_id, int(bool(is_ingoing))])
			row = cur.fetchone()
			if not row is None:
				message = P2pMessage()
				self._unmarshall(message, row)
				return message
			else:
				raise MessagingError("Cannot find message (message_id: %s)" % (message_id))
		finally:
			cur.close()
	
	def is_handled(self, message_id):
		cur = self._conn().cursor()
		try:
			cur.execute("""SELECT in_is_handled FROM p2p_message 
					WHERE message_id = ? AND is_ingoing = 1""", 
					[message_id])
			return cur.fetchone()["in_is_handled"] == 1
		finally:
			cur.close()
	
	def is_delivered(self, message_id):
		cur = self._conn().cursor()
		try:
			cur.execute("""SELECT is_delivered FROM p2p_message
					"WHERE message_id = ? AND is_ingoing = ?""", 
					[message_id, 0])
			return cur.fetchone()["out_is_delivered"] == 1
		finally:
			cur.close()
		
	def is_response_received(self, message_id):
		cur = self._conn().cursor()
		try:
			sql = """SELECT response_id FROM p2p_message 
					WHERE message_id = ? AND is_ingoing = ?"""
			cur.execute(sql, [message_id, 0])
			return cur.fetchone()["response_id"] != ""
		finally:
			cur.close()
		
	def get_response(self, message_id):
		cur = self._conn().cursor()
		try:
			cur.execute("""SELECT response_id FROM p2p_message 
					WHERE message_id = ? AND is_ingoing = ?""", 
					[message_id, 0])
			response_id = cur.fetchone()["response_id"]
			if not response_id is None:
				return self.load(response_id, True)
			return None
		finally:
			cur.close()
		
	def _unmarshall(self, message, row):
		message.fromxml(row["message"])
		
	def _marshall(self, message, row={}):
		row["message_id"] = message.id
		row["message_name"] = message.name
		row["message"] = str(message)
		return row

_message_store = None
def P2pMessageStore():
	global _message_store
	if _message_store is None:
		_message_store = _P2pMessageStore()
	return _message_store


class P2pMessage(Message):

	def __init__(self, name=None, meta=None, body=None):
		Message.__init__(self, name, meta, body)
		self.__dict__["_store"] = P2pMessageStore()
		if bus.cnf:
			cnf = bus.cnf; ini = cnf.rawini
			self.meta[MetaOptions.SERVER_ID] = ini.get('general', 'server_id')
	
	def is_handled(self):
		return self._store.is_handled(self.id)
	
	def is_delivered(self):
		return self._store.is_delivered(self.id)
	
	def is_responce_received(self):
		return self._store.is_response_received(self.id)
		
	def get_response(self):
		return self._store.get_response(self.id)
