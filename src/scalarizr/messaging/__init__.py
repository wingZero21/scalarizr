
class MessageServiceFactory:
	def __init__(self):
		pass
	
	def new_service (self, name, config):
		pass

	
class MessageService:
	def new_message(self, name):
		pass
	
	def new_consumer(self):
		pass
	
	def new_producer(self):
		pass
	
	
class Message:
	id = None	
	name = None
	meta = {}	
	body = {}
	
	def is_delivered(self):
		pass
	
	def is_responce_received(self):
		pass
		
	def get_response(self):
		pass
	
	def __str__(self):
		return ""
		
	
class MessageProducer:
	def send(self, message):
		pass
	
class MessageConsumer:
	def add_message_listener(self, ln):
		"""
		Extend from observable?
		"""
		pass
	
	def start(self):
		pass
	
	def stop(self):
		pass
	