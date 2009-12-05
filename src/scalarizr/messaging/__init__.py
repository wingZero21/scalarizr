
class MessagingError(Exception):
	pass

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
	
	def __init__(self, name=None, meta={}, body={}):
		self.name = name
		self.meta = meta
		self.body = body
	
	def is_delivered(self):
		pass
	
	def is_responce_received(self):
		pass
		
	def get_response(self):
		pass
	
	def fromxml (self, xml):
		from xml.dom.minidom import parseString
		doc = parseString(xml)
		
		root = doc.documentElement
		self.id = root.getAttribute("id")
		self.name = root.getAttribute("name")
		
		for node in root.firstChild.childNodes:
			self.meta[node.getAttribute("name")] = node.firstChild.nodeValue
			
		for node in root.childNodes[1].childNodes:
			self.body[node.getAttribute("name")] = node.firstChild.nodeValue
	
	def __str__(self):
		from xml.dom.minidom import getDOMImplementation
		impl = getDOMImplementation()
		doc = impl.createDocument(None, "message", None)
		
		root = doc.documentElement;
		root.setAttribute("id", self.id)
		root.setAttribute("name", self.name)
		
		meta = doc.createElement("meta")
		for k in self.meta.keys():
			item = doc.createElement("item")
			item.setAttribute("name", k)
			item.appendChild(doc.createTextNode(self.meta[k]))
			meta.appendChild(item)
		root.appendChild(meta)
		
		body = doc.createElement("body")
		for k in self.body.keys():
			item = doc.createElement("item")
			item.setAttribute("name", k)
			item.appendChild(doc.createTextNode(self.body[k]))
			body.appendChild(item)
		root.appendChild(body)
			
		return doc.toxml()
		
	
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
	