
class MessagingError(BaseException):
	pass

class MessageServiceFactory(object):
	_adapters = {}
	
	def new_service (self, name, config):
		if not self._adapters.has_key(name):
			adapter =  __import__("scalarizr.messaging." + name, 
					globals(), locals(), ["new_service"])
			self._adapters[name] = adapter
		return self._adapters[name].new_service(config)

class MessageService(object):
	def new_message(self, name=None):
		pass
	
	def new_consumer(self):
		pass
	
	def new_producer(self):
		pass
	
class MetaOptions(object):
	SERVER_TYPE = "serverType"
	OS_NAME 	= "osName"
	OS_VERSION 	= "osVersion"
	REQUEST_ID 	= "requestId"
	
class Message(object):
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
	
	def toxml (self):
		return str(self)
	
	def __str__(self):
		from xml.dom.minidom import getDOMImplementation
		impl = getDOMImplementation()
		doc = impl.createDocument(None, "message", None)
		
		root = doc.documentElement;
		root.setAttribute("id", str(self.id))
		root.setAttribute("name", str(self.name))
		
		meta = doc.createElement("meta")
		for k in self.meta.keys():
			item = doc.createElement("item")
			item.setAttribute("name", str(k))
			item.appendChild(doc.createTextNode(str(self.meta[k])))
			meta.appendChild(item)
		root.appendChild(meta)
		
		body = doc.createElement("body")
		for k in self.body.keys():
			item = doc.createElement("item")
			item.setAttribute("name", str(k))
			item.appendChild(doc.createTextNode(str(self.body[k])))
			body.appendChild(item)
		root.appendChild(body)
			
		return doc.toxml()
		
	
class MessageProducer(object):
	def send(self, queue, message):
		pass
	
class MessageConsumer(object):
	_listeners = []
	
	def add_message_listener(self, ln):
		if not ln in self._listeners:
			self._listeners.append(ln)
			
	def remove_message_listener(self, ln):
		if ln in self._listeners:
			self._listeners.remove(ln)
	
	def start(self):
		pass
	
	def stop(self):
		pass
	