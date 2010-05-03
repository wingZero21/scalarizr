from scalarizr.util import Observable, xml_strip
import xml.dom.minidom as dom

class MessagingError(BaseException):
	pass

class MessageServiceFactory(object):
	_adapters = {}
	
	def new_service (self, name, **kwargs):
		if not self._adapters.has_key(name):
			adapter =  __import__("scalarizr.messaging." + name, 
					globals(), locals(), ["new_service"])
			self._adapters[name] = adapter
		return self._adapters[name].new_service(**kwargs)

class MessageService(object):
	def new_message(self, name=None, meta={}, body={}):
		pass
	
	def get_consumer(self):
		pass
	
	def get_producer(self):
		pass
	
class MetaOptions(object):
	SERVER_ID 	= "server_id"
	PLATFORM 	= "platform" # ec2, vps, rs
	OS 			= "os" # linux, win, sunos
	REQUEST_ID 	= "request_id"
	
class Message(object):
	
	def __init__(self, name=None, meta={}, body={}):
		self.__dict__["id"] = None
		self.__dict__["name"] = name
		self.__dict__["meta"] = meta
		self.__dict__["body"] = body
	
	def __setattr__(self, name, value):
		if name in self.__dict__:
			self.__dict__[name] = value
		else:
			self.body[name] = value
		
	def __getattr__(self, name):
		return self.body[name] if name in self.body else None
	
	def is_handled(self):
		pass
	
	def is_delivered(self):
		pass
	
	def is_responce_received(self):
		pass
		
	def get_response(self):
		pass
	
	def fromxml (self, xml):
		doc = dom.parseString(xml)
		xml_strip(doc)
		
		root = doc.documentElement
		self.id = root.getAttribute("id")
		self.name = root.getAttribute("name")
		
		self._walk_decode(self.meta, root.firstChild)
		self._walk_decode(self.body, root.childNodes[1])


	def _walk_decode(self, var, el):
		# FIXME: nested elements doesn't supported
		for childEl in el.childNodes:
			var[childEl.nodeName] = childEl.firstChild.nodeValue
	
	def __str__(self):
		from xml.dom.minidom import getDOMImplementation
		impl = getDOMImplementation()
		doc = impl.createDocument(None, "message", None)
		
		root = doc.documentElement;
		root.setAttribute("id", str(self.id))
		root.setAttribute("name", str(self.name))
		
		meta = doc.createElement("meta")
		root.appendChild(meta)
		self._walk_encode(self.meta, meta, doc)
		
		body = doc.createElement("body")
		root.appendChild(body)
		self._walk_encode(self.body, body, doc)
			
		return doc.toxml()
	
	toxml = __str__
		
	def _walk_encode(self, value, el, doc):
		if getattr(value, '__iter__', False):
			if getattr(value, "keys", False):
				for k, v in value.items():
					itemEl = doc.createElement(str(k))
					el.appendChild(itemEl)
					self._walk_encode(v, itemEl, doc)
			else:
				for v in value:
					itemEl = doc.createElement(el.nodeName)
					el.parentNode.appendChild(itemEl)
					self._walk_encode(v, itemEl, doc)
				el.parentNode.removeChild(el)
		else:	
			el.appendChild(doc.createTextNode(str(value)))


class MessageProducer(Observable):
	def __init__(self):
		Observable.__init__(self)
		self.define_events(
			# Fires before message is send
			"before_send", 
			# Fires after message is send
			"send",
			# Fires when error occures
			"send_error"
		)
	
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
	
class Queues:
	CONTROL = "control"
	LOG = "log"
	
class Messages:
	###
	# Scalarizr events
	###
	
	HOST_INIT = "HostInit"
	"""
	Fires when scalarizr is initialized and ready to be configured 
	"""

	HOST_HALT = "Go2Halt"
	"""
	Fires when scalarizr is going to halt
	"""
	
	REBOOT_START = "RebootStart"
	"""
	Fires when scalarizr is going to reboot
	"""
	
	REBOOT_FINISH = "RebootFinish"
	"""
	Fires when scalarizr is resumed after reboot
	"""

	BLOCK_DEVICE_ATTACHED = "BlockDeviceAttached"
	"""
	Fires when block device was attached
	"""
	
	BLOCK_DEVICE_DETACHED = "BlockDeviceDetached"
	"""
	Fires when block device was detached
	"""
	
	EXEC_RESULT = "ExecResult"
	"""
	Fires after script execution
	"""
	
	###
	# Scalr events
	###
	
	HOST_UP = "HostUp"
	"""
	Fired by Scalr when farm is enriched with new server
	"""
	
	HOST_DOWN = "HostDown"
	"""
	Fired by Scalr when one of the farm servers is terminated
	"""
	
	EVENT_NOTICE = "EventNotice"
	"""
	Fired by Scalr when event occurred on one of the farm servers  
	"""
	
	VHOST_RECONFIGURE = "VhostReconfigure"

	HOST_INIT_RESPONSE = "HostInitResponse"
	
	REBUNDLE = "Rebundle"
	
	###
	# Scripts events
	###
	
	BLOCK_DEVICE_UPDATED = "BlockDeviceUpdated"
	"""
	Fired by scripts/udev.py when block device was added/updated/removed 
	"""
	
	SERVER_REBOOT = "ServerReboot"
	"""
	Fired by scripts/reboot.py when server is going to reboot
	"""
	
	SERVER_HALT = "ServerHalt"
	"""
	Fired by scripts/halt.py when server is going to halt
	"""
