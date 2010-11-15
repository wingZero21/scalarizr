'''
Created on Jun 29, 2010

A cute library to read and write configurations in a various formats 
using single interface.
Primary goal: support Ini, Xml, Yaml, ProtocolBuffers, Nginx, Apache2

@author: marat
@author: spike
'''

import sys
if sys.version_info[0:2] >= (2, 7):
	from xml.etree import ElementTree as ET 
else:
	from scalarizr.externals.etree import ElementTree as ET
import re
import os
try:
	from  cStringIO import StringIO
except ImportError:
	from StringIO import StringIO



format_providers = dict()
default_format = "ini"
	
class MetaconfError(Exception):
	pass

class ParseError(BaseException):
	"""
	Throw it in providers read method
	"""
	def __init__(self, errors):
		self._err = "File contains parsing errors: "
		for error in errors:
			self._err += '\n\tNo: %d,\tLine: %s' % error
		
	def __str__(self):
		return self._err

class NoPathError(MetaconfError):
	pass


class Configuration:
	etree = None
	"""
	@ivar xml.etree.ElementTree.ElementTree etree:  
	"""

	_root_path = None
	"""
	@ivar str _root_path: 
	"""
	
	_format = None
	"""
	@ivar str _format: 
	"""
	
	_provider = None

	def __init__(self, format=default_format, root_path="", etree=None):
		
		if etree and not isinstance(etree, ET.ElementTree):
			raise MetaconfError("etree param must be instance of ElementTree. %s passed" % (etree,))
		
		if not format_providers.has_key(format):
			raise MetaconfError("Unknown format: %s" % format)
		
		self._root_path = quote(root_path)
		self._format = format
		self.etree = etree
		self._config_count = 0
	
	def _init(self):
		if not self._provider or self._config_count > 0:
			self._provider = format_providers[self._format]()
		if not self.etree:
			root = ET.Element("mc_conf/")
			self.etree = ET.ElementTree(root)
		self._sections = []
		self._cursect = '.'
	
	def read(self, filenames):
		if isinstance(filenames, basestring):
			self._read0(filenames)
		else:
			for file in filenames:
				self._read0(file)
		if self.etree:
			indent(self.etree.getroot())

	
	def _read0(self, file):
		fp = open(file)
		self.readfp(fp)


		
	def readfp(self, fp):
		self._init()
		nodes = self._provider.read(fp)
		if self._config_count > 0:
			for node in nodes:
				self._extend(node)			
		else:
			root = self.etree.getroot()
			for node in nodes:
				root.append(node)
		self._config_count += 1	
		fp.close()
		"""
		self._init()
		for child in :
			self.etree.getroot().append(child)
		"""
			
	def write_fp(self, fp, close = True):
		"""
		Writes configuration to fp with provider's method 'write'.
		If 'close' parameter passed with 'False' value, fp won't be closed.
		"""
		if not self.etree or self.etree.getroot() == None:
			raise MetaconfError("Nothing to write! Create the tree first (readfp or read)")
		self._provider.write(fp, self.etree, close)
		
	def write(self, file_path):
		if not self.etree or self.etree.getroot() == None:
			raise MetaconfError("Nothing to write! Create the tree first (readfp or read)")
		
		tmp_str = StringIO()
		try:
			self._provider.write(tmp_str, self.etree, close = False)
			fp = open(file_path, 'w')
			fp.write(tmp_str.getvalue())
			fp.close()
		except:
			raise
	
	def extend(self, conf):
		"""
		Extend self with options from another config
		Comments and blank lines from importing config will not be added
		"""
		self._init()	
		for node in conf.etree.getroot():
			self._extend(node)
			
	def comment(self, path):
		"""
		Comment part of the configuration (one option or subtree)
		"""
		temp_nodes = self._find_all(path)
		
		path = quote(path)
		if not temp_nodes:
			return

		parent_els	= self._find_all(os.path.join(path,'..'))

		for temp_node in temp_nodes:
			comment_value = StringIO()
			temp_root	= ET.Element('mc_conf')
			temp_tree	= ET.ElementTree(temp_root)
			temp_root.append(temp_node)
			new_conf	= Configuration(format=self._format, etree=temp_tree)
			new_conf._init()
			new_conf.write_fp(comment_value, close = False)
			parent_el   = parent_els.pop(0)
			index = list(parent_el).index(temp_node)
			comment		= ET.Comment(comment_value.getvalue().strip())
			parent_el.insert(index, comment)
			parent_el.remove(temp_node)

	def uncomment(self, path):
		"""
		Try to find appropriate configuration piece in comments on path's level,
		And then uncomment it		
		"""
		path = quote(path)
		parent_path = os.path.dirname(path)
		el_name = os.path.basename(path)
		temp_nodes = self._find_all(parent_path)
		
		if not temp_nodes:
			raise MetaconfError("Path %s doesn't exist" % unquote(path))
		for temp_node in temp_nodes:
			for child in temp_node:

				if not callable(child.tag):
					continue
				temp_conf = Configuration(self._format)

				try:
					temp_conf.readfp(StringIO(child.text.strip()))
				except:
					continue

				comment_node = temp_conf.etree.find(el_name)

				if comment_node == None:
					continue

				temp_node.insert(list(temp_node).index(child), comment_node)
				temp_node.remove(child)
				del(temp_conf)

	def _extend(self, node):
		if not callable(node.tag) and node.tag != '':
			cursect = self._cursect + '/' + node.tag
			exist_list = self.etree.findall(cursect)
			if exist_list:
				if len(exist_list) == 1 and exist_list[0].attrib == node.attrib:
					if len(exist_list[0]):
						self._sections.append(self._cursect)
						self._cursect  = cursect
						for child in node:
							self._extend(child)
						self._cursect  = self._sections.pop()
					else:
						if exist_list[0].attrib == node.attrib:
							self._add_element(cursect, self._cursect, node)
							self.etree.find(self._cursect).remove(exist_list[0])
							
						#if node.text != exist_list[0].text or (bool(exist_list[0].attrib or node.attrib) ^ (exist_list[0].attrib != node.attrib)):
						#	self._add_element(cursect, self._cursect, node)
				else:
					equal = 0
					for exist_node in exist_list:
						equal += 0 if not self._compare_tree(exist_node, node) else 1
					if not equal:
						self._add_element(cursect, self._cursect, node)
			else:
				self.etree.find(self._cursect).append(node)
				
	def _compare_tree(self, first, second):
		
		if first.text and second.text and first.text.strip() != second.text.strip():
			return False
			
		if first.attrib != second.attrib:
			return False
		
		first_childs = list(first)
		second_childs = list(second)
		
		if first_childs and second_childs:
			if len(first_childs) != len(second_childs):
				return False
			
			comparison = 0
			for f_child in first_childs:
				for s_child in second_childs:
					comparison += 0 if not self._compare_tree(f_child, s_child) else 1
			if comparison != len(first_childs):
				return False

		return True
		
	def _add_element(self, after, parent, node):
		after_element  = self.etree.findall(after)[-1]
		parent_element = self.etree.find(parent)
		parent_element.insert(list(parent_element).index(after_element), node)

	def __iter__(self):
		"""
		Returns keys iterator 
		"""	
		return self.etree.findall(self._root_path + "*")
		#return ElementPath13.findall(self.etree, self._root_path + "*")
		
	def _find_all(self, path):
		if not self.etree:
			self._init()
		if not path:
			path = '.'
		ret = self.etree.findall(self._root_path + quote(path))
		indexes = []
		for node in ret:
			if callable(node.tag):
				indexes.append(ret.index(node))
				
		indexes.reverse()

		for i in indexes:
			del ret[i]

		return ret
		"""
		ret = []
		try:
			it = ElementPath13.findall(self.etree, self._root_path + quote(path))			
			while 1:
				ret.append(it.next())
		except StopIteration:
			return ret
		"""
		
	def _find(self, path):
		if not self.etree:
			self._init()
		el = self.etree.find(self._root_path + quote(path))
		# el = ElementPath13.find(self.etree, self._root_path + quote(path))
		if el != None:
			return el
		else:
			raise NoPathError(quote(path))
	
	def get(self, path):
		if not self.etree:
			self._init()
				
		"""
		@see http://effbot.org/zone/element-xpath.htm
		v = conf.get("general/server_id")
		v = "3233-322"
		"""
		
		return str(self._find(path).text)
	
	def get_float(self, path):
		return float(self.get(path))
	
	def get_int(self, path):
		return int(self.get(path))
	
	def get_boolean(self, path):
		return self.get(path).lower() in ["1", "yes", "true", "on"]
	
	def get_list(self, path):
		return list(el.text for el in self._find_all(path))
	
	def get_dict(self, path):
		return [x.attrib for x in self._find_all(path) if x.attrib]
	
	def _normalize_path(self, path):
		if path[-1] != '/':
			path = path + '/'
		return path

	def _is_element(self, node):
		return node.tag and not callable(node.tag)
	
	def items(self, path):
		'''
		Returns a list of (name, value) pairs
		'''
		return tuple(
			(node.tag, node.text) 
			for node in self._find_all(quote(self._normalize_path(path)))
			if self._is_element(node)
		)
	
	def children(self, path):
		'''
		Returns a list of child names (options and sections)
		'''
		ret_list = self._find_all(quote(self._normalize_path(path)))
		return tuple(node.tag for node in ret_list if self._is_element(node))
	
	def sections(self, path):
		'''
		Returns a list of child sections
		'''
		nodes = self._find_all(quote(self._normalize_path(path)))
		return tuple(node.tag for node in nodes 
				if self._is_element(node) and len(node))
		
	def options(self, path):
		'''
		Returns a list of child options
		'''
		nodes = self._find_all(quote(self._normalize_path(path)))
		return tuple(node.tag for node in nodes 
				if self._is_element(node) and not len(node))
				
	def set(self, path, value, force=False):
		if not self.etree:
			self._init()
		el = self.etree.find(self._root_path + quote(path))
		if el != None:
			self._set(el, value)
		elif force:
			self.add(path, value)
		else:
			raise NoPathError("Path %s doesn't exist" % path)
	
	def _set(self, el, value):
		if isinstance(value, dict):
			for key in value:
				el.attrib.update({key: value[key]})
		else:
			el.text = value
	
	def add(self, path, value=None, before_path=None):
		"""
		Add value at path <path> 
		if before_path specified, new option will be added right after it.
		"""
		value = str(value)
		
		if not self.etree:
			self._init()
		
		after_element = None
		before_element = None
		
		if path.endswith('/'):
			path = path[:-1]
		
		parent_path = os.path.dirname(path) or '.'
		
		parent		= self._find(parent_path)
		el = self._provider.create_element(self.etree, os.path.join(self._root_path, path), value)
				
		if before_path:
			path_list = self._find_all(parent_path +'/'+ before_path)
			if len(path_list):
				before_element = path_list[0]
				
		path_list = self._find_all(path)
		if len(path_list):
			after_element = path_list[-1]
		
		if after_element != None:
			parent.insert(list(parent).index(after_element) + 1, el)
		elif before_element != None:
			parent.insert(list(parent).index(before_element), el)
		else:
			parent.append(el)
			self._set(el, value)
		self._set(el, value)
				
		"""
		1.
		[general]
		server_id = Piska
		
		conf.add("general/behaviour", "cassandra", "general/server_id")
		
		[general]
		behaviour = cassandra
		server_id = Piska
		
		2.
		[general]
		behaviour = app

		conf.add("general/behaviour", "cassandra")
 root_path=path+"/"
		[general]
		behaviour = app
		behaviour = cassandra
		"""
		
		"""
		Create elements, call _set
		"""
	
	
	def remove(self, path, value=None):
		"""
		Remove path. If value is passed path is treatead as list key, 
		# and config removes specified value from it. 
		"""
		"""
		conf.remove("Seeds/Seed")
		empty Seeds
		conf.remove("Seeds/Seed", "143.66.21.76")
		remove "143.66.21.76" from list
		"""
		try:
			opt_list = self._find_all(path)			
			parent = self.subset(path)._find('..')
			if value:
				for opt in opt_list:
					if opt.text.strip() == value:
						parent.remove(opt)
			else:	
				for opt in opt_list:
					parent.remove(opt)
		except NoPathError:
			pass			

	def subset(self, path):
		"""
		Return wrapper for configuration subset under specified path
		"""
		
		"""
		find el at path
		
		subconf = conf["Seeds/Seed[1]"]
		
		"""
		self._find(path)
		return Configuration(format=self._format, etree=self.etree, root_path=path+"/")
	
	
	@property
	def empty(self):
		return not bool(list(self.etree.getroot()))

"""
class PyConfigParserAdapter:
	def __init__(self, conf):
		pass

	def sections(self):
		pass

	def add_section(self, section, before=None):
		pass
	
	def has_section(self, section):
		pass
	
	def options(self, section):
		pass
"""

class FormatProvider:
	_readers = None
	_writers = None
	
	def __init__(self):
		self._readers = ()
		self._writers = ()
		self._sections = []
		self._errors = []
		
	def create_element(self, etree, path, value):
		return ET.Element(quote(os.path.basename(path)))
		
	def read(self, fp, baseline = 0):
		"""
		@return: xml.etree.ElementTree
		"""
		self._lineno = baseline
		if not hasattr(self, '_sections') and not hasattr(self, '_errors'):
			self._sections = []
			self._errors = []
		self._fp = fp
		root = ET.Element("configuration")
		if not hasattr(self, '_cursect'):
			self._cursect = root
		while True:
			line = self._fp.readline()
			if not line:
				break
			self._lineno += 1
			for reader in self._readers:
				if reader(line, root):
					break
			else:
				self._errors.append((self._lineno, line.strip()))

		indent(root)
		if self._errors and not self._sections:
			raise ParseError(self._errors)
		else:
			return list(root)
		
	def write(self, fp, etree, close = True):
		"""
		Write ElementTree <etree> to filepointer <fp>. If <close> is True - close <fp> 
		"""
		try:
			if not (isinstance(etree, ET._ElementInterface) or isinstance(etree, ET.ElementTree)):
				raise MetaconfError("etree param must be instance of _ElementInterface or ElementTree. %s passed" % (etree,))
			errors = []
			toplevel = list(etree.find('.'))
			if not len(toplevel):
				exit
			for section in toplevel:
				for writer in self._writers:
					if writer(fp, section):
						break
				else:
					errors.append(unquote(section.tag))
			if errors:
				raise MetaconfError(errors)
		finally:
			if close:
				fp.close()

class IniFormatProvider(FormatProvider):
	
	_readers = None
	_writers = None
	
	def __init__(self):
		FormatProvider.__init__(self)
		self._readers = (
			self.read_blank,
			self.read_comment,
			self.read_section,
			self.read_option
		)
		self._writers = (								
			self.write_blank,
			self.write_comment,
			self.write_section,
			self.write_option
		)
		
	def create_element(self, etree, path, value):
		el = FormatProvider.create_element(self, etree, path, value)
		parent_path = os.path.dirname(path)
		if os.path.dirname(parent_path) not in ('.', ''):
			raise MetaconfError('Maximum nesting level for ini format is 2')
		elif parent_path in ('.', ''):
			if etree.find(path) is not None:
				raise MetaconfError("Ini file can't contain two identical sections")
			el.attrib['mc_type'] = 'section'
		else:
			el.attrib['mc_type'] = 'option'
		return el

		
	def read_comment(self, line, root):	
		if not hasattr(self, "_comment_re"):
			self._comment_re = re.compile('\s*[#;](.*)$')
		if self._comment_re.match(line):
			comment = ET.Comment(self._comment_re.match(line).group(1))
			self._cursect.append(comment)
			return True
		return False
	
	def read_section(self, line, root):
		if not hasattr(self, "_sect_re"):
			self._sect_re = re.compile(r'\[(?P<header>[^]]+)\]')
		if self._sect_re.match(line):
			self._cursect = ET.SubElement(root, quote(self._sect_re.match(line).group('header')))
			self._cursect.attrib['mc_type'] = 'section'
			return True
		return False
	
	def read_blank(self, line, root):
		if '' == line.strip():
			ET.SubElement(self._cursect, '')
			return True
		return False
	
	def read_option(self, line, root):
		if not hasattr(self, "_opt_re"):
			self._opt_re = re.compile(r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*?)\s*(?P<comment>[#;](.*))?$')
		if self._opt_re.match(line):
			if self._opt_re.match(line).group('comment'):
				comment = ET.Comment(self._opt_re.match(line).group('comment')[1:])
				self._cursect.append(comment)
			new_opt = ET.SubElement(self._cursect, quote(self._opt_re.match(line).group('option').strip()))
			value = self._opt_re.match(line).group('value')
			if len(value) > 1 and value[0] in ('"', "'") and value[-1] in ('"', "'") and value[0] == value[-1]:
				value = value[1:-1]
			new_opt.text = value
			new_opt.attrib['mc_type'] = 'option'
			return True
		return False
	
	def write_comment(self, fp, node):
		if callable(node.tag):
			comment_lines  = node.text.split('\n')
			for line in comment_lines:
					fp.write('#'+line+'\n')
			return True
		return False
	
	def write_section(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section': 
			fp.write('['+unquote(node.tag)+']\n')	
			self.write(fp, node, False)
			return True
		return False
	
	def write_option(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
			value = node.text if node.text else ''
			if re.search('\s', value):
				value = '"' + value + '"'
			fp.write(unquote(node.tag)+"\t= "+value+'\n')
			return True
		return False

	def write_blank(self, fp, node):
		if not node.tag and not callable(node.tag):
			fp.write('\n')
			return True
		return False
					

format_providers["ini"] = IniFormatProvider


class NginxFormatProvider(IniFormatProvider):
	
	def __init__(self):
		IniFormatProvider.__init__(self)
		self._readers += (self.read_statement,)
		self._writers += (self.write_statement,)
		self._nesting  = 0
		self._pad = '	'
	
	def create_element(self, etree, path, value):
		el = FormatProvider.create_element(self, etree, path, value)
		
		parent_path = os.path.dirname(path)
		if parent_path not in  ('.', ''):
			parent = etree.find(parent_path)
			# We are sure that parent element exists, because Configuration calls private method '_find' first
			if parent.attrib.has_key('mc_type') and parent.attrib.has_key('mc_type') != 'section':
				parent.attrib['mc_type'] = 'section'
				if parent.text.strip():
					parent.attrib['value'] = parent.text.strip()
					parent.text = ''				
		
		el.attrib['mc_type'] = 'option' if value else 'statement'
		return el
			
	def read_comment(self, line, root):
		if not hasattr(self, "_comment_re"):
			self._comment_re = re.compile('\s*#(.*)$')
		if self._comment_re.match(line):
			comment = ET.Comment(self._comment_re.match(line).group(1))
			self._cursect.append(comment)
			return True
		return False
	
	
	def read_option(self, line, root):
		if not hasattr(self, "_multi_re"):
			self._multi_re = re.compile("\s*(?P<statement>[^\s]+)\s+(?P<value>.+?)(?P<multi_end>;)?\s*(#(?P<comment>.*))?$")

		result = self._multi_re.match(line)

		if result:
			new_multi = ET.Element(quote(result.group('statement').strip()))
			new_multi.attrib['mc_type'] = 'option'
			multi_value = quote(result.group('value').strip())
			if result.group('comment'):
				comment = ET.Comment(result.group('comment').strip())
				self._cursect.append(comment)
			if result.group('multi_end'):
				new_multi.text = multi_value
				self._cursect.append(new_multi)
				return True
			else:
				opened = 1
				if not hasattr(self, "_multi_block"):
					self._multi_block = re.compile("\s*(?P<value>[^#]+?)(?P<multi_end>;)?\s*(#(?P<comment>.*))?$")
				while opened != 0:
					new_line = self._fp.readline()
					if not new_line:
						return False
					result = self._multi_block.match(new_line)
					if not result:
						return False
					self._lineno += 1
					if result.group('comment'):
						comment = ET.Comment(result.group('comment'))
						self._cursect.append(comment)
					multi_value += quote('\n'+self._pad+result.group('value').strip())
					if result.group('multi_end'):
						opened -= 1
				new_multi.text = multi_value
				self._cursect.append(new_multi)
				return True
		return False
	
	def read_statement(self, line, root):
		if not hasattr(self, "_stat_re"):
			self._stat_re = re.compile(r'\s*([^\s\[\]]*)\s*;\s*$')
		if self._stat_re.match(line):
			new_statement = ET.SubElement(self._cursect, quote(self._stat_re.match(line).group(1)))
			new_statement.attrib['mc_type'] = 'statement'			
			return True
		return False
	
	def read_section(self, line, root):
		if not hasattr(self, "_sect_re"):
			self._sect_re = re.compile('\s*(?P<option>[^\s]+)\s*(?P<value>.*?)\s*{\s*(?P<comment>#(.*))?\s*')
			
		result = self._sect_re.match(line)
		if result:
			new_section = ET.SubElement(self._cursect, quote(result.group('option').strip()))
			new_section.attrib['mc_type'] = 'section'
			if result.group('value'):
				new_section.attrib['value'] = quote(result.group('value').strip())
			if result.group('comment'):
				new_section.append(ET.Comment(result.group(4)))
			opened = 1 if '}' not in line.split('#')[0] else 0
			
			while opened != 0:
				new_line = self._fp.readline()
				if not new_line:
					return False
				line += new_line
				if not hasattr(self, "_block_re"):
					self._block_re = re.compile('[^#]*([{}]).*$')
				result = self._block_re.match(new_line)
				if result:
					opened += 1 if result.group(1) == '{' else -1
			
			self._sections.append(self._cursect)
			self._cursect = new_section
			old_fp = self._fp
			content = re.search(re.compile('{.*?\n(.*)}',re.S), line).group(1).strip()
			self.read(StringIO(content), self._lineno)
			self._fp = old_fp
			self._cursect = self._sections.pop()
			self._lineno += 1
			return True
		return False
	
	def write_comment(self, fp, node):
		if callable(node.tag):
			comment_lines  = node.text.split('\n')
			for line in comment_lines:
				fp.write(self._pad*self._nesting + '#'+line+'\n')
			return True
		return False
	
	def write_statement(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'statement':
			fp.write(self._pad*self._nesting + unquote(node.tag)+';\n')
			return True
		return False
	
	def write_section(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
			value = unquote(node.attrib['value']) if node.attrib.has_key('value') else ''
			fp.write(self._pad*self._nesting + unquote(node.tag) + ' ' + value + ' {\n')
			self._nesting +=1
			try:
				self.write(fp, node, False)
			finally:
				self._nesting -=1
			fp.write(self._pad*self._nesting + '}\n')
			return True
		return False
	
	def write_option(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
			values = node.text.split('\n')
			fp.write (self._pad*self._nesting + unquote(node.tag)+ self._pad + unquote(values.pop(0)))
			if len(values):
				tag_len = len(node.tag)
				for value in values:
					fp.write('\n'+self._pad*self._nesting + ' '*tag_len + unquote(value))
			fp.write(';\n')
			return True
		return False

format_providers["nginx"] = NginxFormatProvider



class XmlFormatProvider:
	
	def create_element(self, etree, path, value):
		return ET.Element(quote(os.path.basename(path)))
		
	def read(self, fp):
		try:
			etree = ET.parse(fp, parser=CommentedTreeBuilder())
		except Exception, e:
			raise ParseError(())

		indent(etree.getroot())
		return [etree.getroot()]

	def write(self, fp, etree, close):
		try:
			new_tree = ET.ElementTree(list(etree.getroot())[0])
			indent(new_tree.getroot())
			new_tree.write(fp)
		finally:
			fp.close()


format_providers["xml"] = XmlFormatProvider

class ApacheFormatProvider(IniFormatProvider):
	
	_readers = None
	_writers = None

	def create_element(self, etree, path, value):
		el = FormatProvider.create_element(self, etree, path, value)
		parent_path = os.path.dirname(path)
		if parent_path not in  ('.', ''):
			parent = etree.find(parent_path)
			# We are sure that parent element exists, because Configuration calls private method '_find' first
			if parent.attrib.has_key('mc_type') and parent.attrib.has_key('mc_type') != 'section':
				parent.attrib['mc_type'] = 'section'
				if parent.text.strip():
					parent.attrib['value'] = parent.text.strip()
					parent.text = ''
		
		el.attrib['mc_type'] = 'option'
		return el

	def __init__(self):
		IniFormatProvider.__init__(self)
		self._nesting  = 0
		self._pad = '	'
		
	def read_option(self, line, root):
		if not hasattr(self, "_opt_re"):
			self._opt_re = re.compile(r'\s*(?P<option>[^<].*?)\s+(?P<value>.*?)\s*?(?P<backslash>\\?)$')
		result = self._opt_re.match(line)
		if result:
			new_opt = ET.SubElement(self._cursect, quote(result.group('option').strip()))
			new_opt.attrib['mc_type'] = 'option'
			value = result.group('value')
			if result.group('backslash'):
				while True:
					new_line = self._fp.readline()
					if not new_line:
						return False
					raw_line = new_line.strip()
					if raw_line.endswith('\\'):
						value += ' ' + raw_line[:-1]
					else:
						value += ' ' + raw_line
						break
			new_opt.text = value
			return True
		return False
	
		
	def write_option(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
			fp.write(self._pad*self._nesting + unquote(node.tag)+"\t"+node.text+'\n')
			return True
		return False
	
	def write_comment(self, fp, node):
		if callable(node.tag):
			comment_lines  = node.text.split('\n')
			for line in comment_lines:
				fp.write(self._pad*self._nesting + '#'+line+'\n')
			return True
		return False
	
	def read_section(self, line, root):
		if not hasattr(self, "_sect_re"):
			self._sect_re = re.compile('\s*<(?P<option>[^\s]+)\s*(?P<value>.*?)\s*>\s*$')
			
		result = self._sect_re.match(line)
		if result:
			tag = result.group('option').strip()
			new_section = ET.SubElement(self._cursect, quote(tag))
			new_section.attrib['mc_type'] = 'section'
			value = result.group('value').strip()
			if value:
				new_section.attrib['value'] = value
				
			opened = 1 
									
			while opened != 0:
				new_line = self._fp.readline()
				if not new_line:
					return False
				
				line += new_line
				stripped = new_line.strip()
				if stripped.startswith('</'+tag+'>'):
					opened -= 1
				if stripped.startswith('<'+tag):
					opened += 1
			
			self._sections.append(self._cursect)
			self._cursect = new_section
			old_fp = self._fp
			content = re.search(re.compile('.*?>\s*\n(.*)<.*?>',re.S), line).group(1).strip()
			self.read(StringIO(content), self._lineno)
			self._fp = old_fp
			self._cursect = self._sections.pop()
			self._lineno += 1
			return True
		return False
	
	def write_section(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
			text = node.text.strip()
			value = ' ' + node.attrib['value'] if node.attrib.has_key('value') else ''
			tag = unquote(node.tag)
			fp.write(self._pad*self._nesting + '<' + tag + value + '>\n')
			self._nesting +=1
			try:
				self.write(fp, node, False)
			finally:
				self._nesting -=1
			fp.write(self._pad*self._nesting + '</'+ tag +'>\n')
			return True
		return False
	
format_providers["apache"] = ApacheFormatProvider
"""
class YamlFormatProvider:

	def __init__(self):
		if not YamlFormatProvider._yaml:
			try:
				YamlFormatProvider._yaml = __import__("yaml")
			except ImportError:
				raise MetaconfError("`'yaml' module is not defined. Install PyYAML package")
	
	def read(self, fp, filename):
		try:
			self._root = ET.Element('configuration')
			self._cursect = self._root
			dict = YamlFormatProvider._yaml.load(fp.read(), Loader = YamlFormatProvider._yaml.BaseLoader)
			self._parse(dict)
			indent(self._root)
			return list(self._root)
		except (BaseException, Exception), e:
			raise ParseError((e,))
			
	def _parse(self, iterable):
		if isinstance(iterable, dict):
			cursect = []
			for key in iterable:
				new_opt = ET.SubElement(self._cursect, str(key))
				cursect.append(self._cursect)
				self._cursect = new_opt
				self._parse(iterable[key])
				self._cursect = cursect.pop()
		elif isinstance(iterable, list):
			for value in iterable:
				self._parse(value)
		else:
			self._cursect.text = str(iterable)

format_providers["yaml"] = YamlFormatProvider
"""

"""
[general]
; Server behaviour (app|www|mysql) 
behaviour = app
behaviour = www
behaviour = mysql

; Role name ex = mysqllvm64
role_name

; Platform on which scalarizr is deployed 
;   ec2     - Amazon EC2, 
;   rs      - RackSpace cloud servers
;   vps     - Standalone VPS server 
platform = vps
"""

class MysqlFormatProvider(IniFormatProvider):
	def __init__(self):
		IniFormatProvider.__init__(self)
		self._readers  = (self.read_statement,
						   self.read_include) + self._readers
		
		self._writers  = (self.write_statement,
						   self.write_include) + self._writers

	def create_element(self, etree, path, value):
		el = FormatProvider.create_element(self, etree, path, value)
		
		parent_path = os.path.dirname(path)
		
		if os.path.dirname(parent_path) not in ('.', ''):
			raise MetaconfError('Maximum nesting level for ini format is 2')
		elif parent_path in ('.', '') and not '!include' in el.tag:
			if etree.find(path) is not None:
				raise MetaconfError("Mysql file can't contain two identical sections")
			el.attrib['mc_type'] = 'section'
		else:
			if value:
				el.attrib['mc_type'] = 'include' if '!include' in el.tag else 'option' 
			else:
				el.attrib['mc_type'] = 'statement'			
		return el
	
	def read_statement(self, line, root):
		if not hasattr(self, "_stat_re"):
			self._stat_re = re.compile(r'\s*([^#=\s\[\]]+)\s*$')
		if self._stat_re.match(line):
			new_statement = ET.SubElement(self._cursect, quote(self._stat_re.match(line).group(1)))
			new_statement.attrib['mc_type'] = 'statement'
			return True
		return False
		
	def read_include(self, line, root):
		if not hasattr(self, "_inc_re"):
			self._inc_re = re.compile(r'\s*(!include(dir)?)\s+(.+)$')
		if self._inc_re.match(line):
			new_include = ET.SubElement(self._cursect, quote(self._inc_re.match(line).group(1)))
			new_include.text = self._inc_re.match(line).group(3).strip()
			new_include.attrib['mc_type'] = 'include'
			return True
		return False


	def write_statement(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'statement': 
			fp.write(unquote(node.tag)+'\n')
			return True
		return False
	
	def write_include(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'include': 
			fp.write(unquote(node.tag)+" "+node.text.strip()+'\n')
			return True
		return False

format_providers["mysql"] = MysqlFormatProvider
	

def indent(elem, level=0):
	i = "\n" + level*"	"
	if len(elem):
		if not elem.text or not elem.text.strip():
			elem.text = i + "	"
		if not elem.tail or not elem.tail.strip():
			elem.tail = i
		for elem in elem:
			indent(elem, level+1)
		if not elem.tail or not elem.tail.strip():
			elem.tail = i
	else:
		if level and (not elem.tail or not elem.tail.strip()):
			elem.tail = i
			
def quote(line):
	line = re.sub(' ', '%20', line)
	return re.sub('"', '%22', line)

def unquote(line):
	line = re.sub('%20', ' ', line)
	return re.sub('%22', '"', line)

			
class CommentedTreeBuilder ( ET.XMLTreeBuilder ):
	def __init__ ( self, html = 0, target = None ):
		ET.XMLTreeBuilder.__init__( self, html, target )
		self._parser.CommentHandler = self.handle_comment

	def handle_comment ( self, data ):
		self._target.start( ET.Comment, {} )
		self._target.data( data.strip() )
		self._target.end( ET.Comment )
		
'''
# use for xml.etree.ElementTree for hierarchy

root = ET.Element("configuration")
gen = ET.SubElement(root, "general")
gen.append(ET.Comment("Server behaviour (app|www|mysql)"))
bh = ET.SubElement(gen, "behaviour")
bh.text = "app"
bh = ET.SubElement(gen, "behaviour")
bh.text = "www"
bh = ET.SubElement(gen, "behaviour")
bh.text = "mysql"
# ...


conf = Configuration("ini")
bhs = conf.get_list("general/behaviour")
platform = conf.get("general/platform")
conf.set("handler_mysql/replication_master", 1, bool)

# Access sections
sect = conf.subset("handler_mysql")  # 1 way
sect = conf["handler_mysql"]			# 2 shorter way
sect.set("replication_master", 1, bool)


class XmlFormatProvider:
	pass
format_providers["xml"] = XmlFormatProvider


"""
<Storage>
  <!--======================================================================-->
  <!-- Basic Configuration                                                  -->
  <!--======================================================================-->

  <!-- 
   ~ The name of this cluster.  This is mainly used to prevent machines in
   ~ one logical cluster from joining another.
  -->
  <ClusterName>Test Cluster</ClusterName>
<AutoBootstrap>false</AutoBootstrap>

  <!--
   ~ See http://wiki.apache.org/cassandra/HintedHandoff
  -->
  <HintedHandoffEnabled>true</HintedHandoffEnabled>

  <!--
   ~ Keyspaces and ColumnFamilies:
   ~ A ColumnFamily is the Cassandra concept closest to a relational
   ~ table.  Keyspaces are separate groups of ColumnFamilies.  Except in
   ~ very unusual circumstances you will have one Keyspace per application.

   ~ There is an implicit keyspace named 'system' for Cassandra internals.
  -->
  <Keyspaces>
    <Keyspace Name="Keyspace1">
    	<ColumnFamily Name="Standard2" 
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
    
    
  </Keyspaces>
  
  
  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>  
  
</Storage> """ 
'''



"""
1. 
####First:

<Storage>
 
  <Keyspaces>
    <Keyspace Name="Keyspace">
    	<ColumnFamily Name="Standard2" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>
   
   <Keyspaces>
    <Keyspace Name="Keyspace">
    	<ColumnFamily Name="Standard3" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>
 
  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>  
</Storage> 

####Second:

<Storage>
  <Keyspaces>
    <Keyspace Name="Keyspace">
    	<ColumnFamily Name="Standard2" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>
  
  <Keyspaces>
    <Keyspace Name="Keyspace">
    	<ColumnFamily Name="Standard4" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>
  
  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>  
</Storage> 

2. #### First

<Keyspaces>
  <Keyspace Name="Keyspace">
 	<ColumnFamily Name="Standard" CompareWith="UTF8Type" KeysCached="100%"/>
     <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
  </Keyspace>
</Keyspaces>

##### Second

<Keyspaces>
  <Keyspace Name="Keyspace3">
 	<ColumnFamily Name="Standard" CompareWith="UTF8Type" KeysCached="100%"/>
     <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
  </Keyspace>
</Keyspaces2>

3. ####### First


"""