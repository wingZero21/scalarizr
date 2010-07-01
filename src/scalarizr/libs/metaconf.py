'''
Created on Jun 29, 2010

A cute library to read and write configurations in a various formats 
using single interface.
Primary goal: support Ini, Xml, Yaml, ProtocolBuffers, Nginx, Apache2

@author: marat
@author: spike
'''
from xml.etree import ElementTree as ET
import ElementPath13
import re


format_providers = dict()
default_format = "ini"
	
class MetaconfError(Exception):
	pass

class ParseError(BaseException):
	"""
	Throw it in providers read method
	"""
	pass

class PathNotExistsError(BaseException):
	pass


class Configuration:
	_etree = None
	"""
	@ivar xml.etree.ElementTree.ElementTree _etree:  
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
		if etree and not (isinstance(etree, ET._ElementInterface) or isinstance(etree, ET.ElementTree)):
			raise MetaconfError("etree param must be instance of _ElementInterface or ElementTree. %s passed" % (etree,))
		self._root_path = root_path
		self._format = format
		self._etree = etree
	
	def read(self, filenames):
		pass
	
	def readfp(self, fp):
		if not self._provider:
			self._provider = format_providers[self._format]()
		self._etree = self._provider.read(fp)
		
	def write(self, fp):
		"""
		"""
		pass
	
	def extend(self, conf):
		"""
		Extend self with options from another config 
		"""
		pass
	
	def __iter__(self):
		return ElementPath13.findall(self._etree, self._root_path + "*")
		
		"""
		Returns keys iterator 
		"""
		pass
	
	def _find_all(self, path):
		ret = []
		try:
			it = ElementPath13.findall(self._etree, self._root_path + path)			
			while 1:
				ret.append(it.next())
		except StopIteration:
			return ret
		
	def _find(self, path):
		try:
			return self._find_all(path)[0]
		except IndexError: 
			raise PathNotExistsError(path)

	
	def get(self, path):
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
		return self.get(path).lower() in ["1", "0", "yes", "true"]
	
	def get_list(self, path):
		return list(el.text for el in self._find_all(path))
	
	def get_dict(self, path):
		return self._find(path).attrib
	
	def set(self, path, value, typecast=None):
		"""
		Set value at path <path> use optional typecast <typecast> int|float|bool
		"""
		"""
		1.
		conf.set("messaging/port", "1234", int)
		value = typecast(value) if typecast else value
		
		2. 
		conf.set("Keyspace1/ColumnFamily[1]", dict(Name="Standard2", CompareWith="UTF8Type"))
		el.attrs = dict
		"""
		
		"""
		Find element, and call _set0
		"""
		el = ElementPath13.find(self._etree, self._root_path + path)
		if el:
			self._set0(el, value, typecast)
	
	def _set0(self, el, value, typecast=None):
		
		pass
	
	def add(self, path, value, typecast=None, before_path=None):
		if before_path:
			path_list = self._etree.findall(self._root_path+before_path)
			if len(path_list):
				before_element = path_list[-1]
		path_list = self._etree.findall(self._root+path)
		if len(path_list):
			before_element = path_list[-1]
		
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

		[general]
		behaviour = app
		behaviour = cassandra
		"""
		
		"""
		Create elements, call _set0
		"""
		pass
	
	
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
		return Configuration(format=self._format, etree=self._etree, root_path=path+"/")
	


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

class IniFormatProvider:
	
	_readers = None
	_writers = None
	
	def __init__(self):
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
		self._specials = ('config_section',
						  'config_blank')

	def read(self, fp):
		"""
		@return: xml.etree.ElementTree
		"""
		errors = []
		lineno = 0
		root = ET.Element("configuration")
		self._cursect = root
		while True:
			line = fp.readline()
			if not line:
				break
			lineno += 1
			for reader in self._readers:
				if reader(line, root):
					break
			else:
				errors.append((lineno, line))

		indent(root)
		if errors:
			raise MetaconfError(errors)
		else:
			return ET.ElementTree(root)
		
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
			self._cursect = ET.SubElement(root, self._sect_re.match(line).group('header'), {'mc:type' : 'section'})
			return True
		return False
	
	def read_blank(self, line, root):
		if '' == line.strip():
			ET.SubElement(self._cursect, '', {'mc:type' : 'blank'})
			return True
		return False
	
	def read_option(self, line, root):
		if not hasattr(self, "_opt_re"):
			self._opt_re = re.compile(r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*)$')
		if self._opt_re.match(line):
			new_opt = ET.SubElement(self._cursect, self._opt_re.match(line).group('option').strip(), {'mc:type' : 'option'})
			new_opt.text = self._opt_re.match(line).group('value')
			return True
		return False
	
	def write_comment(self, fp, node):
		if callable(node.tag):
			fp.write('#'+node.text+'\n')
			return True
		return False
	
	def write_section(self, fp, node):
		if 'mc:type' in node.attrib and 'section' == node.attrib['mc:type']:
			fp.write('['+node.tag+']\n')
			self.write(fp, node)
			return True
		return False
	
	def write_option(self, fp, node):
		if 'mc:type' in node.attrib and 'option' == node.attrib['mc:type']:
			fp.write(node.tag+" = "+node.text+'\n')
			return True
		return False
	
	def write_blank(self, fp, node):
		if 'mc:type' in node.attrib and 'blank' == node.attrib['mc:type']:
			fp.write('\n')
			return True
		return False
			
	def write(self, fp, etree):
		if not (isinstance(etree, ET._ElementInterface) or isinstance(etree, ET.ElementTree)):
			raise MetaconfError("etree param must be instance of _ElementInterface or ElementTree. %s passed" % (etree,))
		errors = []
		toplevel = etree.find('').getchildren()
		if not len(toplevel):
			exit
		for section in toplevel:
			for writer in self._writers:	
				if writer(fp, section):
					break
			else:
				errors.append(section.tag)
		if errors:
			raise MetaconfError(errors)
					

format_providers["ini"] = IniFormatProvider

class XmlFormatProvider:
	def read(self, fp):
		try:
			etree = ET.parse(fp, parser=CommentedTreeBuilder())
		except Exception, e:
			raise ParseError(e)
			
		indent(etree.getroot())
		return etree
	
	def write(self, etree, fp):
		etree.write(fp)
		
		
class YamlFormatProvider:
	
	def __init__(self):
		import yaml
	
	def read(self, fp):
		try:
			self._root = ET.Element('root')
			self._cursect = self._root
			dict = yaml.load(fp.read(), Loader = yaml.BaseLoader)
			self._parse(dict)
			indent(self._root)
			return ET.ElementTree(self._root)
		except (BaseException, Exception), e:
			raise ParseError(e)
			
	def _parse(self, iterable):
		if isinstance(iterable, dict):
			for key in iterable:
				new_opt = ET.SubElement(self._cursect, str(key))
				cursect = self._cursect
				self._cursect = new_opt
				self._parse(iterable[key])
				self._cursect = cursect
		elif isinstance(iterable, list):
			for value in iterable:
				self._parse(value)
		else:
			self._cursect.text = str(iterable)

format_providers["xml"] = XmlFormatProvider
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
		self._readers  += (self.read_statement,
						   self.read_include)
		
		self._writers  += (self.write_statement,
						   self.write_include)
	
	def read_statement(self, line, root):
		if not hasattr(self, "_stat_re"):
			self._stat_re = re.compile(r'\s*([^\s*]*)\s*$')
		if self._stat_re.match(line):
			ET.SubElement(self._cursect, self._stat_re.match(line).group(1), {'mc:type' : 'statement'} )
			return True
		else:
			return False
		
	def read_include(self, line, root):
		if not hasattr(self, "_inc_re"):
			self._inc_re = re.compile(r'\s*(!include(dir)?)\s*([^\s]*)[^\w-]*$')
		if self._inc_re.match(line):
			new_include = ET.SubElement(self._cursect, self._inc_re.match(line).group(1), {'mc:type' : 'include'})
			new_include.text = self._inc_re.match(line).group(3)
			return True
		else:
			return False


	def write_statement(self, fp, node):
		if 'mc:type' in node.attrib and 'statement' == node.attrib['mc:type']:
			fp.write(node.tag+'\n')
			return True
		return False
	
	def write_include(self, fp, node):
		if 'mc:type' in node.attrib and 'include' == node.attrib['mc:type']:
			fp.write(node.tag+" "+node.text.strip()+'\n')
			return True
		return False
		
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