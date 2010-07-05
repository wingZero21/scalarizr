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
import yaml
import os

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
		
		if etree and not isinstance(etree, ET.ElementTree):
			raise MetaconfError("etree param must be instance of ElementTree. %s passed" % (etree,))
				
		self._root_path = root_path
		self._format = format
		self._etree = etree
	
	def read(self, filenames):
		for file in filenames:
			try:
				fp = open(file)
				self.extend(fp)
				fp.close()
			except:
				pass
		indent(self._etree.getroot())
	
	def readfp(self, fp):
		if not self._provider:
			self._provider = format_providers[self._format]()
		if not self._etree:
			root = ET.Element("mc_conf/")
			self._etree = ET.ElementTree(root)
		for child in self._provider.read(fp):
			self._etree.getroot().append(child)
		
	def write(self, fp):
		"""
		"""
		if not self._provider:
			raise MetaconfError("Nothing to write! Create the tree first (readfp or read)")
		self._provider.write(fp, self._etree)
	
	def extend(self, conf):
		"""
		Extend self with options from another config
		Comments and blank lines from importing config will not be added
		"""
		if not self._etree:
			self._root = ET.Element("mc_conf/")
			self._etree = ET.ElementTree(self._root)

		if not self._provider:
			self._provider = format_providers[self._format]()

		node_list = self._provider.read(conf)

		self._sections = []
		self._cursect = '/'
		for node in node_list:
			self._extend(node)

		
	def _extend(self, node):
		if not callable(node.tag) and node.tag != '':
			cursect = self._cursect + '/' + node.tag
			exist_list = self._etree.findall(cursect)
			if exist_list:
				if len(exist_list) == 1 and exist_list[0].attrib == node.attrib:
					childs = exist_list[0].getchildren()
					if len(childs):
						self._sections.append(self._cursect)
						self._cursect  = cursect
						for child in node.getchildren():
							self._extend(child)
						self._cursect  = self._sections.pop()
					else:
						if node.text != exist_list[0].text and \
											(len(node.attrib) ^ (exist_list[0].attrib != node.attrib)):
							self._add_after(cursect, self._cursect, node)
				else:
					equal = 0
					for exist_node in exist_list:
						equal += 0 if not self._compare(exist_node, node) else 1
					if not equal:
						self._add_after(cursect, self._cursect, node)
			else:
				self._etree.find(self._cursect).append(node)
				
	def _compare(self, first, second):
		
		if first.text and second.text:
			first_text = first.text.strip()
			second_text = second.text.strip()
			if first_text != second_text:
				return False
			
		if first.attrib != second.attrib:
			return False
		
		first_childs = first.getchildren()
		second_childs = second.getchildren()
		
		if first_childs and second_childs:			
			if len(first_childs) != len(second_childs):
				return False
			
			comparison = 0
			for f_child in first_childs:
				for s_child in second_childs:
					comparison += 0 if not self._compare(f_child, s_child) else 1
			if comparison != len(first_childs):
				return False
			else:
				return True
		else:
			return True
		
	def _add_after(self, after, parent, node):
		after_element  = self._etree.findall(after)[-1]
		parent_element = self._etree.find(parent)
		it = parent_element.getiterator()
		parent_element.insert(it.index(after_element), node)
				
	def __iter__(self):
		return ElementPath13.findall(self._etree, self._root_path + "*")
		"""
		Returns keys iterator 
		"""	
	def _find_all(self, path):
		ret = []
		try:
			it = ElementPath13.findall(self._etree, self._root_path + path)			
			while 1:
				ret.append(it.next())
		except StopIteration:
			return ret
		
	def _find(self, path):
		el = ElementPath13.find(self._etree, self._root_path+path)
		if None != el:
			return el
		else:
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
		Set value at path <path> use optional typecast <typecast> int|float
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
		if el != None:
			self._set0(el, value, typecast)
	
	def _set0(self, el, value, typecast=None):
		if isinstance(value, dict):
			for key in value:
				el.attrib.update({key: value[key]})
		else:
			if typecast in (float, int):
				try:
					value = str(typecast(value))
				except AttributeError:
					raise MetaconfError('Wrong typecast %s for value ' % (typecast,))
			el.text = value
	
	def add(self, path, value, typecast=None, before_path=None):
		
		after_element = None
		parent_path = os.path.dirname(path)
		parent		= self._find(parent_path)
		el = ET.Element(os.path.basename(path))
		
		if before_path:
			path_list = self._find_all(parent_path +'/'+ before_path)
			if len(path_list):
				before_element = path_list[0]
				
		path_list = self._find_all(path)
		if len(path_list):
			after_element = path_list[-1]
		
		if after_element != None:
			it = parent.getiterator()
			parent.insert(it.index(after_element), el)
		elif before_element != None:
			it = parent.getiterator()
			parent.insert(it.index(before_element), el)
		else:
			parent.append(el)
			self._set0(el, value, typecast)
		self._set0(el, value, typecast)

				
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
		Create elements, call _set0
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
		opt_list = self._find_all(path)
		parent = self.subset(path)._find('..')
		if value:
			for opt in opt_list:
				if opt.text.strip() == value:
					parent.remove(opt)
		else:	
			for opt in opt_list:
				parent.remove(opt)			

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
			del root
			raise ParseError(errors)
		else:
			childs = root.getchildren()
			del root 
			return childs 
		
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
			self._cursect = ET.SubElement(root, self._sect_re.match(line).group('header'), {'mc_type' : 'section'})
			return True
		return False
	
	def read_blank(self, line, root):
		if '' == line.strip():
			ET.SubElement(self._cursect, '', {'mc_type' : 'blank'})
			return True
		return False
	
	def read_option(self, line, root):
		if not hasattr(self, "_opt_re"):
			self._opt_re = re.compile(r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*)$')
		if self._opt_re.match(line):
			new_opt = ET.SubElement(self._cursect, self._opt_re.match(line).group('option').strip(), {'mc_type' : 'option'})
			new_opt.text = self._opt_re.match(line).group('value')
			return True
		return False
	
	def write_comment(self, fp, node):
		if callable(node.tag):
			fp.write('#'+node.text+'\n')
			return True
		return False
	
	def write_section(self, fp, node):
		if 'mc_type' in node.attrib and 'section' == node.attrib['mc_type']:
			fp.write('['+node.tag+']\n')
			self.write(fp, node)
			return True
		return False
	
	def write_option(self, fp, node):
		if 'mc_type' in node.attrib and 'option' == node.attrib['mc_type']:
			fp.write(node.tag+" = "+node.text+'\n')
			return True
		return False
	
	def write_blank(self, fp, node):
		if 'mc_type' in node.attrib and 'blank' == node.attrib['mc_type']:
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
		return [etree.getroot()]
	
	def write(self, etree, fp):
		etree.write(fp)

format_providers["xml"] = XmlFormatProvider
	
		
class YamlFormatProvider:
	
	def __init__(self):
		pass
	
	def read(self, fp):
		try:
			self._root = ET.Element('configuration')
			self._cursect = self._root
			dict = yaml.load(fp.read(), Loader = yaml.BaseLoader)
			self._parse(dict)
			indent(self._root)
			return self._root.getchildren()
		except (BaseException, Exception), e:
			raise ParseError(e)
			
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
			ET.SubElement(self._cursect, self._stat_re.match(line).group(1), {'mc_type' : 'statement'} )
			return True
		else:
			return False
		
	def read_include(self, line, root):
		if not hasattr(self, "_inc_re"):
			self._inc_re = re.compile(r'\s*(!include(dir)?)\s*([^\s]*)[^\w-]*$')
		if self._inc_re.match(line):
			new_include = ET.SubElement(self._cursect, self._inc_re.match(line).group(1), {'mc_type' : 'include'})
			new_include.text = self._inc_re.match(line).group(3)
			return True
		else:
			return False


	def write_statement(self, fp, node):
		if 'mc_type' in node.attrib and 'statement' == node.attrib['mc_type']:
			fp.write(node.tag+'\n')
			return True
		return False
	
	def write_include(self, fp, node):
		if 'mc_type' in node.attrib and 'include' == node.attrib['mc_type']:
			fp.write(node.tag+" "+node.text.strip()+'\n')
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