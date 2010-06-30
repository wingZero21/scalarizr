'''
Created on Jun 29, 2010

A cute library to read and write configurations in a various formats 
using single interface.
Primary goal: support Ini, Xml, Yaml, ProtocolBuffers, Nginx, Apache2

@author: marat
@author: spike
'''
from xml.etree import ElementTree as ET
import re
format_providers = dict()
default_format = "ini"
	
class MetaconfError(Exception):
	pass

class Configuration:
	def __init__(self, format=default_format):
		pass
	
	def read(self, filenames, format=default_format):
		pass
		
	def write(self, fp, format=default_format):
		"""
		"""
		pass
	
	def extend(self, conf):
		"""
		Extend self with options from another config 
		"""
		pass
	
	def __iter__(self):
		"""
		Returns keys iterator 
		"""
		pass
	
	def get(self, key):
		"""
		@see http://effbot.org/zone/element-xpath.htm
		"""
		pass
	
	def set(self, path, value, typecast=None):
		"""
		Set value at path <path> use optional typecast <typecast> int|float|bool
		"""
		pass
	
	def add(self, path, value, typecast=None, before_path=None):
		pass
	
	def remove(self, path, value=None):
		"""
		Remove path. If value is passed path is treatead as list key, 
		# and config removes specified value from it. 
		"""
		pass
	
	def subset(self, path):
		"""
		Return wrapper for configuration subset under specified path
		"""
		pass
	
	def get_float(self, path):
		pass
	
	def get_int(self, path):
		pass
	
	def get_boolean(self, path):
		pass
	
	def get_list(self, path):
		pass
	
	def get_dict(self, path):
		"""
		For XML return node attributes
		For INI return {}
		"""
		pass

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


class IniFormatProvider:
	
	_parsers = None
	
	def __init__(self):
		self._parsers = (
			self.parse_comment,
			self.parse_section,
			self.parse_option
		)
	
	def read(self, fp):
		"""
		@return: xml.etree.ElementTree
		"""
		errors = []
		lineno = 0
		root = ET.Element("configuration")
		cursect = root
		while True:
			line = fp.readline()
			if not line:
				break
			lineno += 1
			if line.strip() == '':
				continue
			for parser in self._parsers:
				if parser(line, cursect, root):
					match = True
					break
			if not match:
				errors.append((lineno, line))

		indent(root)
		if errors:
			raise MetaconfError(errors)
		else:
			return ET.ElementTree(root)
		
	def parse_comment(self, line, cursect, root):
		if not hasattr(self, "_comment_re"):
			self._comment_re = re.compile('\s*[#;]([^\n]*)')
		if self._comment_re.match(line):
			comment = ET.Comment(self._comment_re.match(line).group(1))
			cursect.append(comment)
			return True
		else:
			return False
	
	def parse_section(self, line, cursect, root):
		if not hasattr(self, "_sect_re"):
			self._sect_re = re.compile(r'\[(?P<header>[^]]+)\]')
		if self._sect_re.match(line):
			cursect = ET.SubElement(root, self._sect_re.match(line).group('header'))
			return True
		else:
			return False
			
	
	def parse_option(self, line, cursect, root):
		if not hasattr(self, "_opt_re"):
			self._opt_re = re.compile(r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*)$')
		if self._opt_re.match(line):
			new_opt = ET.SubElement(cursect, self._opt_re.match(line).group('option'))
			new_opt.text = self._opt_re.match(line).group('value')
			return True
		else:
			return False
					
	def write(self, fp, etree):
		if isinstance(etree, ET._ElementInterface) or isinstance(etree, ET.ElementTree):
			toplevel = etree.find('').getchildren()
			if len(toplevel):
				for section in toplevel:
					if callable(section.tag):
						fp.write('#'+section.text+'\n')
					elif len(section.find('').getchildren()):
						fp.write('['+section.tag+']\n')
						self.write(fp, section)
					else:
						try:
							fp.write(section.tag+" = "+section.text+'\n')
						except:
							print section
					
	
format_providers["ini"] = IniFormatProvider

class XmlFormatProvider:
	def read(self, fp):
		try:
			etree = ET.parse(fp, parser=CommentedTreeBuilder())
		except Exception, e:
			raise MetaconfError(e)
			
		indent(etree.getroot())
		return etree
	
	def write(self, fp, etree):
		etree.write(fp)
		
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