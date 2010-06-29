'''
Created on Jun 29, 2010

A cute library to read and write configurations in a various formats 
using single interface.
Primary goal: support Ini, Xml, Yaml, ProtocolBuffers, Nginx, Apache2

@author: marat
@author: spike
'''

format_providers = dict()
default_format = "ini"

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
	def read(self, fp):
		"""
		@return: xml.etree.ElementTree
		"""
		pass
	
	def write(self, fp, etree):
		pass
	
format_providers["ini"] = IniFormatProvider

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

# use for xml.etree.ElementTree for hierarchy
from xml.etree import ElementTree as ET
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
  <Keyspaces>
  
  
  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>  
  
</Storage>
"""
conf = Configuration("xml")

"""
1. Access subsets
"""
k1 = conf["Storage/Keyspaces/Keyspace[@Name=KeySpace1]"]

"""
2. Iterate keys
"""
for k in k1:
	print k1.get(k + "/@Name")
"""
Expected:
	Standard2
	StandardByUUID1
"""

"""
3. Add keys
"""
k1.add("ColumnFamily", dict(Name="Super2", ColumnType="Super", CompareWith="UTF8Type"))
"""
Expected: ColumnFamily[@Name=Super2] added at the end of KeySpace1
"""
k1.add("ColumnFamily", dict(Name="Super22", ColumnType="Super", CompareWith="UTF8Type"), 
		before_path="ColumnFamily[1]")
"""
Expected: ColumnFamily[@Name=Super2] added at the begining of KeySpace1
"""

"""
4. Remove keys
"""
conf.remove("Storage/Keyspaces/Keyspace[1]")
"""
Expected: Removed KeySpace1 and all it's children 
"""

conf.remove("Storage/Seeds/Seed", "10.196.18.36")
"""
Expected: Removed 10.196.18.36 from seeds list
"""



class YamlFormatProvider:
	pass

