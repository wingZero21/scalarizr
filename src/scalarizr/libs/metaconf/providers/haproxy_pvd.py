__author__ = 'Nicholas Demyanchuk'

from . import FormatProvider
from .ini_pvd import IniFormatProvider
from .. import MetaconfError
from ..utils import quote, unquote

import sys
import re

if sys.version_info[0:2] >= (2, 7):
	from xml.etree import ElementTree as ET
else:
	from scalarizr.externals.etree import ElementTree as ET


class HaproxyFormatProvider(IniFormatProvider):

	def __init__(self):
		IniFormatProvider.__init__(self)

		self._comment_re_string = '^\s*#(.*)$'

		sections_names = ('defaults', 'frontend', 'listen', 'backend', 'global')
		self._section_re_string = '^\s*(?P<section_name>%s)\s+(?P<value>[^#]+)?\s*(?P<comment>#.*)?$' %  \
											'|'.join(sections_names)
		self._opt_re_string = '^\s*(?P<option>[^#\s]+)\s+(?P<value>[^#]+)?\s*(?P<comment>#.*)?$'
		self._indent = ''


	def create_element(self, etree, path, value):
		el = FormatProvider.create_element(self, etree, path, value)
		parent_path = os.path.dirname(path)
		if os.path.dirname(parent_path) not in ('.', ''):
			raise MetaconfError('Maximum nesting level for haproxy format is 2')
		elif parent_path in ('.', ''):
			if etree.find(path) is not None:
				raise MetaconfError("Ini file can't contain two identical sections")
			el.attrib['mc_type'] = 'section'
		else:
			el.attrib['mc_type'] = 'option'
		return el


	def read_section(self, line, root):
		if not hasattr(self, '_section_re'):
			self._section_re = re.compile(self._section_re_string)

		res = re.match(self._section_re, line)
		if res:
			if res.group('comment'):
				comment = ET.Comment(res.group('comment')[1:])
				self._cursect.append(comment)
			self._cursect = ET.SubElement(root, quote(res.group('section_name')))
			self._cursect.attrib['mc_type'] = 'section'
			value = res.group('value') or ''
			self._cursect.attrib['value'] = value.strip()
			return True
		return False


	def write_section(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
			fp.write(unquote(node.tag))
			value = node.attrib['value']
			fp.write(' ' + value)
			fp.write('\n')
			self._indent = '\t'
			self.write(fp, node, False)
			self._indent = ''
			return True
		return False


	def read_option(self, line, root):
		if not hasattr(self, '_option_re'):
			self._option_re = re.compile(self._opt_re_string)

		res = re.match(self._option_re, line)

		if res:
			if res.group('comment'):
				comment = ET.Comment(res.group('comment')[1:])
				self._cursect.append(comment)

			new_opt = ET.SubElement(self._cursect, quote(res.group('option').strip()))
			new_opt.attrib['mc_type'] = 'option'

			value = res.group('value') or ''
			new_opt.attrib['value'] = value.strip()

			return True
		return False


	def write_option(self, fp, node):
		if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
			fp.write("\t" + unquote(node.tag))
			value = node.attrib['value']
			if value:
				fp.write('\t' + unquote(value))
			fp.write('\n')
			return True
		return False


	def write_comment(self, fp, node):
		if callable(node.tag):
			comment_lines  = node.text.split('\n')
			for line in comment_lines:
				fp.write(self._indent + '#'+line+'\n')
			return True
		return False