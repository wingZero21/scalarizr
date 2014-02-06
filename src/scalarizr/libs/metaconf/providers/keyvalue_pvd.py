
from .redis_pvd import RedisFormatProvider
from ..utils import unquote


class KeyvalueFormatProvider(RedisFormatProvider):
    _opt_re_string = r'(?P<option>[^\s]+)=(?P<value>.*)(?P<comment>#(.*))?$'
    
    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = str(node.text if node.text else '')
            fp.write(unquote(node.tag)+"="+value+'\n')
            return True
        return False
