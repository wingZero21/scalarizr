'''
Created on Apr 7, 2010

@author: marat
'''

from M2Crypto.EVP import Cipher
from M2Crypto.Rand import rand_bytes
import binascii

def keygen(length=40):
	return binascii.b2a_base64(rand_bytes(length))	
			
def _init_chiper(key, op_enc=1):
	k = binascii.a2b_base64(key)
	return Cipher("bf_cfb", k[0:len(k)-9], k[len(k)-8:], op=op_enc)
		
def encrypt (s, key):
	c = _init_chiper(key, 1)
	ret = c.update(s)
	ret += c.final()
	del c
	return binascii.b2a_base64(ret)
	
def decrypt (s, key):
	c = _init_chiper(key, 0)
	ret = c.update(binascii.a2b_base64(s))
	ret += c.final()
	del c
	return ret

_READ_BUF_SIZE = 1024 * 1024	 # Buffer size in bytes
	
def digest_file(digest, file):
	while 1:
		buf = file.read(_READ_BUF_SIZE)
		if not buf:
			break;
		digest.update(buf)
	return digest.final()

def crypt_file(cipher, in_file, out_file):
	while 1:
		buf = in_file.read(_READ_BUF_SIZE)
		if not buf:
			break
		out_file.write(cipher.update(buf))
	out_file.write(cipher.final())