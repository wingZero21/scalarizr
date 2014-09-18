from __future__ import with_statement
'''
Created on Apr 7, 2010

@author: marat
'''

import binascii
import hmac
import hashlib
import re
import os
import time

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend


crypto_algo = dict(name="des_ede3_cbc", key_size=24, iv_size=8)

def keygen(length=40):
    return binascii.b2a_base64(os.urandom(length))

def _new_cipher(key):
    skey = key[0:crypto_algo["key_size"]]   # Use first n bytes as crypto key
    iv = key[-crypto_algo["iv_size"]:]      # Use last m bytes as IV
    return Cipher(algorithms.TripleDES(skey), modes.CBC(iv), backend=default_backend())

def _new_padding():
    return padding.PKCS7(64)

def encrypt (s, key):
    enc = _new_cipher(key).encryptor()
    pad = _new_padding().padder()
    padded = pad.update(s) + pad.finalize()
    encrypted = enc.update(padded) + enc.finalize()
    return binascii.b2a_base64(encrypted)

def decrypt (s, key):
    dec = _new_cipher(key).decryptor()
    unpad = _new_padding().unpadder()
    encrypted = binascii.a2b_base64(s)
    padded = dec.update(encrypted) + dec.finalize()
    return unpad.update(padded) + unpad.finalize()


_READ_BUF_SIZE = 1024 * 1024     # Buffer size in bytes

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


def _get_canonical_string (params={}):
    s = ""
    for key, value in sorted(params.items()):
        s = s + str(key) + str(value)
    return s

def sign_http_request(data, key, timestamp=None):
    date = time.strftime("%a %d %b %Y %H:%M:%S %Z", timestamp or time.gmtime())
    canonical_string = _get_canonical_string(data) if hasattr(data, "__iter__") else data
    canonical_string += date

    digest = hmac.new(key, canonical_string, hashlib.sha1).digest()
    sign = binascii.b2a_base64(digest)
    if sign.endswith('\n'):
        sign = sign[:-1]
    return sign, date

def pwgen(size):
    return re.sub('[^\w]', '', keygen(size*2))[:size]
