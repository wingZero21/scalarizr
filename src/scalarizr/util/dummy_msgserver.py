'''
Created on Mar 4, 2010

@author: marat
'''

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class HttpRequestHanler(BaseHTTPRequestHandler):
	def do_POST(self):
		self.send_response(201)

server = HTTPServer(("localhost", 9999), HttpRequestHanler)
server.serve_forever()