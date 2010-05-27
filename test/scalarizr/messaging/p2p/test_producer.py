'''
Created on May 18, 2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.bus import bus
from scalarizr.messaging.p2p import P2pMessageService, P2pMessage, P2pSender
from scalarizr.messaging.p2p.producer import P2pMessageProducer
from scalarizr.util import init_tests, configtool, cryptotool
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import binascii

class _P2pMessageProducer(P2pMessageProducer):	
	def _get_next_interval(self):
		return int(self.retries_progression[self._next_retry_index]) * 1.0

class _P2pMessageService(P2pMessageService):
	def get_producer(self):
		if self._producer is None:
			self._producer = _P2pMessageProducer(**self._kwargs)
		return self._producer
	

class TestProducer(unittest.TestCase):
	
	_msg_url = "http://localhost:9999"
	_request_handler_class = None
	retries_progression = '1,2'
	attempt = 0
	last_request_time = None

	def setUp(self):
		gen_sect = configtool.section_wrapper(bus.config, configtool.SECT_GENERAL)
		
		self._srv = _P2pMessageService(
			server_id=gen_sect.get(configtool.OPT_SERVER_ID),
			crypto_key_path=gen_sect.get(configtool.OPT_CRYPTO_KEY_PATH),
			consumer_url=self._msg_url, 
			producer_url=self._msg_url,
			producer_retries_progression=self.retries_progression,
			producer_sender=P2pSender.DAEMON
		) 
		self._producer = self._srv.get_producer()
		self._crypto_key = binascii.a2b_base64(configtool.read_key("default", private=True))
		self.progression = self.retries_progression.split(',')
		
	def tearDown(self):
		self._server.shutdown()
		self._server_thread.join()

	def _decrypt_message(self, raw_message):
		xml = cryptotool.decrypt(raw_message, self._crypto_key)
		message = P2pMessage()
		message.fromxml(xml)
		return message

	def _put_undelivered_message(self, message_name, message_id):
		db = bus.db
		conn = db.get().get_connection()
		cur = conn.cursor()
		queue = "test"
		undelivered_message = self._srv.new_message(message_name)
		undelivered_message.id = message_id
		
		sql = """INSERT INTO p2p_message (id, message, message_id, 
					message_name, queue, is_ingoing, out_is_delivered)
				VALUES 
					(NULL, ?, ?, ?, ?, ?, ?)"""
		cur.execute(sql, [str(undelivered_message), undelivered_message.id, undelivered_message.name, queue, 0, 0])

	def _start_server(self):
		self._server = HTTPServer(("localhost", 9999), self._request_handler_class)
		t = threading.Thread(target=self._server.serve_forever)
		t.start()
		self._server_thread = t
		time.sleep(0.5)

	def test_right_order(self):
		class RequestHandler(BaseHTTPRequestHandler):
			testcase = None
			send_seq = []
			received_seq = []
			
			def do_POST(self):
				raw_message = self.rfile.read(int(self.headers["Content-length"]))
				message = self.testcase._decrypt_message(raw_message)
				self.send_response(201)
				self.received_seq.append(message.id)	
				
				if len(self.send_seq) > 1:
					self.testcase.assertEquals(self.received_seq[-3], self.testcase._undelivered_message_id)
					self.testcase.assertEquals(self.received_seq[-2], self.send_seq[0])										
					self.testcase.assertEquals(self.received_seq[-1], self.send_seq[1])
			
		RequestHandler.testcase = self
		self._request_handler_class = RequestHandler
		self._start_server()

		self._undelivered_message_id = "000001"
		self._put_undelivered_message("first(undelivered)", self._undelivered_message_id)

		queue = "test"
		message = self._srv.new_message("second")
		message.id = "000002"
		RequestHandler.send_seq.append(message.id)
		self._producer.send(queue, message)
		time.sleep(1)
		
		message = self._srv.new_message("third(Portishead)")
		message.id = "000003"
		RequestHandler.send_seq.append(message.id)
		self._producer.send(queue, message)
		time.sleep(2)
	
	def _test_attempts(self):	
		class RequestHandler(BaseHTTPRequestHandler):
			testcase = None
			def do_POST(self):
				raw_message = self.rfile.read(int(self.headers["Content-length"]))
				message = self.testcase._decrypt_message(raw_message)
				if self.testcase.attempt >= len(self.testcase.progression):
					self.send_response(201)
					return
				else:
					self.send_response(400)
					response_time = time.time()
					if self.testcase.last_request_time:
						time_spent = response_time - self.testcase.last_request_time
						time_delta = self.testcase.progression[self.testcase.attempt-1]
						self.testcase.assertEqual(round(time_spent), int(time_delta))
					self.testcase.last_request_time = response_time				 				
				self.testcase.attempt += 1
					
		RequestHandler.testcase = self
		self._request_handler_class = RequestHandler
		self._start_server()
		
		queue = "test"
		message = self._srv.new_message("won`t be delivered twice")
		message.id = "000000"
		self._producer.send(queue, message)
		time.sleep(sum(map(int,self.progression))+1)


if __name__ == "__main__":
	init_tests()
	unittest.main()