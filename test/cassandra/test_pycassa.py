'''
Created on Aug 03, 2010

@author: Dmytro Korsakov
'''

import os
import sys
import uuid
import time
import threading
import string
import random
import struct

deps_path = os.path.realpath(os.path.join(os.path.dirname(__file__), 'pycassa'))
if os.path.exists(deps_path):
	sys.path.append(deps_path)
else:
	raise

from pycassa.connection import connect_thread_local
from pycassa.columnfamily import ColumnFamily

KEYSPACE = 'Twissandra'
CF_USER = 'User'
CF_USERNAME = 'Username'
CF_FRIENDS = 'Friends'
CF_FOLLOWRES = 'Followers'
CF_TWEET = 'Tweet'
CF_TIMELINE = 'Timeline'
CF_USERLINE = 'Userline'


class User:
	
	client = None
	id = None
	username = None
	tweet = None
	
	def __init__(self, name, password):

		self.client = connect_thread_local(framed_transport=True)

		self.id = str(uuid.uuid4())
		#add user
		cols = {'id':self.id, 'username':name, 'password':password}
		user = ColumnFamily(self.client, KEYSPACE, CF_USER)
		user.insert(self.id, cols)
		#add username
		self.username = ColumnFamily(self.client, KEYSPACE, CF_USERNAME)
		self.username.insert(name,{'id':self.id})


	def add_friend(self, friend_id):

		stamp = self._time_stamp()
		#add friend to list
		friends = ColumnFamily(self.client, KEYSPACE, CF_FRIENDS)
		friends.insert(self.id, {friend_id:stamp})
		#add myself to friend`s list
		followers = ColumnFamily(self.client, KEYSPACE, CF_FOLLOWRES)
		followers.insert(friend_id, {self.id:self.stamp})

	def post_tweet(self, body):

		tweet_id = str(uuid.uuid4())
		stamp = self._time_stamp()
		ts = _long(stamp)
		
		#post tweet
		cols = {'id':tweet_id, 'user_id':self.id, 'body':body, '_ts':str(stamp)}
		tweet = ColumnFamily(self.client, KEYSPACE, CF_TWEET)
		tweet.insert(tweet_id, cols)
		#feel Userline
		userline = ColumnFamily(self.client, KEYSPACE, CF_USERLINE)
		userline.insert(self.id, {ts:self.id})
		#feel Timeline
		#timeline = ColumnFamily(self.client, KEYSPACE, CF_TIMELINE)
		#timeline.insert(stamp,tweet_id)

	def _get_id(self, username):
		return self.username.get(username)['id']

	def _time_stamp(self):
		return int(time.time() * 1e6)

	def get_friends_list(self):
		friends = []
		#get
		return friends

	def get_followers_list(self):
		followers = []
		#get
		return followers

	def delete_friend(self):
		#remove from friends
		pass

	def self_destruction(self):
		#delete myself and all my activity
		pass


class Robot:

	users = []
	twits = 0

	_thread = None
	_stop_event = None

	def __init__(self, number):
		self._add_users(number)
		self._thread = threading.Thread(target=self._loop)
		self._thread.daemon = True
		self._stop_event = threading.Event()
																								
	def _add_users(self, number):
		for u in range(number):
			new_user = User(self._generate_name(),self._generate_password())
			self.users.append(new_user)
		print "Added %s users." % number

	def start(self):
		print "Strating.."
		self._thread.start()

	def _loop(self):
		while not self._stop_event.isSet():
			for user in self.users:
				user.post_tweet(self.generate_twit())
				self.twits += 1
				if self._stop_event.isSet():
					print "Stopping"
					return
		print "Stopping."

	def stop(self):
		if self._stop_event:
			self._stop_event.set()
		print "Done."
		print "Total: %s active users wrote %s twits." % ( self.get_users_count(), self.twits)

	def _generate_name(self):
		return "".join([random.choice(string.letters+string.digits) for x in range(8)])

	def _generate_password(self):
		return "".join([random.choice(string.letters+string.digits) for x in range(8)])

	def generate_twit(self,length=139):
		return "".join([random.choice(string.letters+string.digits) for x in range(length)])

	def get_users_count(self):
		return len(self.users)


def _long(i):
	"""
	Packs a long into the expected sequence of bytes that Cassandra expects.
	"""
	return struct.pack('>d', long(i))


if __name__ == "__main__":
	R = Robot(300)
	R.start()
	seconds = 60
	for i in range(1,seconds):
		time.sleep(1)
		print "%d seconds left. %d twits posted (TPS: %d)" % (seconds - i, R.twits, R.twits // i)
	R.stop()



"""
#export PYTHONPATH=/root/twissandra/deps/
#easy_install thrift
#from pycassa import *

>>> from pycassa import *
>>> CLIENT = connect_thread_local(framed_transport=True)
>>> USER = ColumnFamily(CLIENT, 'Twissandra', 'User')
>>> USER.insert('a4a70900-24e1-11df-8924-001ff3591711',{'username':'ericflo'})
1280839194579172L
>>> USER.get('a4a70900-24e1-11df-8924-001ff3591711')
{'username': 'ericflo'}
>>> USER.remove('a4a70900-24e1-11df-8924-001ff3591711')
1280839480802087L
>>> USER.get('a4a70900-24e1-11df-8924-001ff3591711')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "pycassa/columnfamily.py", line 160, in get
    raise NotFoundException()
cassandra.ttypes.NotFoundException: NotFoundException()

>>> USER.insert('a4a70900-24e1-11df-8924-001ff3591711',{'username':'ericflo'})
1280839567392101L
>>> USER.get('a4a70900-24e1-11df-8924-001ff3591711')
{'username': 'ericflo'}
>>> USER.remove('a4a70900-24e1-11df-8924-001ff3591711')
1280839574832068L
>>> USER.get('a4a70900-24e1-11df-8924-001ff3591711')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "pycassa/columnfamily.py", line 160, in get
    raise NotFoundException()
cassandra.ttypes.NotFoundException: NotFoundException()
"""									                                            