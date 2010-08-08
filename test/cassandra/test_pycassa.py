'''
Created on Aug 03, 2010

@author: Dmytro Korsakov
'''
#export PYTHONPATH=/root/twissandra/deps/
#easy_install thrift
#from pycassa import *

"""
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


import os
import sys
import uuid
import time
import threading
import string
import random


deps_path = '/root/twissandra/deps/'
if os.path.exists(deps_path):
	sys.path.append(deps_path)
else:
	raise

KEYSPACE = 'Twissandra'
CF_USER = 'User'
CF_USERNAME = 'Username'
CF_FRIENDS = 'Friends'
CF_FOLLOWRES = 'Followers'
CF_TWEET = 'Tweet'
CF_TIMELINE = 'Timeline'
CF_USERLINE = 'Userline'

def connect_thread_local():
	pass
class ColumnFamily():
	def __init__(self):
		pass
	def insert(self):
		pass
	def get(self):
		pass


class User:

	client = None
	id = None
	username = None
	tweet = None
	
	def __init__(self, name, password):
		
		self.client = connect_thread_local(framed_transport=True)
		
		self.id = uuid.uuid4()
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
		
		tweet_id = uuid.uuid4()
		stamp = self._time_stamp()
		#post tweet
		cols = {'id':self.tweet_id, 'user_id':self.id, 'body':body, '_ts':stamp} 
		tweet = ColumnFamily(self.client, KEYSPACE, CF_TWEET)
		tweet.insert(tweet_id, cols)
		#feel Timeline
		timeline = ColumnFamily(self.client, KEYSPACE, CF_TIMELINE)
		timeline.insert(stamp,tweet_id)
		#feel Userline
		userline = ColumnFamily(self.client, KEYSPACE, CF_USERLINE)
		userline.insert(stamp,tweet_id)
		
		
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
		for u in range[number]:
			new_user = User(self._generate_name(),self._generate_password())
			self.users.append(new_user)
		
	def start(self):
		self._thread.start()
	
	def _loop(self):
		while not self._stop_event.isSet():
			for user in self.users:
				user.post_tweet("trololo")
				self.twits += 1
				if self._stop_event.isSet():
					break
	
	def stop(self):
		if self._send_event:
			self._stop_event.set()
		print "Stopped."
		print "Total:"
		print "% active users wrote % twits.", self.get_users_count(), self.twits		
	
	def _generate_name(self):
		return uuid.uuid4()[:-6]
	
	def _generate_password(self):
		return uuid.uuid4()
	
	def generate_twit(self):
		return "".join([random.choice(string.letters+string.digits) for x in range(139)])
		
	def get_users_count(self):
		return len(self.users)
	
		
if __name__ == "__main__":
	pass