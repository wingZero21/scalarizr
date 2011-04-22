'''
Created on Apr 6, 2011

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.messaging import Messages, Queues
from scalarizr.handlers import Handler, script_executor
from scalarizr.util import system2, disttool, dicts
from scalarizr.queryenv import Script
import shutil
import sys


import os
import logging
import urllib2
from urlparse import urlparse
import mimetypes


class SourceError(BaseException):
	pass
class UndefinedSourceError(SourceError):
	pass

def get_handlers():
	return (DeploymentHandler(), )

class DeploymentHandler(Handler):

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._log_hdlr = DeployLogHandler()
		self._script_executor = None

	def _exec_script(self, name=None, body=None, exec_timeout=None):
		if not self._script_executor:
			self._script_executor = script_executor.get_handlers()[0]
			
		self._logger.info('Executing %s script', name)
		kwargs = dict(name=name, body=body, exec_timeout=exec_timeout or 3600)
		self._script_executor.exec_scripts_on_event(scripts=(Script(**kwargs), ))
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.DEPLOY
	
	def on_Deploy(self, message):
		try:
			msg_body = dicts.encode(message.body, encoding='ascii')
						
			assert 'deploy_task_id' in msg_body, 'deploy task is undefined'
			assert 'source' in msg_body, 'source is undefined'
			assert 'type' in msg_body['source'], 'source type is undefined'
			assert 'remote_path' in msg_body, 'remote path is undefined'
			assert 'body' in msg_body['pre_deploy_routines'] if 'pre_deploy_routines' in msg_body else True
			assert 'body' in msg_body['post_deploy_routines'] if 'post_deploy_routines' in msg_body else True

			self._log_hdlr.deploy_task_id = msg_body['deploy_task_id']
			self._logger.addHandler(self._log_hdlr)

			src_type = msg_body['source'].pop('type')
			src = Source.from_type(src_type, **msg_body['source'])
			if msg_body.get('pre_deploy_routines') and msg_body['pre_deploy_routines'].get('body'):
				self._exec_script(name='PreDeploy', **msg_body['pre_deploy_routines'])
			src.update(msg_body['remote_path'])
			if msg_body.get('post_deploy_routines') and msg_body['post_deploy_routines'].get('body'):
				self._exec_script(name='PostDeploy', **msg_body['post_deploy_routines'])
			
			self.send_message(
				Messages.DEPLOY_RESULT, 
				dict(
					status='ok', 
					deploy_task_id=msg_body['deploy_task_id']
				)
			)
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			self.send_message(
				Messages.DEPLOY_RESULT, 
				dict(
					status='error', 
					last_error=str(e), 
					deploy_task_id=msg_body['deploy_task_id']
				)
			)
			
		finally:
			self._logger.removeHandler(self._log_hdlr)


class Source(object):
	def update(self, workdir):
		raise NotImplementedError()
	
	@staticmethod
	def from_type(srctype, **init_kwargs):
		clsname = srctype[0].upper() + srctype.lower()[1:] + 'Source'
		assert clsname in globals(), 'implementation class %s of source type %s is undefined' % (clsname, srctype)
		return globals()[clsname](**init_kwargs)

class SvnSource(Source):
	EXECUTABLE = '/usr/bin/svn'
	
	def __init__(self, url=None, user=None, password=None, executable=None):
		self._logger = logging.getLogger(__name__)
		self.url = url
		self.user = user
		self.password = password
		self.executable = self.EXECUTABLE
		
	def update(self, workdir):
		if not os.access(self.executable, os.X_OK):
			self._logger.info('Installing Subversion SCM...')
			if disttool.is_debian_based():
				system2(('apt-get', '-y', 'install', 'subversion'))
			elif disttool.is_redhat_based():
				system2(('yum', '-y', 'install', 'subversion'))
			else:
				raise SourceError('Cannot install Subversion. Unknown distribution %s' % 
								str(disttool.linux_dist()))
		
		do_update = False
		if os.path.exists(os.path.join(workdir, '.svn')):
			out = system2(('svn', 'info', workdir))[0]
			try:
				svn_url = filter(lambda line: line.startswith('URL:'), out.split('\n'))[0].split(':', 1)[1].strip()
			except IndexError:
				raise SourceError('Cannot extract Subversion URL. Text:\n %s', out)
			if svn_url != self.url:
				raise SourceError('Working copy %s is checkouted from different repository %s' % (workdir, svn_url))
			do_update = True
			
		args = [
			'svn' , 
			'update' if do_update else 'co'
		]
		if self.user and self.password:
			args += [
				'--username', self.user,
				'--password', self.password,
			]
		if args[1] == 'co':
			args += [self.url]
		args += [workdir]
		
		self._logger.info('Updating source from %s into working dir %s', self.url, workdir)		
		out = system2(args)[0]
		self._logger.info(out)


class GitSource(Source):
	EXECUTABLE = '/usr/bin/git'	
	
	def __init__(self, url=None, ssl_cert=None, ssl_pk=None, ssl_ca_info=None, ssl_no_verify=None, executable=None):
		self._logger = logging.getLogger(__name__)
		self.url = url
		self.ssl_cert = ssl_cert
		self.ssl_pk = ssl_pk
		self.ssl_ca_info = ssl_ca_info
		self.ssl_no_verify = ssl_no_verify
		self.executable = executable or self.EXECUTABLE

	def update(self, workdir):
		if not os.access(self.executable, os.X_OK):
			self._logger.info('Installing Git SCM...')
			if disttool.is_debian_based():
				system2(('apt-get', '-y', 'install', 'git-core'))
			elif disttool.is_redhat_based():
				system2(('yum', '-y', 'install', 'git'))
			else:
				raise SourceError('Cannot install Git. Unknown distribution %s' % 
								str(disttool.linux_dist()))
		
		env = {}
		cnf = bus.cnf		
		if self.ssl_cert and self.ssl_pk:
			env['GIT_SSL_CERT'] = cnf.write_key('git-client.crt', self.ssl_cert)
			env['GIT_SSL_KEY'] = cnf.write_key('git-client.key', self.ssl_pk)
		if self.ssl_ca_info:
			env['GIT_SSL_CAINFO'] = cnf.write_key('git-client-ca.crt', self.ssl_ca_info)
		if self.ssl_no_verify:
			env['GIT_SSL_NO_VERIFY'] = '1'
		
		try:
			self._logger.info('Updating source from %s into working dir %s', self.url, workdir)		
			if os.path.exists(os.path.join(workdir, '.git')):
				out = system2(('git', 'pull'), cwd=workdir, env=env)[0]
			else:
				out = system2(('git', 'clone', self.url, workdir), env=env)[0]
			self._logger.info(out)
			
		finally:
			for var in ('GIT_SSL_CERT', 'GIT_SSL_KEY', 'GIT_SSL_CAINFO'):
				if var in env:
					os.remove(env[var])
		
		

class HttpSource(Source):
	def __init__(self, url=None):
		self._logger = logging.getLogger(__name__)
		self.url = url

	def update(self, workdir):
		purl = urlparse(self.url)
		
		self._logger.info('Downloading %s', self.url)
		try:
			hdlrs = [urllib2.HTTPRedirectHandler()]
			if purl.scheme == 'https':
				hdlrs.append(urllib2.HTTPSHandler())
			opener = urllib2.build_opener(*hdlrs)
			resp = opener.open(self.url)
		except urllib2.URLError, e:
			raise SourceError('Downloading %s failed. %s' % (self.url, e))
		
		tmpdst = os.path.join('/tmp', os.path.basename(purl.path))
		fp = open(tmpdst, 'w+')
		num_read = 0
		while True:
			buf = resp.read(8192)
			if not buf:
				break
			num_read += len(buf)
			self._logger.debug('%d bytes downloaded', num_read)
			fp.write(buf)
		fp.close()
		self._logger.info('File saved as %s', tmpdst)

		try:
			mime = mimetypes.guess_type(tmpdst)
									
			if mime[0] in ('application/x-tar', 'application/zip'):
				unar = None					
				if mime[0] == 'application/x-tar':
					unar = ['tar']
					if mime[1] == 'gzip':
						unar += ['-xzf']
					elif mime[2] in ('bzip', 'bzip2'):
						unar += ['-xjf']
					else:
						raise UndefinedSourceError()
					unar += [tmpdst, '-C', workdir]
				
				elif mime[0] == 'application/zip':
					unar = ['unzip', tmpdst, '-d', workdir]
				else:
					raise UndefinedSourceError('Unexpected archive format %s' % str(mime))						

				self._logger.info('Extracting source from %s into %s', tmpdst, workdir)
				out = system2(unar)[0]
				self._logger.info(out)
			else:
				self._logger.info('Moving source from %s to %s', tmpdst, workdir)
				shutil.move(tmpdst, workdir)
			
		except:
			exc = sys.exc_info()
			if isinstance(exc[0], SourceError):
				raise
			raise SourceError, exc[1], exc[2]
		finally:
			if os.path.exists(tmpdst):
				os.remove(tmpdst)
			
			
class DeployLogHandler(logging.Handler):
	def __init__(self, deploy_task_id=None):
		logging.Handler.__init__(self)
		self.deploy_task_id = deploy_task_id
		self._msg_service = bus.messaging_service
		
	def emit(self, record):
		msg = self._msg_service.new_message(Messages.DEPLOY_LOG, body=dict(
			deploy_task_id = self.deploy_task_id,
			message = str(record.msg) % record.args if record.args else str(record.msg)
		))
		self._msg_service.get_producer().send(Queues.LOG, msg)