
from scalarizr.api import operation

import mock
from nose.tools import eq_, ok_

import pprint


@mock.patch.dict('scalarizr.node.__node__', {
	'messaging': mock.MagicMock()
})
class TestOperation(object):

	def assert_op_result(self):
		from scalarizr.node import __node__
		args, kwds = __node__['messaging'].send.call_args 
		eq_(args[0], 'OperationResult')
		return args, kwds	

	def test_serialize_error(self):
		def fn_raises_error(op):
			def deep():
				op.logger.info('Im deep and going deeper')
				deeper()
			def deeper():
				op.logger.info('Im in deep and continue')
				abyss()
			def abyss():
				op.logger.info('Im in abyss and trying to open file')
				open('/non/existed/file')
			deep()

		op = operation.OperationAPI().create('test_serialize_error', fn_raises_error)
		op.run()

		_, kwds = self.assert_op_result()
		eq_(kwds['body']['name'], 'test_serialize_error')
		eq_(kwds['body']['status'], 'failed')
		eq_(len(kwds['body']['logs']), 4)
		ok_(kwds['body']['logs'][-1].endswith('Reason: [Errno 2] No such file or directory: \'/non/existed/file\''))
		ok_('deep()' in kwds['body']['trace'])
		ok_('deeper()' in kwds['body']['trace'])
		ok_('abyss()' in kwds['body']['trace'])

	def test_serialize(self):
		result_data = {
			'embed': 'data'
		}

		def fn(op):
			return result_data
		op = operation.OperationAPI().create('test_serialize', fn)
		op.run()

		_, kwds = self.assert_op_result()
		eq_(kwds['body']['status'], 'completed')
		eq_(kwds['body']['result'], result_data)

