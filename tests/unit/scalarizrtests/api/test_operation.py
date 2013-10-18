
from scalarizr.api import operation

import mock
from nose.tools import eq_


@mock.patch.dict('scalarizr.node.__node__', {
	'messaging': mock.MagicMock()
})
class TestOperation(object):
	def test_serialize_error(self):
		def fn_raises_error(op):
			def deep():
				deeper()
			def deeper():
				abyss()
			def abyss():
				open('/non/existed/file')
			deep()

		op = operation.OperationAPI().create('read-file', fn_raises_error)
		op.execute()

		from scalarizr.node import __node__
		args, kwds = __node__['messaging'].send.call_args 
		eq_(args[0], 'OperationResult')
		eq_(kwds['body']['status'], 'failed')
		print kwds['body']

	def test_serialize_logs(self):
		pass