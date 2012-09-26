
import mock

from scalarizr.storage2.cloudfs import glacier


@mock.patch.object(glacier.GlacierFilesystem, '_connect_glacier')
class TestGlacierFilesystem(object):

	def test_multipart_init(self, *args):
		driver = glacier.GlacierFilesystem()
		driver.multipart_init('glacier://Vault_1/', 1024)

		driver._conn.initiate_multipart_upload.assert_called_with('Vault_1', 1024, None)

	def test_multipart_put(self, *args):
		driver = glacier.GlacierFilesystem()
		driver.multipart_init('glacier://Vault_1/', 1024)
		driver.multipart_put('LSKGJ236AD36EQ36_e42YT5', 1, '0123456789')

		driver._conn.upload_part.assert_called_with(
			'Vault_1',
	   		'LSKGJ236AD36EQ36_e42YT5',
	   		'84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882',
	   		'84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882',	
	   		(1024, 1033),
			'0123456789'
		)

	def test_multipart_complete(self, *args):
		driver = glacier.GlacierFilesystem()
		driver.multipart_init('glacier://Vault_1/', 1024)
		driver.multipart_put('LSKGJ236AD36EQ36_e42YT5', 0, '0123456789')
		driver.multipart_put('LSKGJ236AD36EQ36_e42YT5', 1, '0123456789')
		driver.multipart_complete('LSKGJ236AD36EQ36_e42YT5') 

		driver._conn.complete_multipart_upload.assert_called_with(
			'Vault_1',
	   		'LSKGJ236AD36EQ36_e42YT5',
			'3f9b42f36cfc0aec3d720aeb2af77f46b84106e0f3f62c28cc3286530c27496a',
			20
		)

	def test_multipart_abort(self, *args):
		driver = glacier.GlacierFilesystem()
		driver.multipart_init('glacier://Vault_1/', 1024)
		driver.multipart_abort('LSKGJ236AD36EQ36_e42YT5')

		driver._conn.abort_multipart_upload.assert_called_with('Vault_1', 'LSKGJ236AD36EQ36_e42YT5')
