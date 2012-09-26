
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

		driver._conn._upload_part.assert_called_with(
			'Vault_1',
	   		'LSKGJ236AD36EQ36_e42YT5',
	   		'84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882',
	   		'84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882',	
	   		(1024, 1033),
			'0123456789'
		)
