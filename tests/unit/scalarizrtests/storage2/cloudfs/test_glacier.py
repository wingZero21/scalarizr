
import mock

from scalarizr.storage2.cloudfs import glacier


@mock.patch.object(glacier.GlacierFilesystem, '_connect_glacier')
class TestGlacierFilesystem(object):

	def test_multipart_init(self):
		driver = glacier.GlacierFilesystem()
		driver.multipart_init('glacier://Vault_1/', 1024)

		driver._conn.initiate_multipart_upload.assert_called_with('Vault_1', 1024, None)

	
