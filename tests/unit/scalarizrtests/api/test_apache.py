__author__ = 'shaitanich'

import os
import unittest
from scalarizr import bus
from scalarizr.api import apache


bus.etc_path = '/etc/scalr'


simple_vhost = '''<VirtualHost *:80>
       ServerAlias www.dima.com
       ServerAdmin dmitry@scalr.com
       DocumentRoot /var/www
       ServerName dima.com
       CustomLog /var/log/http-dima.com-access.log combined
       ScriptAlias /cgi-bin/ /var/www/cgi-bin/'''


simple_vhost_ssl = '''<IfModule mod_ssl.c>
       <VirtualHost *:443>
               ServerName dima.com
               ServerAlias www.dima.com
               ServerAdmin dmitry@scalr.com
               DocumentRoot /var/www
               CustomLog /var/log/http-dima.com-access.log combined

               SSLEngine on
               SSLCertificateFile /etc/scalr/private.d/keys/https.crt
               SSLCertificateKeyFile /etc/scalr/private.d/keys/https.key
               ErrorLog /var/log/http-dima.com-ssl.log

               ScriptAlias /cgi-bin/ /var/www/cgi-bin/
               SetEnvIf User-Agent ".*MSIE.*" nokeepalive ssl-unclean-shutdown
       </VirtualHost>
</IfModule>'''


class ApacheWebServerTest(unittest.TestCase):


    def setUp(self):
        self.webserver = apache.ApacheWebServer()


    def _test_ensure_vhost_dir(self):
        '''
        vhosts_dir = self.webserver.vhosts_dir
        self.assertEqual(vhosts_dir, '/etc/scalr/private.d/vhosts')
        os.rmdir(vhosts_dir)
        self.assertFalse(os.path.exists(vhosts_dir))
        self.assertTrue(os.path.exists(self.webserver.vhosts_dir))
        '''
        pass


    def test_get_server_root(self):
        self.assertEqual('/etc/apache2',self.webserver.server_root)


    def test_APACHE_CONF_PATH(self):
        self.assertTrue('/etc/apache2/apache2.conf', apache.APACHE_CONF_PATH)


    def test_cert_path(self):
        self.assertEqual('/etc/scalr/private.d/keys', self.webserver.cert_path)


    def test_singleton(self):
        self.assertEqual(self.webserver, apache.ApacheWebServer())


    def test_patch_ssl_conf(self):
        #better be implemented as a functional test
        pass

    def test_list_served_vhosts(self):
        self.assertEqual({'*:80': ['/etc/apache2/sites-enabled/000-default']},self.webserver.list_served_vhosts())


class ApacheVirtualHostTest(unittest.TestCase):


    def setUp(self):
        self.vhost = apache.ApacheVirtualHost('dima.com', 80, simple_vhost)
        self.ssl_vhost = apache.ApacheVirtualHost('secure.dima.com', 443, simple_vhost_ssl)


    def test_vhost_path(self):
        self.assertEqual('/etc/scalr/private.d/vhosts/dima.com.vhost.conf', self.vhost.vhost_path)
        self.assertEqual(os.path.dirname(self.vhost.vhost_path), self.vhost.webserver.vhosts_dir)


    def _test_ensure_document_root_paths(self):
        dirs = self.ssl_vhost._get_document_root_paths()
        print dirs


    def test_ensure_vhost(self):
        self.vhost.ensure()
        self.assertTrue(os.path.exists(self.vhost.vhost_path))

if __name__ == '__main__':
    unittest.main()
