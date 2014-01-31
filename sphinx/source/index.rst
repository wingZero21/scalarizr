.. scalarizr-apidoc documentation master file, created by Dmytro Korsakov

Scalarizr API
*************

Базовая информация
==================

Endpoint::

	http://{$hostname}:8010

Protocol: `JSON-RPC <http://jsonrpc.org/>`_

Аутентификация
--------------

Запрос шифруется и подписывается ключом сервера:

Шифорвание::
	
	request = JSON-RPC request
	crypto_key =  Base64 encoded key
	raw_key = base64_decode(crypto_key)
	encrypted_request = crypt(request, algo="des_ede3_cbc", key=raw_key[0:24], iv=raw_key[-8:])

Подпись::

	date = "%a %d %b %Y %H:%M:%S UTC"
	canonical_string = encrypted_request + date
	signature = hmac(raw_key, canonical_string, "sha1")

Пример запроса::

	api_client.storage.create(
	    storage_config={
	        'type': 'ebs', 
	        'size': 1
	    }, 
	    _platform_access_data={
	        'key_id': 'XXXaws_access_key', 
	        'key': 'XXXaws_secret_key'
	    }
	)

JSON-RPC layer::

	{"params": {"_platform_access_data": {"key_id": "XXXaws_access_key", "key": "XXXaws_secret_key"}, "storage_config": {"type": "ebs", "size": 1}}, "method": "create", "id": 1340184043.523402}

HTTP layer::

	POST /storage HTTP/1.1
	Host: 23.22.0.61:8011
	Date: Wed 20 Jun 2012 09:20:43 UTC
	X-Signature: CCl+4bChjhxMJ4XGfvQ5MqL1q6s=
	Accept-Encoding: identity
	User-Agent: Python-urllib/2.7
	Connection: close


	SPtWJrFw680SHyxut73xHf8nfu9piUgipoENF/NYaEEIhPshippxiDxZPFZvsoSJtwYaNrM0UEnNm1Gop3kbYSsA840204hQ9/aiKbyiueugwbQhH0s42TNB8sO88OvM8qssmt7IF+VZw9CG//Jk5dgDAjcKbW87bnLij7+roUSvAsgv5FwuZ5Im2Y4p9zXrCTvFO/M9l0nmzsmM3Be9YioOdmXzHuzNN14ZihC+1AYRGZR47NAepqdWEnWn1Q9OTCwQhgaCFQJMlEWYVXKF9Q==

Мета параметры
++++++++++++++

*_platform_access_data* - Секретные данные доступа к клауд платформе. Описаны в спецификациях каждой клауд платформы. Идентичны *body.platform_access_data* в мессаджинге.



API
===

.. toctree::
   :maxdepth: 2

.. autoclass:: scalarizr.api.apache.ApacheAPI	
	:members: create_vhost,
		update_vhost,
		delete_vhosts,
		reconfigure,
		list_served_virtual_hosts,
		set_default_ssl_certificate,
		get_webserver_statistics,
		start_service,
		stop_service,
		restart_service,
		reload_service,
		configtest

