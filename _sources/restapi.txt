HTTP REST API
=============

EVA supports running a HTTP REST API which can be used for monitoring and
controlling. This feature is enabled by providing the ``--rest-server-port``
argument when starting EVA:

.. code-block:: bash

   python3 -m eva --rest-server-port 12345

The above example will start the REST server on ``0.0.0.0:12345``.


RPC Client
----------

To simplify the process of making requests to the API, EVA ships with a ``rpc`` tool:

.. code-block:: bash

   bin/rpc --server <host:port> --help


Health checks
-------------

Make a request to ``/health``. If EVA reponds with a ``2xx`` status code, the
system is functioning correctly. A code of ``555`` signals that the connection
to the Kafka message queue has probably been lost, while any other error code
means that the server is misbehaving or is buggy.

You may also query the health using the RPC tool:

.. code-block:: bash

   bin/rpc health


Read-only requests
------------------

Read-only requests are done with a HTTP ``GET`` request. All read requests are
publicly available. EVA will not expose any sensitive data through GET
requests.


Write requests
--------------

Write requests are done with HTTP ``POST``, ``PUT``, ``PATCH`` and ``DELETE``
requests, and the payload for the request must be included as a JSON object.
The ``Content-Type`` header must be set to ``application/json``.

Furthermore, write requests must be authenticated using a GPG signature. The
GPG key IDs allowed to make requests must be added to the EVA configuration.

.. code-block:: ini

   [eva]
   rest_server = rest.api

   [rest.api]
   class = eva.rest.Server
   gpg_key_ids = 12345678, 90ABCDEF

The client must generate a detached GPG signature of the JSON payload. The
signature is then sent as HTTP headers along with the request. The GPG
signature must be made within 2 seconds of the server time, or else the request
will be rejected. This prevents, to a certain degree, request replay attacks.

If you use the RPC client, all this will be handled for you. For instance, the
following command:

.. code-block:: bash

   ~/eva/bin/rpc control --drain

...will generate the following HTTP conversation:

.. code-block:: http

   > POST /control/drain HTTP/1.1
   > Host: localhost:8080
   > Accept-Encoding: gzip, deflate
   > Accept: */*
   > Authorization: Basic bG9sZmFjZTphYmM=
   > Connection: keep-alive
   > Content-Length: 2
   > Content-Type: application/json
   > User-Agent: python-requests/2.9.1
   > X-EVA-Request-Signature-001: -----BEGIN PGP SIGNATURE-----
   > X-EVA-Request-Signature-002: Version: GnuPG v1
   > X-EVA-Request-Signature-003: 
   > X-EVA-Request-Signature-004: iQIcBAABAgAGBQJYV7nHAAoJEIJRrk/X9SU7lUMQAK+6jOa0JH9UL9LcVX30qgpT
   > X-EVA-Request-Signature-005: K3IFtkZXfbaPMdVtlfzeT8lcretFV93zFE7RrSYfvcXYGC9H6doEsKWX53EdYUeJ
   > X-EVA-Request-Signature-006: 4PGU7DISRX4BBVM0b4+mnbU580JX73NhY1w0f/kXQQCoRIdJ87j+aihSEC7/LaIP
   > X-EVA-Request-Signature-007: 1VlCf9H+R/qU6DmVRTQHrrY29WZxIig+r5vHzQOWijMv+YLEcAqy2c5zorvIuQlR
   > X-EVA-Request-Signature-008: LsX02YHqqK1E6sOTSmJaXe6Se3DOB48N/LfTjRdCLHK4+X8HljHS9/hMyOSw1ebT
   > X-EVA-Request-Signature-009: NVTzJ26Et1Sp4qA3rctaZoqGeIdnGo48WT+I7bzpG0TgtsHhqP/kTApdvimxLos+
   > X-EVA-Request-Signature-010: xn97zJ7PRn4AdOm88RxwWkumTRMFXI9crjb5s0RtnJOiWTjsRkmuR3m+k2EbgHPD
   > X-EVA-Request-Signature-011: e6aLRmUvPqmpZCKio8CHrGCBHm6DM/Xu7WwWyQfuB+8TtXSWysPm+Nr53/7vj+BO
   > X-EVA-Request-Signature-012: cW2jmLPZF9sljWlep5ZKL8uHqBtgs+LytfQEhVobyfdC0fao8Hfpnjh9sb+jQZRE
   > X-EVA-Request-Signature-013: RtqUie2OutJHaixRf3A83qtETaUMp8cpJty31DGaLzTuzSY6ISdzOxy/Hp0apowL
   > X-EVA-Request-Signature-014: BlrEwHuxqyWyJKxS77VmkP59bbtlNh153BZlbjuLvmAb/nXJsR1xs4JB+pVQvp55
   > X-EVA-Request-Signature-015: 0fmKDGP/1+s1WE58xh/r
   > X-EVA-Request-Signature-016: =/zZr
   > X-EVA-Request-Signature-017: -----END PGP SIGNATURE-----
   >
   > {}

   < HTTP/1.0 200 OK
   < Date: Mon, 19 Dec 2016 10:43:19 GMT
   < Server: WSGIServer/0.2 CPython/3.4.3
   < content-type: application/json; charset=UTF-8
   < content-length: 38
   <
   < {"message": "Drain has been enabled."}
