Settings
========
Client configuration
--------------------
You can pass configuration arguments directly to the :ref:`ClientConfig <class_client_ClientConfig>` constructor, for example
to configure the base url of your requests or any additional headers. For a list of all configuration arguments,
see the :ref:`ClientConfig <class_client_ClientConfig>` class definition.

To initialise a ``CogniteClient``, simply pass this configuration object, (an instance of ``ClientConfig``) to it:

.. code:: python

    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import Token
    my_config = ClientConfig(
        client_name="my-client",
        project="myproj",
        credentials=Token("verysecret"),
    )
    client = CogniteClient(my_config)

Global configuration
--------------------
You can set global configuration options like this:

.. code:: python

    from cognite.client import global_config, ClientConfig
    from cognite.client.credentials import Token
    global_config.default_client_config = ClientConfig(
        client_name="my-client", project="myproj", credentials=Token("verysecret")
    )
    global_config.disable_pypi_version_check = True
    global_config.disable_gzip = False
    global_config.disable_ssl = False
    global_config.max_retries = 10
    global_config.max_retry_backoff = 10
    global_config.max_connection_pool_size = 10
    global_config.status_forcelist = {429, 502, 503, 504}

These must be set prior to instantiating a ``CogniteClient`` in order for them to take effect.

Concurrency and connection pooling
----------------------------------
This library does not expose API limits to the user. If your request exceeds API limits, the SDK splits your
request into chunks and performs the sub-requests in parallel. To control how many concurrent requests you send
to the API, you can either pass the :code:`max_workers` attribute to :ref:`ClientConfig <class_client_ClientConfig>` before
you instantiate the :ref:`cognite_client:CogniteClient` from it, or set the global :code:`max_workers` config option.

If you are working with multiple instances of :ref:`cognite_client:CogniteClient`, all instances will share the same connection pool.
If you have several instances, you can increase the max connection pool size to reuse connections if you are performing a large amount of concurrent requests.
You can increase the max connection pool size by setting the :code:`max_connection_pool_size` config option.

Debug logging
-------------
If you need to know the details of the http requests the SDK sends under the hood, you can enable debug logging. One way
is to pass :code:`debug=True` argument to :ref:`ClientConfig <class_client_ClientConfig>`. Alternatively, you can toggle debug
logging on and off by setting the :code:`debug` attribute on the :ref:`ClientConfig <class_client_ClientConfig>` object.

.. code:: python

    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import Token
    client = CogniteClient(
        ClientConfig(
            client_name="my-client",
            project="myproj",
            credentials=Token("verysecret"),
            debug=True,
        )
    )
    print(client.config.debug)   # True, all http request details will be logged
    client.config.debug = False  # disable debug logging
    client.config.debug = True   # enable debug logging again
