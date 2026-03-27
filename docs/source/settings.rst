Settings
========
Client configuration
--------------------
You can pass configuration arguments directly to the :ref:`ClientConfig <class_client_ClientConfig>` constructor, for example
to configure the base url of your requests or any additional headers. For a list of all configuration arguments,
see the :ref:`ClientConfig <class_client_ClientConfig>` class definition.

To initialise an ``AsyncCogniteClient`` or ``CogniteClient``, simply pass this configuration object (an instance of ``ClientConfig``) to it:

.. code:: python

    from cognite.client import AsyncCogniteClient, CogniteClient, ClientConfig
    from cognite.client.credentials import Token
    my_config = ClientConfig(
        client_name="my-client",
        project="myproj",
        cluster="westeurope-1",  # or pass the full 'base_url'
        credentials=Token("verysecret"),
    )

    # Async client (recommended for new code)
    async_client = AsyncCogniteClient(my_config)

    # Sync client (for backward compatibility)
    sync_client = CogniteClient(my_config)

Global configuration
--------------------
You can set global configuration options like this:

.. code:: python

    from cognite.client import global_config, ClientConfig
    from cognite.client.credentials import Token
    global_config.default_client_config = ClientConfig(
        client_name="my-client",
        project="myproj",
        cluster="westeurope-1",  # or pass the full 'base_url'
        credentials=Token("verysecret"),
    )
    global_config.disable_pypi_version_check = True
    global_config.disable_gzip = False
    global_config.disable_ssl = False
    global_config.max_retries = 10
    global_config.max_retry_backoff = 10
    global_config.max_connection_pool_size = 10
    global_config.status_forcelist = {429, 502, 503, 504}

You should **assume that these must be set prior to instantiating** an ``AsyncCogniteClient`` or ``CogniteClient`` in order for them to *take effect*.

Concurrency Settings
--------------------
The SDK allows you to control how many concurrent API requests are made for different categories of APIs
and operation types. This is managed through the ``concurrency_settings`` attribute on ``global_config``.

All concurrency settings apply *per project*, so if you have multiple clients pointing to different CDF projects,
each project gets its own independent concurrency budget. See `Per-project concurrency`_ for more details.

For backend services expected to serve a high volume of requests, you may want to increase the ``max_connection_pool_size``
(default is 20). This is the "global concurrency limiter". See `Connection pooling`_ for more details.

.. note::
    CDF is a shared backend service. Excessive concurrency from one user can degrade performance for others
    in your organization. The default limits are set quite generously to balance throughput with fair resource
    usage. Keep that in mind when adjusting these settings!

API categories
^^^^^^^^^^^^^^
Concurrency is configured separately for three API categories, *but more are likely to be added in the future*.
This happens if specific API endpoints have greatly different performance characteristics (higher- or lower throughput)
that in turn warrant having their own concurrency settings (to unlock better performance, or for protection):

- **general**: Covers all API endpoints except those covered by other categories
- **raw**: Covers the Raw API endpoints
- **datapoints**: Time series datapoints API endpoints
- **data_modeling**: Data Modeling API endpoints (has additional operation types, see below)

Operation types
^^^^^^^^^^^^^^^
Within each API category, you can set separate limits for different operation types:

- **read**: List, retrieve, search, aggregate and other read operations
- **write**: Create, update, upsert operations
- **delete**: Delete operations

The **data_modeling** category has additional operation types to differentiate schema operations from
regular instance operations:

- **search**: Search and aggregation requests for instances
- **read_schema**: Schema read operations (views, data models, containers) and statistics
- **write_schema**: Schema write operations (views, data models, containers)

The **total concurrency budget** is the sum of all limits across all categories and operation types.

.. code:: python

    from cognite.client import global_config

    # Configure concurrency limits before making any API requests
    # (In addition to 'general', there's also: settings.raw and settings.datapoints)
    settings = global_config.concurrency_settings
    settings.general.read = 8
    settings.general.write = 4
    settings.general.delete = 2

    # Data modeling has additional operation types due to its large API interface:
    settings.data_modeling.search = 2
    settings.data_modeling.read_schema = 2
    settings.data_modeling.write_schema = 1

.. warning::
    Concurrency settings **must be configured before making any API requests**. Once any API request is made,
    these settings become frozen and attempting to modify them will raise a ``RuntimeError``.

    This is because the settings are used to initialize semaphores that control concurrent access to the API.
    Once created, these semaphores cannot be resized.

You can check whether settings have been frozen:

.. code:: python

    from cognite.client import global_config

    global_config.concurrency_settings.is_frozen

Per-project concurrency
^^^^^^^^^^^^^^^^^^^^^^^
The concurrency limits apply **per CDF project**. If you have multiple clients pointing to different CDF projects,
each project gets its own independent concurrency budget. This means that if you set ``general.read = 5``, you can have
up to 5 concurrent general read requests *per project*, not 5 total across all projects.

Connection pooling
------------------
If you are working with multiple instances of ``AsyncCogniteClient`` or ``CogniteClient``, all instances will share the same connection pool.
If you have several instances, you can increase the max connection pool size to reuse connections if you are performing a large amount of concurrent requests.
You can increase the max connection pool size by setting the :code:`max_connection_pool_size` config option. It defaults to 20.

The setting should closely match the total concurrency budget (sum of all concurrency limits expected to be in use + a small buffer)
to optimize connection reuse. *Note that you will never see a higher level of concurrency than the connection pool size, even if your
individual concurrency limits are set higher*.

Debug logging
-------------
If you need to inspect the details of the HTTP requests and responses, or monitor the SDK's retry behavior (e.g. during throttling),
you can enable debug logging.

One way is to pass the :code:`debug=True` argument to :ref:`ClientConfig <class_client_ClientConfig>` when you instantiate your client.
Alternatively, you can toggle debug logging on or off dynamically by setting the :code:`debug` attribute on the
:ref:`ClientConfig <class_client_ClientConfig>` object.

.. code:: python

    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import Token
    client = CogniteClient(
        ClientConfig(
            client_name="my-client",
            project="myproj",
            cluster="api",  # or pass the full 'base_url'
            credentials=Token("verysecret"),
            debug=True,
        )
    )
    print(client.config.debug)   # True, requests, responses, and retries are logged to stderr
    client.config.debug = False  # disable debug logging
    client.config.debug = True   # enable debug logging again

Note: Large outgoing or incoming payloads will be truncated to 1000 characters in the logs to avoid overwhelming the log output.

HTTP Request logging
--------------------
Internally this library uses the ``httpx`` library to perform network calls to the Cognite API service endpoints. For authentication and
token management we depend on ``authlib`` and ``msal``. ``msal`` uses the `requests <https://pypi.org/project/requests/>`_ library under
the hood, which in turn is built on `urllib3 <https://pypi.org/project/urllib3/>`_.

If you are enabling DEBUG level logging, please be advised that requests going through ``urllib3`` will not be sanitized at all, meaning
sensitive information such as authentication credentials and sensitive data may be logged. Thus, it is not recommended in production
environments, or where credentials cannot be easily disabled or rotated, or where log data may be accessed by others.
