CogniteClient
=============

The SDK provides two client classes:

* **AsyncCogniteClient** - The primary async client using native ``async/await`` patterns (new in v8)
* **CogniteClient** - The synchronous client for backward compatibility (wraps the async client internally)

Both clients share the same configuration via :ref:`ClientConfig <class_client_ClientConfig>`.

.. _class_client_AsyncCogniteClient:

AsyncCogniteClient
------------------
.. autoclass:: cognite.client.AsyncCogniteClient
    :members:
    :member-order: bysource

.. _class_client_CogniteClient:

CogniteClient (sync)
--------------------
.. autoclass:: cognite.client.CogniteClient
    :members:
    :member-order: bysource

.. _class_client_ClientConfig:
.. autoclass:: cognite.client.config.ClientConfig
    :members:
    :member-order: bysource

.. _class_client_GlobalConfig:
.. autoclass:: cognite.client.config.GlobalConfig
    :members:
    :member-order: bysource
