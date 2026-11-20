Testing
-------
The SDK provides mock classes for both sync and async clients to facilitate unit testing.
These mocks use ``create_autospec`` with ``spec_set=True`` to provide better type safety and call signature checking.

Object to use as a mock for CogniteClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: cognite.client.testing.CogniteClientMock

Object to use as a mock for AsyncCogniteClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: cognite.client.testing.AsyncCogniteClientMock

Use a context manager to monkeypatch CogniteClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autofunction:: cognite.client.testing.monkeypatch_cognite_client

Use a context manager to monkeypatch AsyncCogniteClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autofunction:: cognite.client.testing.monkeypatch_async_cognite_client
