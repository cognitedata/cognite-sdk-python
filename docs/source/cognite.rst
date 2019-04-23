Getting Started
===============
Authenticating
--------------
The preferred way of authenticating towards the Cognite API is by setting the environment variable
:code:`COGNITE_API_KEY`. All examples in this documentation require that it has been set.

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>

You may also pass your api-key directly to the CogniteClient.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient(api_key="your-key")

Instantiating a client and getting your login status
----------------------------------------------------
You can instantiate a client and get your login status like this. This will return an object with
attributes describing which project and service account your api-key belongs to.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `Cognite Client`_ and what functionality it exposes below.

FAQs
====
How do I post an asset hierarchy?
---------------------------------
In order to post an asset hierarchy you need to build a tree by setting :code:`ref_id` and :code:`parent_ref_id`
on all assets before passing them to :code:`assets.create()`. These relations will indicate which assets have which parents
within your hierarchy.

What is the global client and what does it do?
----------------------------------------------
The global client is the first client instantiated by the user.

Certain data class methods, such as :code:`TimeSeries(id=...).plot()`, require that an instance of :code:`CogniteClient`
is set on the object. When a an instance of a data class is returned from the CogniteClient, it will attach itself
to the object. If you instantiate a data class yourself, the global client will be attached to the instance.

.. WARNING::
    The global client is not thread-safe and will always be set to the first client you instantiate.
    So beware if you are working against multiple Cognite projects.

You may update the global client by setting it yourself. The SDK will then exhibit the following behaviour:

.. code:: python

    >>> from cognite.client import global_client, CogniteClient
    >>> c1 = CogniteClient()
    >>> assert global_client.get() == c1
    >>> c2 = CogniteClient()
    >>> assert global_client.get() == c1
    >>> global_client.set(c2)
    >>> assert global_client.get() == c2

How can I set default configurations for the SDK?
-------------------------------------------------
Default configurations may be set using the following environment variables

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>
    $ export COGNITE_BASE_URL = http://<host>:<port>
    $ export COGNITE_MAX_RETRIES = <number-of-retries>
    $ export COGNITE_MAX_WORKERS = <number-of-workers>
    $ export COGNITE_TIMEOUT = <num-of-seconds>
    $ export COGNITE_DISABLE_GZIP = "1"

What is the difference between the :code:`cognite-sdk` and :code:`cognite-sdk-core` libraries?
----------------------------------------------------------------------------------------------
The two libraries are exactly the same, except that :code:`cognite-sdk-core` does not specify :code:`pandas`
or :code:`numpy` as dependencies. This means that :code:`cognite-sdk-core` will have only a subset
of the features available through the :code:`cognite-sdk` package. If you attempt to use functionality
that :code:`cognite-sdk-core` does not support, a :code:`CogniteImportError` will be raised.

This is useful if you do not want to include those heavy dependencies in your project.

API
===
Cognite Client
--------------
.. autoclass:: cognite.client.CogniteClient
    :members:
    :member-order: bysource

Authentication
--------------
Get login status
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.login.LoginAPI.status


Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.login
    :members:
    :exclude-members: LoginAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Assets
------
Get Assets
^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.get

List Assets
^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.list

Create Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create

Delete Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.delete

Update Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.update

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.assets
    :members:
    :exclude-members: AssetsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Events
------
Get Events
^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.get

List Events
^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.list

Create Events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.create

Delete Events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.delete

Update Events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.update

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.events
    :members:
    :exclude-members: EventsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:


Files
-----
Get files metadata
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.get

List Files metadata
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.list

Upload a file or directory
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.upload

Upload a string or bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.upload_bytes

Download files to disk
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.download

Download a file as bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.download_bytes

Delete Files
^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.delete

Update Files metadata
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.update

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.files
    :members:
    :exclude-members: FilesAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Time Series
-----------
Get Time Series
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.get

List Time Series
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.list

Create Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.create

Delete Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.delete

Update Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.update

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.time_series
    :members:
    :exclude-members: TimeSeriesAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Datapoints
----------
Get datapoints
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.get

Get pandas dataframe
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.get_dataframe

Perform datapoints queries
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.query

Get latest
^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.get_latest

Insert datapoints
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert

Insert datapoints into multiple time series
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert_multiple

Insert pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert_dataframe

Delete a range of datapoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.delete_range

Delete ranges of datapoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.delete_ranges


Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.datapoints
    :members:
    :exclude-members: DatapointsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Raw
---
List databases
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.list

Create new databases
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.create

Delete databases
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.delete

List tables in a database
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawTablesAPI.list

Create new tables in a database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawTablesAPI.create

Delete tables from a database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawTablesAPI.delete

Get a row from a table
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawRowsAPI.get

List rows in a table
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawRowsAPI.list

Insert rows into a table
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawRowsAPI.insert

Delete rows from a table
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.raw.RawRowsAPI.delete

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.raw
    :members:
    :exclude-members: RawRowsAPI, RawTablesAPI, RawDatabasesAPI, RawAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Identity and Access Managment
-----------------------------
Service Accounts
^^^^^^^^^^^^^^^^
List Service Accounts
~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.list

Create Service Accounts
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.create

Delete Service Accounts
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.delete


API Keys
^^^^^^^^
List API Keys
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.list

Create API Keys
~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.create

Delete API Keys
~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.delete


Groups
^^^^^^
List Groups
~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.list

Create Groups
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.create

Delete Groups
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.delete

List service accounts in a group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.list_service_accounts

Add service accounts to a group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.add_service_account

Remove service accounts from a group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.remove_service_account


Security Categories
^^^^^^^^^^^^^^^^^^^
List Security Categories
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.list

Create Security Categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.create

Delete Security Categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.delete


Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.iam
    :members:
    :exclude-members: SecurityCategoriesAPI, ServiceAccountsAPI, GroupsAPI, APIKeysAPI, IAMAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Exceptions
==========
.. automodule:: cognite.client.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
