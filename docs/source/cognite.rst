Quickstart
==========
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

Instantiating a new client
--------------------------
You can instantiate a client and get your login status like this. This will return an object with
attributes describing which project and service account your api-key belongs to.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `Cognite Client`_ and what functionality it exposes below.

Plotting Time Series
--------------------
There are several ways of plotting a time series you have fetched from the API. The easiest is to call
:code:`.plot()` on the returned :code:`TimeSeries` or :code:`TimeSeriesList` objects. This will by default plot the raw
data points for the last 24h.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_time_series = c.time_series.get(id=[1, 2])
    >>> my_time_series.plot()

You can also pass arguments to the plot method to change the start, end, aggregates, and granularity of the
request.

.. code:: python

    >>> my_time_series.plot(start="365d-ago", end="now", aggregates=["avg"], granularity="1d")

The :code:`Datapoints` and :code:`DatapointsList` objects returned when fetching datapoints also have :code:`.plot()`
methods you can use to plot the data.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_datapoints = c.datapoints.get(
    ...                     id=[1, 2],
    ...                     start="10d-ago",
    ...                     end="now",
    ...                     aggregates=["max"],
    ...                     granularity="1h"
    ...                 )
    >>> my_datapoints.plot()

.. NOTE::
    In order to use the :code:`.plot()` functionality you need to install :code:`matplotlib`

Creating an Asset Hierarchy
---------------------------
When posting an asset to the API you may indicate that it is a root asset (has no parent) by not specifying a parent ID.
You can also specify a parent ID, making it a child of an asset already in the API.

.. code::

    >>> from cognite.client import CogniteClient, Asset
    >>> c = CogniteClient()
    >>> my_asset = Asset(name="my first asset", parent_id=123)
    >>> c.assets.create(my_asset)

If you want to post an entire asset hierarchy, you can do this by describing the relations within your asset hierarchy
using the :code:`ref_id` and :code:`parent_ref_id` attributes on the :code:`Asset` object. You can post
an arbitrary number of assets like this, and the SDK will handle splitting this into multiple requests for you, resolving
:code:`parent_ref_ids` as :code:`parent_ids` as it posts the assets.


This example shows how to post an asset hierarchy of depth 3 consisting of three assets.

.. code::

    >>> from cognite.client import CogniteClient, Asset
    >>> c = CogniteClient()
    >>> root = Asset(name="root", ref_id="1")
    >>> child = Asset(name="child", ref_id="2", parent_ref_id="1")
    >>> descendant = Asset(name="descendant", ref_id="3", parent_ref_id="2")
    >>> c.assets.create([root, child, descendant])

Concepts
========
Pandas Integration
------------------
This library is tightly integrated with the `pandas <https://pandas.pydata.org/pandas-docs/stable/>`_ library.
This means that you can use the :code:`.to_pandas()` method on pretty much any object and get a pandas data frame
describing the data.

This is particularly useful when working with time series data and tabular data from the Raw API.

The Global Client
-----------------
The global client is the first client instantiated by the user.

Certain data class methods, such as :code:`TimeSeries(id=...).plot()`, require that an instance of :code:`CogniteClient`
is set on the object. When a an instance of a data class is returned from the :code:`CogniteClient`, the client instance
will be set on the object. If you instantiate a data class yourself on the other hand, the `global client` will be
attached to the instance.

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

Setting Default Environment Configurations
------------------------------------------
Default configurations may be set using the following environment variables

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>
    $ export COGNITE_BASE_URL = http://<host>:<port>
    $ export COGNITE_MAX_RETRIES = <number-of-retries>
    $ export COGNITE_MAX_WORKERS = <number-of-workers>
    $ export COGNITE_TIMEOUT = <num-of-seconds>
    $ export COGNITE_DISABLE_GZIP = "1"

:code:`cognite-sdk` vs. :code:`cognite-sdk-core`
------------------------------------------------
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

Search for Assets
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.search

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

Search for Events
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.search

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

Search for Files
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.search

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

Search for Time Series
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.search

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

3D
--
Models
^^^^^^
Retrieve a model by id
~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.get

List Models
~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.list

Update Models
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.update

Delete Models
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.delete


Revisions
^^^^^^^^^
Retrieve a Revision by ID
~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.get

Create a Revision
~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.create

List Revisions
~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list

Update Revisions
~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.update

Delete Revisions
~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.delete

Update a Revision Thumbnail
~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.update_thumbnail

List Nodes
~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_nodes

List Ancestor Nodes
~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_ancestor_nodes


Files
^^^^^
Retrieve a 3D File
~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDFilesAPI.get

Asset Mappings
^^^^^^^^^^^^^^
Create an Asset Mapping
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.create

List Asset Mappings
~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.list

Delete Asset Mappings
~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.delete


Reveal
^^^^^^
Retrieve a Revision by ID
~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevealAPI.get_revision

List Sectors
~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevealAPI.list_sectors

List Nodes
~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_nodes

List Ancestor Nodes
~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_ancestor_nodes

Data Classes
^^^^^^^^^^^^
.. automodule:: cognite.client._api.three_d
    :members:
    :exclude-members: ThreeDModelsAPI, ThreeDRevisionsAPI, ThreeDFilesAPI, ThreeDAssetMappingAPI, ThreeDRevealAPI, ThreeDAPI
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
----------
.. automodule:: cognite.client.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
