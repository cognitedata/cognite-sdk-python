Quickstart
==========
Authenticate
------------
The preferred way to authenticate against the Cognite API is by setting the :code:`COGNITE_API_KEY` environment variable. All examples in this documentation require that the variable has been set.

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>

You can also pass your API key directly to the CogniteClient.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient(api_key="your-key")

Instantiate a new client
------------------------
Use this code to instantiate a client and get your login status. CDF returns an object with
attributes that describe which project and service account your API key belongs to.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `CogniteClient`_ and the functionality it exposes below.

Plot time series
----------------
There are several ways of plotting a time series you have fetched from the API. The easiest is to call
:code:`.plot()` on the returned :code:`TimeSeries` or :code:`TimeSeriesList` objects. By default, this plots the raw
data points for the last 24 hours.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_time_series = c.time_series.retrieve(id=[1, 2])
    >>> my_time_series.plot()

You can also pass arguments to the :code:`.plot()` method to change the start, end, aggregates, and granularity of the
request.

.. code:: python

    >>> my_time_series.plot(start="365d-ago", end="now", aggregates=["avg"], granularity="1d")

The :code:`Datapoints` and :code:`DatapointsList` objects that are returned when you fetch data points, also have :code:`.plot()`
methods you can use to plot the data.

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> my_datapoints = c.datapoints.retrieve(
    ...                     id=[1, 2],
    ...                     start="10d-ago",
    ...                     end="now",
    ...                     aggregates=["max"],
    ...                     granularity="1h"
    ...                 )
    >>> my_datapoints.plot()

.. NOTE::
    To use the :code:`.plot()` functionality you need to install :code:`matplotlib`.

Create an asset hierarchy
-------------------------
To create a root asset (an asset without a parent), omit the parent ID when you post the asset to the API.
To make an asset a child of an existing asset, you must specify a parent ID.

.. code::

    >>> from cognite.client import CogniteClient
    >>> from cognite.client.data_classes import Asset
    >>> c = CogniteClient()
    >>> my_asset = Asset(name="my first asset", parent_id=123)
    >>> c.assets.create(my_asset)

To post an entire asset hierarchy, you can describe the relations within your asset hierarchy
using the :code:`external_id` and :code:`parent_external_id` attributes on the :code:`Asset` object. You can post
an arbitrary number of assets, and the SDK will split the request into multiple requests and create the assets
in the correct order

This example shows how to post a three levels deep asset hierarchy consisting of three assets.

.. code::

    >>> from cognite.client import CogniteClient
    >>> from cognite.client.data_classes import Asset
    >>> c = CogniteClient()
    >>> root = Asset(name="root", external_id="1")
    >>> child = Asset(name="child", external_id="2", parent_external_id="1")
    >>> descendant = Asset(name="descendant", external_id="3", parent_external_id="2")
    >>> c.assets.create([root, child, descendant])

Wrap the .create() call in a try-except to get information if posting the assets fails:

- Which assets were posted. (The request yielded a 201.)
- Which assets may have been posted. (The request yielded 5xx.)
- Which assets were not posted. (The request yielded 4xx, or was a descendant of another asset which may or may not have been posted.)

.. code::

    >>> from cognite.client.exceptions import CogniteAPIError
    >>> try:
    ...     c.create([root, child, descendant])
    >>> except CogniteAPIError as e:
    ...     assets_posted = e.successful
    ...     assets_may_have_been_posted = e.unknown
    ...     assets_not_posted = e.failed

Settings
========
Client configuration
--------------------
You can pass configuration arguments directly to the :code:`CogniteClient` constructor, for example to configure the base url of your requests and additional headers. For a list of all configuration arguments, see the `CogniteClient`_ class definition.

Environment configuration
-------------------------
You can set default configurations with these environment variables:

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>
    $ export COGNITE_BASE_URL = http://<host>:<port>
    $ export COGNITE_MAX_WORKERS = <number-of-workers>
    $ export COGNITE_TIMEOUT = <num-of-seconds>

    $ export COGNITE_MAX_RETRIES = <number-of-retries>
    $ export COGNITE_MAX_CONNECTION_POOL_SIZE = <number-of-connections-in-pool>
    $ export COGNITE_STATUS_FORCELIST = "429,502,503"
    $ export COGNITE_DISABLE_GZIP = "1"

Concurrency and connection pooling
----------------------------------
This library does not expose API limits to the user. If your request exceeds API limits, the SDK splits your
request into chunks and performs the sub-requests in parallel. To control how many concurrent requests you send
to the API, you can either pass the :code:`max_workers` attribute when you instantiate the :code:`CogniteClient` or set the :code:`COGNITE_MAX_WORKERS` environment variable.

If you are working with multiple instances of :code:`CogniteClient`, all instances will share the same connection pool.
If you have several instances, you can increase the max connection pool size to reuse connections if you are performing a large amount of concurrent requests. You can increase the max connection pool size by setting the :code:`COGNITE_MAX_CONNECTION_POOL_SIZE` environment variable.

Extensions and core library
============================
Pandas integration
------------------
The SDK is tightly integrated with the `pandas <https://pandas.pydata.org/pandas-docs/stable/>`_ library.
You can use the :code:`.to_pandas()` method on pretty much any object and get a pandas data frame describing the data.

This is particularly useful when you are working with time series data and with tabular data from the Raw API.

Matplotlib integration
----------------------
You can use the :code:`.plot()` method on any time series or data points result that the SDK returns. The method takes keyword
arguments which are passed on to the underlying matplotlib plot function, allowing you to configure for example the
size and layout of your plots.

You need to install the matplotlib package manually:

.. code:: bash

    $ pip install matplotlib

:code:`cognite-sdk` vs. :code:`cognite-sdk-core`
------------------------------------------------
If your application doesn't require the functionality from the :code:`pandas`
or :code:`numpy` dependencies, you should install the :code:`cognite-sdk-core` library.

The two libraries are exactly the same, except that :code:`cognite-sdk-core` does not specify :code:`pandas`
or :code:`numpy` as dependencies. This means that :code:`cognite-sdk-core` only has a subset
of the features available through the :code:`cognite-sdk` package. If you attempt to use functionality
that :code:`cognite-sdk-core` does not support, a :code:`CogniteImportError` is raised.

API
===
CogniteClient
-------------
.. autoclass:: cognite.client.CogniteClient
    :members:
    :member-order: bysource

Authentication
--------------
Get login status
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.login.LoginAPI.status


Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.login
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Assets
------
Get assets
^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve

List assets
^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.list

Search for assets
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.search

Create assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create

Delete assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.delete

Update assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.update

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.assets
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Events
------
Get events
^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.retrieve

List events
^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.list

Search for events
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.search

Create events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.create

Delete events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.delete

Update events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.update

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.events
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:


Files
-----
Get files metadata
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.retrieve

List files metadata
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.list

Search for files
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

Delete files
^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.delete

Update files metadata
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.update

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.files
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Time series
-----------
Get time series
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.retrieve

List time series
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.list

Search for time series
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.search

Create time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.create

Delete time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.delete

Update time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.update

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.time_series
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Data points
-----------
Get data points
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve

Get pandas dataframe
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve_dataframe

Perform data points queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.query

Get latest
^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve_latest

Insert data points
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert

Insert data points into multiple time series
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert_multiple

Insert pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.insert_dataframe

Delete a range of data points
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.delete_range

Delete ranges of data points
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.delete_ranges


Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.datapoints
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Raw
---
Databases
^^^^^^^^^
List databases
~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.list

Create new databases
~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.create

Delete databases
~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawDatabasesAPI.delete


Tables
^^^^^^
List tables in a database
~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawTablesAPI.list

Create new tables in a database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawTablesAPI.create

Delete tables from a database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawTablesAPI.delete


Rows
^^^^
Get a row from a table
~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.retrieve

List rows in a table
~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.list

Insert rows into a table
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.insert

Delete rows from a table
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.delete

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.raw
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

3D
--
Models
^^^^^^
Retrieve a model by ID
~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.retrieve

List models
~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.list

Create models
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.create

Update models
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.update

Delete models
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDModelsAPI.delete


Revisions
^^^^^^^^^
Retrieve a revision by ID
~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.retrieve

Create a revision
~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.create

List revisions
~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list

Update revisions
~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.update

Delete revisions
~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.delete

Update a revision thumbnail
~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.update_thumbnail

List nodes
~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_nodes

List ancestor nodes
~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_ancestor_nodes


Files
^^^^^
Retrieve a 3D file
~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDFilesAPI.retrieve

Asset mappings
^^^^^^^^^^^^^^
Create an asset mapping
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.create

List asset mappings
~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.list

Delete asset mappings
~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.three_d.ThreeDAssetMappingAPI.delete

..
    Reveal
    ^^^^^^
    Retrieve a revision by ID
    ~~~~~~~~~~~~~~~~~~~~~~~~~
    .. automethod:: cognite.client._api.three_d.ThreeDRevealAPI.retrieve_revision

    List sectors
    ~~~~~~~~~~~~
    .. automethod:: cognite.client._api.three_d.ThreeDRevealAPI.list_sectors

    List nodes
    ~~~~~~~~~~
    .. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_nodes

    List ancestor nodes
    ~~~~~~~~~~~~~~~~~~~
    .. automethod:: cognite.client._api.three_d.ThreeDRevisionsAPI.list_ancestor_nodes

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.three_d
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Identity and access management
------------------------------
Service accounts
^^^^^^^^^^^^^^^^
List service accounts
~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.list

Create service accounts
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.create

Delete service accounts
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.ServiceAccountsAPI.delete


API keys
^^^^^^^^
List API keys
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.list

Create API keys
~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.create

Delete API keys
~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.APIKeysAPI.delete


Groups
^^^^^^
List groups
~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.list

Create groups
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.create

Delete groups
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


Security categories
^^^^^^^^^^^^^^^^^^^
List security categories
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.list

Create security categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.create

Delete security categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.delete


Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.iam
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Exceptions
----------
CogniteAPIError
^^^^^^^^^^^^^^^
.. autoexception:: cognite.client.exceptions.CogniteAPIError

CogniteAPIKeyError
^^^^^^^^^^^^^^^^^^
.. autoexception:: cognite.client.exceptions.CogniteAPIKeyError

CogniteImportError
^^^^^^^^^^^^^^^^^^
.. autoexception:: cognite.client.exceptions.CogniteImportError

CogniteMissingClientError
^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoexception:: cognite.client.exceptions.CogniteMissingClientError

Utils
-----
Convert timestamp to milliseconds since epoch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autofunction:: cognite.client.utils.timestamp_to_ms

Convert milliseconds since epoch to datetime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autofunction:: cognite.client.utils.ms_to_datetime
