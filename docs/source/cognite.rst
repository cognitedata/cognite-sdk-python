Getting Started
===============
Set the environment variable COGNITE_API_KEY. All examples in this documentation require it.

.. code:: bash

    $ export COGNITE_API_KEY = <your-api-key>

Instantiate a client and get your login status like this

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> c = CogniteClient()
    >>> status = c.login.status()

Read more about the `Cognite Client`_ below.

Cognite Client
==============
.. autoclass:: cognite.client.CogniteClient
    :members:
    :member-order: bysource

API
===
Login
-----
Get login status
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.login.LoginAPI.status

Data Classes
^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^^^^^^^^^^^^^^^^
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
^^^^^^^^^^^^^^^^^^^^^
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

Exceptions
==========
.. automodule:: cognite.client.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
