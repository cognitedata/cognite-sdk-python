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
.. automethod:: cognite.client.api.login.LoginAPI.status

Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.login
    :members:
    :exclude-members: LoginAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Assets
------
Get Assets
^^^^^^^^^^
.. automethod:: cognite.client.api.assets.AssetsAPI.get

List Assets
^^^^^^^^^^^
.. automethod:: cognite.client.api.assets.AssetsAPI.list

Create Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.assets.AssetsAPI.create

Delete Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.assets.AssetsAPI.delete

Update Assets
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.assets.AssetsAPI.update

Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.assets
    :members:
    :exclude-members: AssetsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Events
------
Get Events
^^^^^^^^^^
.. automethod:: cognite.client.api.events.EventsAPI.get

List Events
^^^^^^^^^^^
.. automethod:: cognite.client.api.events.EventsAPI.list

Create Events
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.events.EventsAPI.create

Delete Events
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.events.EventsAPI.delete

Update Events
^^^^^^^^^^^^^
.. automethod:: cognite.client.api.events.EventsAPI.update

Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.events
    :members:
    :exclude-members: EventsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:


Files
-----
Get files metadata
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.get

List Files metadata
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.list

Upload a file or directory
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.upload

Upload a string or bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.upload_bytes

Download files to disk
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.download

Download a file as bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.download_bytes

Delete Files
^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.delete

Update Files metadata
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.files.FilesAPI.update

Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.files
    :members:
    :exclude-members: FilesAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Time Series
-----------
Get Time Series
^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.time_series.TimeSeriesAPI.get

List Time Series
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.time_series.TimeSeriesAPI.list

Create Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.time_series.TimeSeriesAPI.create

Delete Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.time_series.TimeSeriesAPI.delete

Update Time Series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.time_series.TimeSeriesAPI.update

Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.time_series
    :members:
    :exclude-members: TimeSeriesAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Datapoints
----------
Get datapoints
^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.get

Get pandas dataframe
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.get_dataframe

Perform datapoints queries
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.query

Get latest
^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.get_latest

Insert datapoints
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.insert

Insert datapoints into multiple time series
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.insert_multiple

Insert pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.insert_dataframe

Delete a range of datapoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.delete_range

Delete ranges of datapoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client.api.datapoints.DatapointsAPI.delete_ranges


Data Classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.api.datapoints
    :members:
    :exclude-members: DatapointsAPI
    :undoc-members:
    :show-inheritance:
    :inherited-members:

Exceptions
==========
.. automodule:: cognite.client.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
