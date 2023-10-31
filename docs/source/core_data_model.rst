Core Data Model
===============
Assets
------
Retrieve an asset by id
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve

Retrieve multiple assets by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve_multiple

Retrieve an asset subtree
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.retrieve_subtree

List assets
^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.list

Aggregate assets
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate

Aggregate Asset Count
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate_count

Aggregate Asset Value Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate_cardinality_values

Aggregate Asset Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate_cardinality_properties

Aggregate Asset Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate_unique_values

Aggregate Asset Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.aggregate_unique_properties

Search for assets
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.search

Create assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create

Create asset hierarchy
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.create_hierarchy

Delete assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.delete

Filter assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.filter

Update assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.update

Upsert assets
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.assets.AssetsAPI.upsert

Asset Data classes
^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.assets
    :members:
    :show-inheritance:

Events
------
Retrieve an event by id
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.retrieve

Retrieve multiple events by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.retrieve_multiple

List events
^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.list

Aggregate events
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate

Aggregate Event Count
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate_count

Aggregate Event Values Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate_cardinality_values

Aggregate Event Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate_cardinality_properties

Aggregate Event Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate_unique_values

Aggregate Event Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.aggregate_unique_properties

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

Upsert events
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.upsert

Filter events
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.events.EventsAPI.filter

Events Data classes
^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.events
    :members:
    :show-inheritance:

Data points
-----------
.. warning::
    TimeSeries unit support is a new feature:
      * The API specification is in beta.
      * The SDK implementation is in alpha.

    Unit conversion is implemented in the Datapoints APIs with the parameters `target_unit` and `target_unit_system` in
    the retrieve methods below. It is only the use of these arguments that is in alpha. Using the methods below
    without these arguments is stable.

    Thus, breaking changes may occur without further notice, see :ref:`appendix-alpha-beta-features` for more information.


Retrieve datapoints
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve

Retrieve datapoints as numpy arrays
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve_arrays

Retrieve datapoints in pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve_dataframe

Retrieve datapoints in time zone in pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints.DatapointsAPI.retrieve_dataframe_in_tz

Retrieve latest datapoint
^^^^^^^^^^^^^^^^^^^^^^^^^
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


Data Points Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.datapoints
    :members:
    :show-inheritance:

Files
-----
Retrieve file metadata by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.retrieve

Retrieve multiple files' metadata by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.retrieve_multiple

List files metadata
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.list

Aggregate files metadata
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.aggregate

Search for files
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.search

Create file metadata
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.create

Upload a file or directory
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.upload

Upload a string or bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.upload_bytes

Retrieve download urls
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.retrieve_download_urls

Download files to disk
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.download

Download a single file to a specific path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.download_to_path

Download a file as bytes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.download_bytes

Delete files
^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.delete

Update files metadata
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.files.FilesAPI.update

Files Data classes
^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.files
    :members:
    :show-inheritance:



Geospatial
----------
.. note::
   Check https://github.com/cognitedata/geospatial-examples for some complete examples.

Create feature types
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.create_feature_types

Delete feature types
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.delete_feature_types

List feature types
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.list_feature_types

Retrieve feature types
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.retrieve_feature_types

Update feature types
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.patch_feature_types

Create features
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.create_features

Delete features
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.delete_features

Retrieve features
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.retrieve_features

Update features
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.update_features

List features
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.list_features

Search features
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.search_features

Stream features
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.stream_features

Aggregate features
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.aggregate_features

Get coordinate reference systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.get_coordinate_reference_systems

List coordinate reference systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.list_coordinate_reference_systems

Create coordinate reference systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.create_coordinate_reference_systems


Put raster data
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.put_raster

Delete raster data
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.delete_raster

Get raster data
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.get_raster

Compute
^^^^^^^
.. automethod:: cognite.client._api.geospatial.GeospatialAPI.compute

Geospatial Data classes
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.geospatial
    :members:
    :show-inheritance:


Synthetic time series
---------------------

Calculate the result of a function on time series
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.synthetic_time_series.SyntheticDatapointsAPI.query


Time series
-----------

.. warning::
    TimeSeries unit support is a new feature:
      * The API specification is in beta.
      * The SDK implementation is in alpha.

    Unit is implemented in the TimeSeries APIs with the parameters `unit_external_id` and `unit_quantity` in
    the methods below. It is only the use of these arguments that is in alpha. Using the methods below
    without these arguments is stable.

    Thus, breaking changes may occur without further notice, see :ref:`appendix-alpha-beta-features` for more information.


Retrieve a time series by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.retrieve

Retrieve multiple time series by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.retrieve_multiple

List time series
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.list

Aggregate time series
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate

Aggregate Time Series Count
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate_count

Aggregate Time Series Values Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate_cardinality_values

Aggregate Time Series Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate_cardinality_properties

Aggregate Time Series Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate_unique_values

Aggregate Time Series Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.aggregate_unique_properties

Search for time series
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.search

Create time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.create

Delete time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.delete

Filter time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.filter

Update time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.update

Upsert time series
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.time_series.TimeSeriesAPI.upsert


Time Series Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.time_series
    :members:
    :show-inheritance:


Sequences
---------

Retrieve a sequence by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.retrieve

Retrieve multiple sequences by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.retrieve_multiple

List sequences
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.list

Aggregate sequences
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate

Aggregate Sequences Count
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_count

Aggregate Sequences Value Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_cardinality_values

Aggregate Sequences Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_cardinality_properties

Aggregate Sequences Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_unique_values

Aggregate Sequences Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_unique_properties

Search for sequences
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.search

Create a sequence
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.create

Delete sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.delete

Filter sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.filter


Update sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.update

Upsert sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.upsert


Retrieve data
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.retrieve

Insert rows into a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.insert

Insert a pandas dataframe into a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.insert_dataframe

Delete rows from a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.delete

Delete a range of rows from a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.delete_range

Sequence Data classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.sequences
    :members:
    :show-inheritance:
    :exclude-members: Sequence

    .. autoclass:: Sequence
        :noindex:

Data Point Subscriptions
---------------------------

.. warning::
    DataPoint Subscription is a new feature:
      * The API specification is in beta.
      * The SDK implementation is in alpha.

    Thus, breaking changes may occur without further notice, see :ref:`appendix-alpha-beta-features` for more information.


Create data point subscriptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.create

Retrieve a data point subscription by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.retrieve

List data point subscriptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.list

List member time series of subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.list_member_time_series

Iterate over subscriptions data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.iterate_data

Update data point subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.update

Delete data point subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.datapoints_subscriptions.DatapointsSubscriptionAPI.delete

Data Point Subscription classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.datapoints_subscriptions
    :members:
    :show-inheritance:
