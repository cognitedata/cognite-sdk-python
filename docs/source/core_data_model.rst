Core Data Model
===============
Assets
------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.assets.retrieve
   CogniteClient.assets.retrieve_multiple
   CogniteClient.assets.retrieve_subtree
   CogniteClient.assets.list
   CogniteClient.assets.aggregate
   CogniteClient.assets.aggregate_metadata_keys
   CogniteClient.assets.aggregate_metadata_values
   CogniteClient.assets.search
   CogniteClient.assets.create
   CogniteClient.assets.create_hierarchy
   CogniteClient.assets.delete
   CogniteClient.assets.update

Asset Data classes
^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:
   
   cognite.client.data_classes.assets

Events
------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.events.retrieve
   CogniteClient.events.retrieve_multiple
   CogniteClient.events.list
   CogniteClient.events.aggregate
   CogniteClient.events.aggregate_unique_values
   CogniteClient.events.list
   CogniteClient.events.search
   CogniteClient.events.create
   CogniteClient.events.delete
   CogniteClient.events.update

Events Data classes
^^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:
   
   cognite.client.data_classes.events

Data points
-----------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.time_series.data.retrieve
   CogniteClient.time_series.data.retrieve_arrays
   CogniteClient.time_series.data.retrieve_dataframe
   CogniteClient.time_series.data.retrieve_dataframe_in_tz
   CogniteClient.time_series.data.retrieve_latest
   CogniteClient.time_series.data.insert
   CogniteClient.time_series.data.insert_multiple
   CogniteClient.time_series.data.insert_dataframe
   CogniteClient.time_series.data.delete_range
   CogniteClient.time_series.data.delete_ranges


Data Points Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:
   
   cognite.client.data_classes.datapoints

Files
-----
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

    CogniteClient.files.retrieve
    CogniteClient.files.retrieve_multiple
    CogniteClient.files.list
    CogniteClient.files.aggregate
    CogniteClient.files.search
    CogniteClient.files.create
    CogniteClient.files.upload
    CogniteClient.files.upload_bytes
    CogniteClient.files.retrieve_download_urls
    CogniteClient.files.download
    CogniteClient.files.download_to_path
    CogniteClient.files.download_bytes
    CogniteClient.files.delete
    CogniteClient.files.update

Files Data classes
^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:
   
   cognite.client.data_classes.files


Geospatial
----------
.. note::
   Check https://github.com/cognitedata/geospatial-examples for some complete examples.
.. currentmodule:: cognite.client
.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.geospatial.create_feature_types
   CogniteClient.geospatial.delete_feature_types
   CogniteClient.geospatial.list_feature_types
   CogniteClient.geospatial.retrieve_feature_types
   CogniteClient.geospatial.patch_feature_types
   CogniteClient.geospatial.create_features
   CogniteClient.geospatial.delete_features
   CogniteClient.geospatial.retrieve_features
   CogniteClient.geospatial.update_features
   CogniteClient.geospatial.list_features
   CogniteClient.geospatial.search_features
   CogniteClient.geospatial.stream_features
   CogniteClient.geospatial.aggregate_features
   CogniteClient.geospatial.get_coordinate_reference_systems
   CogniteClient.geospatial.list_coordinate_reference_systems
   CogniteClient.geospatial.create_coordinate_reference_systems
   CogniteClient.geospatial.delete_coordinate_reference_systems
   CogniteClient.geospatial.put_raster
   CogniteClient.geospatial.delete_raster
   CogniteClient.geospatial.get_raster
   CogniteClient.geospatial.compute

Geospatial Data classes
^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:
   
   cognite.client.data_classes.geospatial


Synthetic time series
---------------------
.. currentmodule:: cognite.client
.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.time_series.data.synthetic.query


Time series
-----------
.. currentmodule:: cognite.client
.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.time_series.retrieve
   CogniteClient.time_series.retrieve_multiple
   CogniteClient.time_series.list
   CogniteClient.time_series.aggregate
   CogniteClient.time_series.search
   CogniteClient.time_series.create
   CogniteClient.time_series.delete
   CogniteClient.time_series.update

Time Series Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary:: 
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   cognite.client.data_classes.time_series

Sequences
---------
.. currentmodule:: cognite.client
.. autosummary::
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.sequences.retrieve
   CogniteClient.sequences.retrieve_multiple
   CogniteClient.sequences.list
   CogniteClient.sequences.aggregate
   CogniteClient.sequences.search
   CogniteClient.sequences.create
   CogniteClient.sequences.delete
   CogniteClient.sequences.update
   CogniteClient.sequences.data.retrieve
   CogniteClient.sequences.data.retrieve_dataframe
   CogniteClient.sequences.data.retrieve_latest
   CogniteClient.sequences.data.insert
   CogniteClient.sequences.data.insert_dataframe
   CogniteClient.sequences.data.delete
   CogniteClient.sequences.data.delete_range

Sequence Data classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.sequences
    :members:
    :show-inheritance:
    :exclude-members: Sequence

    .. autoclass:: Sequence
        :noindex:
