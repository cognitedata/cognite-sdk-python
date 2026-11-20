Geospatial
==========
.. note::
   Check https://github.com/cognitedata/geospatial-examples for some complete examples.

.. currentmodule:: cognite.client

Feature types
-------------
.. autosummary::
   :toctree: generated/ 
   :template: custom-accessor-template.rst

   AsyncCogniteClient.geospatial.create_feature_types
   AsyncCogniteClient.geospatial.delete_feature_types
   AsyncCogniteClient.geospatial.list_feature_types
   AsyncCogniteClient.geospatial.retrieve_feature_types
   AsyncCogniteClient.geospatial.patch_feature_types

Features
--------
.. autosummary::
   :toctree: generated/ 
   :template: custom-accessor-template.rst

   AsyncCogniteClient.geospatial.create_features
   AsyncCogniteClient.geospatial.delete_features
   AsyncCogniteClient.geospatial.retrieve_features
   AsyncCogniteClient.geospatial.update_features
   AsyncCogniteClient.geospatial.list_features
   AsyncCogniteClient.geospatial.search_features
   AsyncCogniteClient.geospatial.stream_features
   AsyncCogniteClient.geospatial.aggregate_features

Reference systems
-----------------
.. autosummary::
   :toctree: generated/ 
   :template: custom-accessor-template.rst

   AsyncCogniteClient.geospatial.get_coordinate_reference_systems
   AsyncCogniteClient.geospatial.list_coordinate_reference_systems
   AsyncCogniteClient.geospatial.create_coordinate_reference_systems


Raster data
-----------
.. autosummary::
   :toctree: generated/ 
   :template: custom-accessor-template.rst

   AsyncCogniteClient.geospatial.put_raster
   AsyncCogniteClient.geospatial.delete_raster
   AsyncCogniteClient.geospatial.get_raster
   AsyncCogniteClient.geospatial.compute

Geospatial Data classes
-----------------------
.. automodule:: cognite.client.data_classes.geospatial
    :members:
    :show-inheritance: