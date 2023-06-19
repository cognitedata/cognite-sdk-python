Data Ingestion
==============

Raw
---
Databases
^^^^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.raw.databases.list
   CogniteClient.raw.databases.create
   CogniteClient.raw.databases.delete


Tables
^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.raw.tables.list
   CogniteClient.raw.tables.create
   CogniteClient.raw.tables.delete


Rows
^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.raw.rows.retrieve
   CogniteClient.raw.rows.list
   CogniteClient.raw.rows.insert
   CogniteClient.raw.rows.delete
   CogniteClient.raw.rows.retrieve_dataframe
   CogniteClient.raw.rows.insert_dataframe


RAW Data classes
^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

   Database
   DatabaseList
   Row 
   RowList
   Table
   TableList


Extraction pipelines
--------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.extraction_pipelines.list
   CogniteClient.extraction_pipelines.create
   CogniteClient.extraction_pipelines.retrieve
   CogniteClient.extraction_pipelines.retrieve_multiple
   CogniteClient.extraction_pipelines.update
   CogniteClient.extraction_pipelines.delete


Extraction pipeline runs
------------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.extraction_pipelines.runs.list
   CogniteClient.extraction_pipelines.runs.create


Extraction pipeline configs
---------------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.extraction_pipelines.config.retrieve
   CogniteClient.extraction_pipelines.config.list
   CogniteClient.extraction_pipelines.config.create
   CogniteClient.extraction_pipelines.config.revert

Extractor Config Data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

   ExtractionPipelineContact
   ExtractionPipeline
   ExtractionPipelineUpdate
   ExtractionPipelineList
   ExtractionPipelineRun
   ExtractionPipelineRunList
   extractionpipelines.StringFilter
   extractionpipelines.ExtractionPipelineRunFilter
   ExtractionPipelineConfigRevision
   ExtractionPipelineConfig
   ExtractionPipelineConfigRevisionList
