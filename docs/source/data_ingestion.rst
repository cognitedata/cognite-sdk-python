Data Ingestion
==============

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

Iterate through rows in a table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.iterate_rows

Insert rows into a table
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.insert

Delete rows from a table
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.delete

Retrieve pandas dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.retrieve_dataframe

Insert pandas dataframe
~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.raw.RawRowsAPI.insert_dataframe


RAW Data classes
^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.raw
    :members:
    :show-inheritance:

Extraction pipelines
--------------------
List extraction pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.list

Create extraction pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.create

Retrieve an extraction pipeline by ID
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.retrieve

Retrieve multiple extraction pipelines by ID
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.retrieve_multiple

Update extraction pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.update

Delete extraction pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelinesAPI.delete


Extraction pipeline runs
------------------------
List runs for an extraction pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineRunsAPI.list

Report new runs
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineRunsAPI.create


Extraction pipeline configs
---------------------------
Get the latest or a specific config revision
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineConfigsAPI.retrieve

List configuration revisions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineConfigsAPI.list

Create a config revision
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineConfigsAPI.create

Revert to an earlier config revision
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.extractionpipelines.ExtractionPipelineConfigsAPI.revert

Extractor Config Data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.extractionpipelines
    :members:
    :show-inheritance:
