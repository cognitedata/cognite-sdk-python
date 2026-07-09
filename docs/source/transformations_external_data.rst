External Data Sources
======================

.. currentmodule:: cognite.client

.. warning::

   **OneLake is read-only from a transform perspective.** Transforms can read data from Fabric OneLake tables
   via ``ext_onelake()`` SQL, but writing back to OneLake is **not supported**. External data sources are
   credentials and location information that transforms use to access OneLake tables — they are not
   destinations for transform output.

Introduction
------------

External data sources allow transformations to read data from Fabric OneLake by registering OneLake
workspace and lakehouse credentials. Once registered, a transform can access tables in OneLake via
the ``ext_onelake('source-id', 'table_name')`` SQL function.

Each external data source is identified by a unique ``external_id`` and stores Azure service principal
credentials (client ID, tenant ID, and client secret) along with the target Fabric workspace and
lakehouse identifiers.

Quickstart
----------

Register a Fabric OneLake Source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the factory method ``ExternalDataSourceWrite.onelake()`` to create a source, then call ``upsert()``
to register it in your project:

.. code-block:: python

   from cognite.client import CogniteClient
   from cognite.client.data_classes.transformations.external_data import ExternalDataSourceWrite

   client = CogniteClient()

   # Create the external data source
   source = ExternalDataSourceWrite.onelake(
       external_id="fabric-lakehouse-prod",
       name="Production lakehouse",
       client_id="<azure-app-id>",
       tenant_id="<azure-tenant-uuid>",
       client_secret="<secret>",
       workspace_name="<fabric-workspace-guid>",
       container_name="<fabric-lakehouse-guid>",
       data_set_id=123456,
   )

   # Register the source
   registered_source = client.transformations.external_data_sources.upsert(source)
   print(f"Registered source: {registered_source.external_id}")

Verify Source Usability
^^^^^^^^^^^^^^^^^^^^^^^

Before running a transform, verify that the source is accessible and credentials are valid:

.. code-block:: python

   result = client.transformations.external_data_sources.verify_usability("fabric-lakehouse-prod")

   if result.usable_version is not None:
       print("Source is accessible")
   else:
       print("Source cannot be accessed — check credentials")

Create and Run a Transform Using OneLake Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the external data source is registered, use it in a transform SQL via the ``ext_onelake()`` function.
The transform can read from OneLake tables and write results to CDF:

.. code-block:: python

   from cognite.client.data_classes.transformations import (
       Transformation,
       TransformationDestination,
   )

   transform = Transformation(
       external_id="onelake-to-cdf-assets",
       name="OneLake to CDF Assets",
       destination=TransformationDestination.assets(),
       query="""
       SELECT
           name,
           description
       FROM ext_onelake('fabric-lakehouse-prod', 'my_table')
       WHERE active = true
       """,
   )

   created = client.transformations.create(transform)
   print(f"Created transform: {created.external_id}")

   # Run the transform
   job = client.transformations.jobs.run(created.id)
   print(f"Job {job.id} started")

List Registered Sources
^^^^^^^^^^^^^^^^^^^^^^^

Retrieve all registered external data sources:

.. code-block:: python

   sources = client.transformations.external_data_sources.list()
   for source in sources:
       print(f"Source: {source.external_id} ({source.name})")

Delete a Source
^^^^^^^^^^^^^^^

Remove a source when it is no longer needed:

.. code-block:: python

   client.transformations.external_data_sources.delete("fabric-lakehouse-prod")
   print("Source deleted")

API Reference
-------------

TransformationExternalDataAPI
-----------------------------

.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.transformations.external_data_sources

Data Classes
------------

.. automodule:: cognite.client.data_classes.transformations.external_data
    :members:
    :show-inheritance:
