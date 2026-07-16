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

As part of the **Zero Copy** initiative, the recommended pattern is to read directly from OneLake
and write into the **Cognite Data Model** (instances/nodes) — not into RAW. This keeps Fabric as the
system of record while exposing curated data in CDF without an intermediate RAW copy.

Each external data source is identified by a unique ``external_id`` and stores Azure service principal
credentials (client ID, tenant ID, and client secret) along with the target Fabric workspace and
lakehouse identifiers (GUID or human-readable name).

Permissions
-----------

Managing external data sources requires ``transformationsExternalDataSourcesAcl`` with ``READ`` and
``WRITE``. Running transforms that call ``ext_onelake()`` also requires ``USE`` on that ACL, plus the
usual transformation ACLs and destination ACLs (for example ``dataModelInstancesAcl`` ``WRITE`` when
writing to a Data Model view).

Quickstart
----------

Register a Fabric OneLake Source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use :class:`~cognite.client.data_classes.transformations.OneLakeExternalDataSourceWrite` to create a
source, then call ``upsert()`` to register it in your project:

.. code-block:: python

   from cognite.client import CogniteClient
   from cognite.client.data_classes.transformations import OneLakeExternalDataSourceWrite

   client = CogniteClient()

   # Create the external data source
   source = OneLakeExternalDataSourceWrite(
       external_id="fabric-lakehouse-prod",
       name="Production lakehouse",
       client_id="<azure-app-id>",
       tenant_id="<azure-tenant-uuid>",
       client_secret="<secret>",
       workspace_name="<fabric-workspace-name-or-guid>",
       container_name="<fabric-lakehouse-name-or-guid>",
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

Update Credentials for a Listed Source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Listing returns read models without ``client_secret``. To re-register credentials for an existing source,
convert the listed item to a write model and upsert again:

.. code-block:: python

   sources = client.transformations.external_data_sources.list()
   write_source = sources[0].as_write(client_secret="<secret>")
   client.transformations.external_data_sources.upsert(write_source)

Create and Run a Transform Using OneLake Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the external data source is registered, use it in a transform SQL via the ``ext_onelake()`` function.
Map the result into a **Data Model** view (Zero Copy pattern — read from OneLake, write instances to CDF):

.. code-block:: python

   from cognite.client.data_classes import TransformationDestination, TransformationUpdate, TransformationWrite
   from cognite.client.data_classes.transformations.common import NonceCredentials, ViewInfo

   view = ViewInfo(
       space="my-model-space",
       external_id="Event",
       version="1",
   )
   transform = TransformationWrite(
       external_id="onelake-to-data-model",
       name="OneLake to Data Model",
       destination=TransformationDestination.nodes(view=view, instance_space="my-instance-space"),
       conflict_mode="upsert",
       query="""
       SELECT
           externalId AS externalId,
           description AS name
       FROM ext_onelake('fabric-lakehouse-prod', 'my_table')
       """,
   )

   created = client.transformations.create(transform)
   print(f"Created transform: {created.external_id}")

   # Transformations need session credentials before run()
   session = client.iam.sessions.create()
   nonce = NonceCredentials(session.id, session.nonce, client.config.project)
   client.transformations.update(
       TransformationUpdate(id=created.id)
       .source_nonce.set(nonce)
       .destination_nonce.set(nonce)
   )

   job = client.transformations.run(transformation_id=created.id, wait=True)
   print(f"Job {job.id} finished with status {job.status}")

List Registered Sources
^^^^^^^^^^^^^^^^^^^^^^^

Retrieve all registered external data sources. OneLake sources are returned as
:class:`~cognite.client.data_classes.transformations.OneLakeExternalDataSource` instances; other formats
are returned as forward-compatible read models:

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
