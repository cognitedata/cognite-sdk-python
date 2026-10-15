External Data Sources
=====================

.. note::
    This API is in public beta. The contract may change before general availability.

An external data source is a reusable, named connection that lets a transformation read from storage
outside Cognite Data Fusion (CDF). You configure the credentials and the location once, then reference the
source from transformation SQL by its external ID. Microsoft Fabric OneLake is currently the supported
system.

Using this API requires the Fabric connector to be enabled for the project. Contact
`Cognite Support <https://support.cognite.com/>`_ to have it enabled.

.. warning::
    Store the client secret securely, for example in an environment variable or a secret manager. All the
    examples below read it from the environment rather than hard-coding it.

Concepts
--------

* **Format** — the external system the source connects to. ``one_lake`` is the supported value, and each
  format has its own pair of data classes:
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSourceWrite` for
  writing and :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSource`
  for reading.
* **Credentials** — the Microsoft Entra ID application credentials, held by
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeCredentialsWrite`. The client
  secret is write-only: it is stored encrypted and never returned when you read a data source, which is why
  the read model, :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeCredentials`, has
  no ``client_secret``.
* **Location** — where in OneLake the source points, held by
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeLocationDescription`. Both IDs
  are GUIDs from the Fabric portal: the workspace ID under *Workspace settings > Workspace ID* and the
  lakehouse ID under *Lakehouse settings > Item ID*.
* **Data set scoping** — an optional ``data_set_id`` that scopes access to the data source through CDF data
  set permissions.

Permissions
-----------

Every operation is governed by ``transformationsExternalDataSourcesAcl``, available in the SDK as
``TransformationsExternalDataSourcesAcl``:

========================================= ==========
Operation                                 Action
========================================= ==========
``list()``                                ``READ``
``create()``                              ``WRITE``
``delete()``                              ``WRITE``
``verify_usability()``                    ``USE``
========================================= ==========

Running a transformation that reads from a data source also requires ``USE``, in addition to the usual
transformation and destination capabilities.

Register a data source
----------------------

Creating a data source requires a unique external ID. If an external ID you submit already exists for
the project, the request is rejected with a 409 and no items are created — there is no in-place update.
To modify an existing data source, delete it and create a replacement (see `Rotate credentials`_):

.. code-block:: python

    import os

    from cognite.client import CogniteClient
    from cognite.client.data_classes.transformations.externaldata import (
        OneLakeCredentialsWrite,
        OneLakeExternalDataSourceWrite,
        OneLakeLocationDescription,
        OneLakeSettingsWrite,
    )

    client = CogniteClient()

    data_source = OneLakeExternalDataSourceWrite(
        external_id="fabric-lakehouse-prod",
        name="Production lakehouse",
        data_set_id=123456,
        settings=OneLakeSettingsWrite(
            credentials=OneLakeCredentialsWrite(
                client_id=os.environ["ONELAKE_CLIENT_ID"],
                tenant_id=os.environ["ONELAKE_TENANT_ID"],
                client_secret=os.environ["ONELAKE_CLIENT_SECRET"],
            ),
            location_description=OneLakeLocationDescription(
                workspace_id=os.environ["ONELAKE_WORKSPACE_ID"],
                container_id=os.environ["ONELAKE_CONTAINER_ID"],
            ),
        ),
    )
    registered = client.transformations.external_data_sources.create(data_source)

Verify that a data source is usable
-----------------------------------

Before you wire a data source into a transformation, check that CDF can reach the storage location with the
stored credentials. That way you find configuration problems here instead of in a failed transformation job:

.. code-block:: python

    usability = client.transformations.external_data_sources.verify_usability("fabric-lakehouse-prod")

    if not usability.is_usable:
        raise RuntimeError("The data source is missing, or its credentials do not grant access")

Read OneLake tables from transformation SQL
-------------------------------------------

A registered data source is referenced from transformation SQL through the ``ext_onelake()`` table-valued
function, which takes the external ID of the data source and the table name, plus an optional schema name:

.. code-block:: sql

    SELECT * FROM ext_onelake('fabric-lakehouse-prod', 'my_table')
    SELECT * FROM ext_onelake('fabric-lakehouse-prod', 'my_table', 'my_schema')

Data read this way can be written to any transformation destination. This example writes it into a data
model view:

.. code-block:: python

    from cognite.client.data_classes import TransformationDestination, TransformationWrite
    from cognite.client.data_classes.transformations.common import NonceCredentials, ViewInfo

    session = client.iam.sessions.create()
    nonce = NonceCredentials(session.id, session.nonce, client.config.project)

    transformation = client.transformations.create(
        TransformationWrite(
            external_id="onelake-to-data-model",
            name="OneLake to data model",
            query="""
                SELECT
                    externalId AS externalId,
                    description AS name
                FROM ext_onelake('fabric-lakehouse-prod', 'my_table')
            """,
            destination=TransformationDestination.nodes(
                view=ViewInfo(space="my-model-space", external_id="MyView", version="v1"),
                instance_space="my-instance-space",
            ),
            conflict_mode="upsert",
            source_nonce=nonce,
            destination_nonce=nonce,
        )
    )
    job = client.transformations.run(transformation_id=transformation.id)

See :doc:`transformations` for the rest of the transformation API, including the other ways of supplying
credentials.

List data sources
-----------------

Listing returns read models, so OneLake data sources come back as
:class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSource`:

.. code-block:: python

    for data_source in client.transformations.external_data_sources.list(limit=None):
        print(data_source.external_id, data_source.settings.location_description.workspace_id)

To iterate without holding every data source in memory, call the API directly, optionally in chunks:

.. code-block:: python

    for data_source in client.transformations.external_data_sources():
        ...  # do something with the data source

    for chunk in client.transformations.external_data_sources(chunk_size=25):
        ...  # do something with the chunk

Rotate credentials
------------------

Because a read model never carries the client secret, it cannot be turned back into a write model —
``as_write()`` raises a ``TypeError``. There is no update endpoint for this resource, so rotating a
secret (or changing any other setting) means deleting the existing data source and creating a
replacement with the same external ID:

.. code-block:: python

    client.transformations.external_data_sources.delete("fabric-lakehouse-prod")

    rotated = OneLakeExternalDataSourceWrite(
        external_id="fabric-lakehouse-prod",
        name="Production lakehouse",
        data_set_id=123456,
        settings=OneLakeSettingsWrite(
            credentials=OneLakeCredentialsWrite(
                client_id=os.environ["ONELAKE_CLIENT_ID"],
                tenant_id=os.environ["ONELAKE_TENANT_ID"],
                client_secret=os.environ["ONELAKE_NEW_CLIENT_SECRET"],
            ),
            location_description=OneLakeLocationDescription(
                workspace_id=os.environ["ONELAKE_WORKSPACE_ID"],
                container_id=os.environ["ONELAKE_CONTAINER_ID"],
            ),
        ),
    )
    client.transformations.external_data_sources.create(rotated)

Delete a data source
--------------------

Transformations that still reference a deleted data source fail the next time they run:

.. code-block:: python

    client.transformations.external_data_sources.delete("fabric-lakehouse-prod")

API reference
-------------

The methods are documented under
:ref:`transformations:Transformation External Data Sources`, and the data classes under
:ref:`transformations:Data classes`.
