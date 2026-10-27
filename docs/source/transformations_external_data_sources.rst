External Data Sources
=====================

.. note::
    This API is in public beta. The contract may change before general availability.

An external data source is a reusable, named connection that lets a transformation read from storage
outside Cognite Data Fusion (CDF). You configure the credentials and the location once, then reference the
source from transformation SQL by its external ID. Microsoft Fabric OneLake and Snowflake are the supported
systems.

Using this API for OneLake requires the Fabric connector to be enabled for the project. Contact
`Cognite Support <https://support.cognite.com/>`_ to have it enabled.

.. warning::
    For OneLake, store the client secret securely, for example in an environment variable or a secret manager.
    The examples below read it from the environment rather than hard-coding it.

Concepts
--------

* **Format** — the external system the source connects to. ``one_lake`` and ``snowflake`` are the supported
  values, and each format has its own pair of data classes:
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSourceWrite` and
  :class:`~cognite.client.data_classes.transformations.externaldata.SnowflakeExternalDataSourceWrite` for
  writing, :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSource` and
  :class:`~cognite.client.data_classes.transformations.externaldata.SnowflakeExternalDataSource` for reading.
* **Credentials** — the Microsoft Entra ID application credentials, held by
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeCredentialsWrite`. For OneLake, you
  supply the client secret when you create the data source. It is stored encrypted and never returned when you
  read a data source, which is why the read model,
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeCredentials`, has no
  ``client_secret``. The API does not manage or rotate OneLake credentials after creation; the Entra ID
  application and its secret remain in your Azure tenant.
* **Location** — where in OneLake the source points, held by
  :class:`~cognite.client.data_classes.transformations.externaldata.OneLakeLocationDescription`. Both IDs
  are GUIDs from the Fabric portal: the workspace ID under *Workspace settings > Workspace ID* and the
  lakehouse ID under *Lakehouse settings > Item ID*.
* **Snowflake keys** — for Snowflake, you send no keys. The server generates a key pair and returns only
  the public key, which you register with the Snowflake user (``ALTER USER ... SET RSA_PUBLIC_KEY``).
  ``expiry_time`` (milliseconds since epoch) is required for Snowflake, must be within one year from now,
  and is not allowed for OneLake.
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
``retrieve_multiple()``                   ``READ``
``create()``                              ``WRITE``
``delete()``                              ``WRITE``
``rotate_keys()``                         ``WRITE``
``verify_usability()``                    ``USE``
========================================= ==========

Running a transformation that reads from a data source also requires ``USE``, in addition to the usual
transformation and destination capabilities.

Register a data source
----------------------

Creating a data source requires a unique external ID. If an external ID you submit already exists for
the project, the request is rejected with a 409 and no items are created — there is no in-place update.
To modify an existing data source, delete it and create a replacement (see `Change OneLake credentials`_):

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

Register a Snowflake data source
--------------------------------

.. code-block:: python

    from cognite.client.data_classes.transformations.externaldata import (
        SnowflakeCredentialsWrite,
        SnowflakeExternalDataSourceWrite,
        SnowflakeLocationDescription,
        SnowflakeSettingsWrite,
    )
    from cognite.client.utils._time import timestamp_to_ms

    snowflake_source = SnowflakeExternalDataSourceWrite(
        external_id="snowflake-analytics-prod",
        expiry_time=timestamp_to_ms("now") + 30 * 24 * 3600 * 1000,
        settings=SnowflakeSettingsWrite(
            credentials=SnowflakeCredentialsWrite(
                account_identifier="org-account", user_name="COGNITE_SVC", role_name="COGNITE_READER"
            ),
            location_description=SnowflakeLocationDescription(warehouse_name="COMPUTE_WH"),
        ),
    )
    created = client.transformations.external_data_sources.create(snowflake_source)
    public_key = created.settings.credentials.public_key  # register this with the Snowflake user

Verify that a data source is usable
-----------------------------------

Before you wire a data source into a transformation, check that CDF can reach the storage location with the
stored credentials. That way you find configuration problems here instead of in a failed transformation job:

.. code-block:: python

    usability = client.transformations.external_data_sources.verify_usability("fabric-lakehouse-prod")

    if not usability.is_usable:
        raise RuntimeError("The data source is missing, or its credentials do not grant access")

The same check works for Snowflake data sources, which are passed by external ID in the same way:

.. code-block:: python

    usability = client.transformations.external_data_sources.verify_usability("snowflake-analytics-prod")

    if not usability.is_usable:
        raise RuntimeError("The data source is missing, or its credentials do not grant access")

Read tables from transformation SQL
-----------------------------------

A registered data source is referenced from transformation SQL through a table-valued function that depends on
the format: ``ext_onelake()`` for OneLake and ``ext_snowflake()`` for Snowflake.

For OneLake, the ``ext_onelake()`` function takes the external ID of the data source and the table name, plus an
optional schema name:

.. code-block:: sql

    SELECT * FROM ext_onelake('fabric-lakehouse-prod', 'my_table')
    SELECT * FROM ext_onelake('fabric-lakehouse-prod', 'my_table', 'my_schema')

For Snowflake, ``ext_snowflake()`` takes the external ID of the data source, then the database name, schema name,
and table name, in that order:

.. code-block:: sql

    select * from ext_snowflake('snowflake-sales-prod', 'mydb', 'myschema', 'mytable')

Data read this way can be written to any transformation destination. This example reads from OneLake and writes
it into a data model view:

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

To read from a Snowflake data source instead, change only the ``query=`` argument. The rest of the
``TransformationWrite`` call stays the same:

.. code-block:: python

    query="""
        SELECT
            externalId AS externalId,
            description AS name
        FROM ext_snowflake('snowflake-sales-prod', 'mydb', 'myschema', 'mytable')
    """,

See :doc:`transformations` for the rest of the transformation API, including the other ways of supplying
credentials.

List data sources
-----------------

Listing returns read models: OneLake data sources come back as
:class:`~cognite.client.data_classes.transformations.externaldata.OneLakeExternalDataSource`, and Snowflake
data sources come back as
:class:`~cognite.client.data_classes.transformations.externaldata.SnowflakeExternalDataSource`. Check the class
to read the format-specific location fields:

.. code-block:: python

    from cognite.client.data_classes.transformations.externaldata import (
        OneLakeExternalDataSource,
        SnowflakeExternalDataSource,
    )

    for data_source in client.transformations.external_data_sources.list(limit=None):
        if isinstance(data_source, OneLakeExternalDataSource):
            print(data_source.external_id, "OneLake", data_source.settings.location_description.workspace_id)
        elif isinstance(data_source, SnowflakeExternalDataSource):
            print(data_source.external_id, "Snowflake", data_source.settings.location_description.warehouse_name)

Filter on format (``one_lake`` or ``snowflake``), and retrieve data sources by external ID. A data source you do not have access to counts as missing: ``retrieve_multiple()`` raises for it unless you pass ``ignore_unknown_ids=True``:

.. code-block:: python

    onelake_sources = client.transformations.external_data_sources.list(format="one_lake")
    snowflake_sources = client.transformations.external_data_sources.list(format="snowflake")
    data_sources = client.transformations.external_data_sources.retrieve_multiple(
        ["snowflake-analytics-prod", "fabric-lakehouse-prod"], ignore_unknown_ids=True
    )

To iterate without holding every data source in memory, call the API directly, optionally in chunks:

.. code-block:: python

    for data_source in client.transformations.external_data_sources():
        ...  # do something with the data source

    for chunk in client.transformations.external_data_sources(chunk_size=25):
        ...  # do something with the chunk

Rotate Snowflake keys
---------------------

Snowflake data sources have a dedicated endpoint that generates a new key pair on the server. This operation
is Snowflake only: the API returns a 400 if you pass the external ID of a OneLake data source (for OneLake, see
`Change OneLake credentials`_). Pass up to
10 data sources per call; the operation is atomic, so either all keys are rotated or none. Each
``expiry_time`` must be in the future and at most one year from now.

The rotation is complete only after you register the new public key with the Snowflake user:

1. Call ``rotate_keys()`` and read the returned ``public_key``.
2. Run ``ALTER USER ... SET RSA_PUBLIC_KEY`` on the Snowflake user with that key.

.. code-block:: python

    from cognite.client.data_classes.transformations.externaldata import ExternalDataSourceRotateKeys
    from cognite.client.utils._time import timestamp_to_ms

    rotated = client.transformations.external_data_sources.rotate_keys(
        ExternalDataSourceRotateKeys("snowflake-analytics-prod", timestamp_to_ms("now") + 30 * 24 * 3600 * 1000)
    )
    public_key = rotated.public_key

    # Final step, outside the SDK: ALTER USER COGNITE_SVC SET RSA_PUBLIC_KEY = '<public_key>'

Change OneLake credentials
--------------------------

OneLake credentials are managed by you, not by Cognite. The Entra ID application and its client secret live
in your Azure tenant, and the Cognite REST APIs do not generate or rotate them. There is no update or rotate
endpoint for OneLake data sources. Snowflake data sources do not use this flow; rotate their keys with
`Rotate Snowflake keys`_ instead.

To change the credentials, or any other setting, delete the data source and create a replacement with the
same external ID:

.. code-block:: python

    client.transformations.external_data_sources.delete("fabric-lakehouse-prod")

    replacement = OneLakeExternalDataSourceWrite(
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
    client.transformations.external_data_sources.create(replacement)

Because a read model never carries the client secret, it cannot be turned back into a write model:
``as_write()`` raises a ``TypeError``. Build the replacement from your own configuration, as shown above.

Transformations that reference the data source fail while it is deleted, so create the replacement promptly.
See `Delete a data source`_.

Delete a data source
--------------------

Transformations that still reference a deleted data source fail the next time they run. ``delete()`` takes
external IDs of either format:

.. code-block:: python

    client.transformations.external_data_sources.delete("fabric-lakehouse-prod")
    client.transformations.external_data_sources.delete("snowflake-analytics-prod")

A single call can delete a mixed list of OneLake and Snowflake data sources:

.. code-block:: python

    client.transformations.external_data_sources.delete(["fabric-lakehouse-prod", "snowflake-analytics-prod"])

API reference
-------------

The methods are documented under
:ref:`transformations:Transformation External Data Sources`, and the data classes under
:ref:`transformations:Data classes`.
