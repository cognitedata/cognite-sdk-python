Data Modeling
=============

Data Models
------------
Retrieve data models by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.data_models.DataModelsAPI.retrieve

List data models
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.data_models.DataModelsAPI.list

Apply data models
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.data_models.DataModelsAPI.apply

Delete data models
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.data_models.DataModelsAPI.delete

Data model data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.data_models
    :members:
    :show-inheritance:

Spaces
------
Retrieve a space by id
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.spaces.SpacesAPI.retrieve

List spaces
^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.spaces.SpacesAPI.list

Apply spaces
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.spaces.SpacesAPI.apply

Delete spaces
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.spaces.SpacesAPI.delete

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.spaces
    :members:
    :show-inheritance:

Views
------------
Retrieve views by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.views.ViewsAPI.retrieve

List views
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.views.ViewsAPI.list

Apply view
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.views.ViewsAPI.apply

Delete views
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.views.ViewsAPI.delete

View data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.views
    :members:
    :show-inheritance:

Containers
------------
Retrieve containers by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.retrieve

List containers
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.list

Apply containers
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.apply

Delete containers
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.delete

Delete constraints
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.delete_constraints

Delete indexes
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.containers.ContainersAPI.delete_indexes

Containers data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.containers
    :members:
    :show-inheritance:

Instances
------------
Retrieve instances by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.retrieve

Retrieve Nodes by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.retrieve_nodes

Retrieve Edges by id(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.retrieve_edges

List instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.list

Apply instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.apply

Search instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.search

Aggregate instances
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.aggregate

Query instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.query

Inspect instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.inspect

Sync instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.sync

Subscribe to instance changes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.subscribe

Examples of subscribing to instance changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Async API (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^
.. code:: python

    import asyncio
    import json
    import sqlite3

    from cognite.client import AsyncCogniteClient
    from cognite.client.data_classes.data_modeling.instances import (
        AsyncSubscriptionContext,
    )
    from cognite.client.data_classes.data_modeling.query import (
        QueryResult,
        QuerySync,
        NodeResultSetExpressionSync,
        SelectSync,
    )
    from cognite.client.data_classes.filters import Equals

    client = AsyncCogniteClient()

    def sqlite_connection(db_name: str) -> sqlite3.Connection:
        return sqlite3.connect(db_name, check_same_thread=False)

    def bootstrap_sqlite(db_name: str) -> None:
        with sqlite_connection(db_name) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS instance (
                    space TEXT,
                    external_id TEXT,
                    data JSON,
                    PRIMARY KEY(space, external_id)
                )
                """
            )
            connection.execute("CREATE TABLE IF NOT EXISTS cursor (cursor TEXT)")

    async def sync_space_to_sqlite(
        db_name: str, space_to_sync: str
    ) -> AsyncSubscriptionContext:
        with sqlite_connection(db_name) as connection:
            existing_cursor = connection.execute(
                "SELECT cursor FROM cursor"
            ).fetchone()
            if existing_cursor:
                print("Found existing cursor, using that")

        query = QuerySync(
            with_={
                "nodes": NodeResultSetExpressionSync(
                    filter=Equals(property=["node", "space"], value=space_to_sync)
                )
            },
            select={"nodes": SelectSync()},
            cursors={"nodes": existing_cursor[0] if existing_cursor else None},
        )

        async def _sync_batch_to_sqlite(result: QueryResult):
            with sqlite_connection(db_name) as connection:
                inserts = []
                deletes = []
                for node in result["nodes"]:
                    if node.deleted_time is None:
                        inserts.append((node.space, node.external_id, json.dumps(node.dump())))
                    else:
                        deletes.append((node.space, node.external_id))
                # Updates must be done in the same transaction as persisting the cursor.
                connection.executemany(
                    "DELETE FROM instance WHERE space=? AND external_id=?", deletes
                )
                connection.executemany(
                    "INSERT INTO instance VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET data=excluded.data",
                    inserts,
                )
                connection.execute(
                    "INSERT INTO cursor VALUES (?)", [result.cursors["nodes"]]
                )
                connection.commit()
            print(f"Wrote {len(inserts)} nodes and deleted {len(deletes)} nodes")

        return await client.data_modeling.instances.subscribe(query, _sync_batch_to_sqlite)

    # Example usage
    SQLITE_DB_NAME = "test.db"
    SPACE_TO_SYNC = "mytestspace"
    bootstrap_sqlite(db_name=SQLITE_DB_NAME)

    subscription_context = await sync_space_to_sqlite(
        db_name=SQLITE_DB_NAME, space_to_sync=SPACE_TO_SYNC
    )

    # Let it run for a while, then cancel
    await asyncio.sleep(60)
    subscription_context.cancel()

Delete instances
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.delete

Instances core data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.instances
    :members:
    :show-inheritance:

Instances query data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.query
    :members:
    :show-inheritance:

Data Modeling ID data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.ids
    :members:
    :show-inheritance:

Statistics
------------

Project
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.statistics.StatisticsAPI.project

Retrieve space statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.statistics.SpaceStatisticsAPI.retrieve

List space statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.statistics.SpaceStatisticsAPI.list

Data modeling statistics data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.statistics
    :members:
    :show-inheritance:

GraphQL
-------
Apply DML
^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.graphql.DataModelingGraphQLAPI.apply_dml

Execute GraphQl query
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.graphql.DataModelingGraphQLAPI.query

Core Data Model
---------------
.. automodule:: cognite.client.data_classes.data_modeling.cdm.v1
    :members:
    :show-inheritance:

Extractor Extensions
--------------------
.. automodule:: cognite.client.data_classes.data_modeling.extractor_extensions.v1
    :members:
    :show-inheritance:
