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
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.sync
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.subscribe

.. _dm_instances_subscribe_example:

Example: Syncing instances to a local SQLite database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example demonstrates how to use the `subscribe` method to create a live, local replica
of instances from a specific space in a SQLite database.

.. code:: python

    import asyncio
    import json
    import sqlite3
    from typing import Optional

    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig
    from cognite.client.data_classes.data_modeling import (
        Query,
        QueryResult,
        NodeResultSetExpression,
        Select,
        SubscriptionContext,
    )
    from cognite.client.data_classes.filters import Equals


    def sqlite_connection(db_name: str) -> sqlite3.Connection:
        return sqlite3.connect(db_name)


    def bootstrap_sqlite(db_name: str) -> None:
        """Sets up the initial database schema if it doesn't exist."""
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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cursor (
                    space TEXT PRIMARY KEY,
                    cursor TEXT
                )
                """
            )
            connection.commit()


    async def sync_space_to_sqlite(
        async_client: AsyncCogniteClient, db_name: str, space_to_sync: str
    ) -> SubscriptionContext:
        """
        Sets up and starts a subscription to sync a space to a local SQLite database.
        """
        # 1. Read the last known cursor from the database.
        #    This is a blocking call, so we run it in a thread.
        def _get_cursor() -> Optional[str]:
            with sqlite_connection(db_name) as connection:
                result = connection.execute(
                    "SELECT cursor FROM cursor WHERE space = ?", (space_to_sync,)
                ).fetchone()
                if result:
                    print(f"Found existing cursor for space {space_to_sync!r}")
                    return result[0]
                return None

        existing_cursor = await asyncio.to_thread(_get_cursor)

        query = Query(
            with_={
                "nodes": NodeResultSetExpression(
                    filter=Equals(property=["node", "space"], value=space_to_sync)
                )
            },
            select={"nodes": Select()},
            cursors={"nodes": existing_cursor},
        )

        # 2. Define the callback that will process each batch of results.
        #    The callback itself does not have to be async, but it is preferable.
        async def _sync_batch_to_sqlite(result: QueryResult) -> None:
            def _write_to_db() -> tuple[int, int]:
                # 3. Prepare all data in memory before opening the database
                #    connection to minimize lock time.
                inserts, deletes = [], []
                for node in result["nodes"]:
                    if node.deleted_time is None:
                        inserts.append(
                            (node.space, node.external_id, json.dumps(node.dump()))
                        )
                    else:
                        deletes.append((node.space, node.external_id))

                # 4. Perform all database operations within a single transaction
                #    to ensure data consistency.
                with sqlite_connection(db_name) as connection:
                    # Note: Deletions must be processed before insertions. This ensures
                    # that we don't lose an instance that has been deleted and re-created
                    # in the same sync batch.
                    connection.executemany(
                        "DELETE FROM instance WHERE space=? AND external_id=?", deletes
                    )
                    connection.executemany(
                        "INSERT INTO instance (space, external_id, data) VALUES (?, ?, ?) "
                        "ON CONFLICT(space, external_id) DO UPDATE SET data=excluded.data",
                        inserts,
                    )
                    # Finally, persist the cursor for this space.
                    connection.execute(
                        "INSERT INTO cursor (space, cursor) VALUES (?, ?) "
                        "ON CONFLICT(space) DO UPDATE SET cursor=excluded.cursor",
                        (space_to_sync, result.cursors["nodes"]),
                    )
                    connection.commit()

                return len(inserts), len(deletes)

            # 5. Run the blocking database write operation in a separate thread.
            inserts, deletes = await asyncio.to_thread(_write_to_db)
            print(f"Wrote {inserts} nodes and deleted {deletes} nodes for space {space_to_sync!r}")

        # 6. Start the subscription and return the SubscriptionContext.
        return await async_client.data_modeling.instances.subscribe(query, _sync_batch_to_sqlite)


    async def main():
        async_client = AsyncCogniteClient(ClientConfig(...))

        SQLITE_DB_NAME = "my_instances.db"
        SPACE_TO_SYNC = "my-awesome-space"

        bootstrap_sqlite(db_name=SQLITE_DB_NAME)

        print(f"Starting subscription for space: {SPACE_TO_SYNC!r}...")
        subscription = await sync_space_to_sqlite(
            async_client, db_name=SQLITE_DB_NAME, space_to_sync=SPACE_TO_SYNC
        )
        print("Subscription is live. Press Ctrl+C (or Cmd+C) to stop.")
        try:
            # Keep the application alive to allow the background subscription to run.
            while True:
                await asyncio.sleep(10)

        except asyncio.CancelledError:
            print("Main task cancelled.")
        finally:
            # Ensure we clean up and cancel the subscription task on exit.
            print("Stopping subscription...")
            subscription.cancel()
            # Give the task a moment to shut down gracefully
            await asyncio.sleep(1)
            print("Subscription stopped gracefully.")


    if __name__ == "__main__":
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass


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
