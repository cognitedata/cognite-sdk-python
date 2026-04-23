Data Modeling
=============
.. note::
    The data modeling section of the SDK provides low level access to the data modeling API. Most users should use
    the higher level libraries described in the sections below.

Consuming data models
---------------------
If you are consuming data from data models, it is recommended to use `Cognite Pygen <https://cognite-pygen.readthedocs-hosted.com/en/latest/>`_
unless you need the full flexibility of the SDK. `pygen` provides a high-level, user-friendly interface for working
with data models, and it handles many of the complexities of the underlying API for you.
The same applies if you need to write or update instances in a data model in a dynamic way that cannot be handled by
:ref:`transformations`.

Building data models
--------------------
If you are building data models, it is recommended to use `Cognite Toolkit <https://docs.cognite.com/cdf/deploy/cdf_toolkit/>`_
to design and maintain your data models. `Cognite Toolkit` provides a high-level interface for defining and managing
data models, and it includes features such as versioning, testing, and deployment tools that can help you build robust
and maintainable data models.

.. currentmodule:: cognite.client

Data Models
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.data_models

Data model data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.data_models
    :members:
    :show-inheritance:

.. currentmodule:: cognite.client

Spaces
------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.spaces

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.spaces
    :members:
    :show-inheritance:

.. currentmodule:: cognite.client

Views
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.views

View data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.views
    :members:
    :show-inheritance:

.. currentmodule:: cognite.client

Containers
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.containers

Containers data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.containers
    :members:
    :show-inheritance:

.. currentmodule:: cognite.client

Instances
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.instances

.. _dm_instances_subscribe_example:

Example: Syncing instances to a local SQLite database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
        QueryResult,
        QuerySync,
        NodeResultSetExpressionSync,
        SelectSync,
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

        query = QuerySync(
            with_={
                "nodes": NodeResultSetExpressionSync(
                    filter=Equals(property=["node", "space"], value=space_to_sync)
                )
            },
            select={"nodes": SelectSync()},
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

.. currentmodule:: cognite.client

Statistics
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.statistics
   AsyncCogniteClient.data_modeling.statistics.spaces

Data modeling statistics data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.statistics
    :members:
    :show-inheritance:

.. currentmodule:: cognite.client

GraphQL
-------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.data_modeling.graphql

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

Debugging Data Classes
----------------------
.. automodule:: cognite.client.data_classes.data_modeling.debug
    :members:
    :show-inheritance:
