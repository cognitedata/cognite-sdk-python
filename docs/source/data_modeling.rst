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

Sync instances
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.sync
.. automethod:: cognite.client._api.data_modeling.instances.InstancesAPI.subscribe

Example on syncing instances to local sqlite
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: python

    import json
    import time
    import sqlite3

    from cognite.client import CogniteClient
    from cognite.client.data_classes.data_modeling.instances import (
        SubscriptionContext,
    )
    from cognite.client.data_classes.data_modeling.query import (
        QueryResult,
        Query,
        NodeResultSetExpression,
        Select,
    )
    from cognite.client.data_classes.filters import Equals

    c = CogniteClient()


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


    def sync_space_to_sqlite(
        db_name: str, space_to_sync: str
    ) -> SubscriptionContext:
        with sqlite_connection(db_name) as connection:
            existing_cursor = connection.execute(
                "SELECT cursor FROM cursor"
            ).fetchone()
            if existing_cursor:
                print("Found existing cursor, using that")

        query = Query(
            with_={
                "nodes": NodeResultSetExpression(
                    filter=Equals(property=["node", "space"], value=space_to_sync)
                )
            },
            select={"nodes": Select()},
            cursors={"nodes": existing_cursor[0] if existing_cursor else None},
        )

        def _sync_batch_to_sqlite(result: QueryResult):
            with sqlite_connection(db_name) as connection:
                inserts = []
                deletes = []
                for node in result["nodes"]:
                    if node.deleted_time is None:
                        inserts.append(
                            (node.space, node.external_id, json.dumps(node.dump()))
                        )
                    else:
                        deletes.append((node.space, node.external_id))
                # Updates must be done in the same transaction as persisting the cursor.
                # A transaction is implicitly started by sqlite here.
                #
                # It is also important that deletes happen first as the same (space, external_id)
                # may appear as several tombstones and then a new instance, which must result in
                # the instance being saved.
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

        return c.data_modeling.instances.subscribe(query, _sync_batch_to_sqlite)


    if __name__ == "__main__":
        SQLITE_DB_NAME = "test.db"
        SPACE_TO_SYNC = "mytestspace"
        bootstrap_sqlite(db_name=SQLITE_DB_NAME)
        sync_space_to_sqlite(db_name=SQLITE_DB_NAME, space_to_sync=SPACE_TO_SYNC)
        while True:
            # Keep main thread alive
            time.sleep(10)


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

GraphQL
-------
Apply DML
^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.graphql.DataModelingGraphQLAPI.apply_dml

Execute GraphQl query
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_modeling.graphql.DataModelingGraphQLAPI.query
