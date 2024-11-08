Data Modeling
=============
.. currentmodule:: cognite.client

Data Models
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.data_models

Data model data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.data_models

Spaces
------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.spaces

Spaces data classes
^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.spaces

Views
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.views

View data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.views

Containers
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.containers

Containers data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.containers

Instances
------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.instances

Example on syncing instances to local sqlite
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

    client = CogniteClient()


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

        return client.data_modeling.instances.subscribe(query, _sync_batch_to_sqlite)


    if __name__ == "__main__":
        SQLITE_DB_NAME = "test.db"
        SPACE_TO_SYNC = "mytestspace"
        bootstrap_sqlite(db_name=SQLITE_DB_NAME)
        sync_space_to_sqlite(db_name=SQLITE_DB_NAME, space_to_sync=SPACE_TO_SYNC)
        while True:
            # Keep main thread alive
            time.sleep(10)


Instances core data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.instances

Instances query data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.query

IDs data classes
^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.data_modeling.ids

GraphQL
-------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.data_modeling.graphql
