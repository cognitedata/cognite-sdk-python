Aggregates
==========

Aggregates summarise the items matching a query instead of returning them. Each class here
represents one aggregate and serialises to the request body the API expects.

The classes are shared between the data modeling aggregate endpoints and are therefore not named
after any single one of them. Properties are referenced by their full container path, e.g.
``Average(["mySpace", "myContainer", "temp"])``. Import the module by its own path:

.. code-block:: python

    from cognite.client.data_classes.data_modeling import aggregates as aggs

    client.data_modeling.records.aggregate(
        stream_id="my-stream",
        aggregates={
            "per_day": aggs.TimeHistogram(
                ["mySpace", "Game", "startTime"],
                calendar_interval="1d",
                aggregates={"games": aggs.Count()},
            ),
        },
    )

.. note::

    These are not the same classes as the identically named ones in
    ``cognite.client.data_classes.aggregations``, which belong to the deprecated
    ``instances.aggregate`` endpoint and reference view properties by identifier instead.

Aggregate data classes
^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_modeling.aggregates
    :members:
    :show-inheritance:
