Filters
=======

The filter language provides a set of classes that can be used to construct complex
queries for filtering data. Each filter class represents a specific filtering criterion,
allowing users to tailor their queries to their specific needs. Below is an overview of the
available filters:

Filter
------
Base class for all filters

.. autoclass:: cognite.client.data_classes.filters.Filter
    :members:
    :member-order: bysource

Logical Operators
-----------------

And
^^^
The `And` filter performs a logical AND operation on multiple filters, requiring all specified
conditions to be true for an item to match the filter.

.. autoclass:: cognite.client.data_classes.filters.And
    :members:
    :member-order: bysource

Not
^^^
The `Not` filter performs a logical NOT operation on a sub-filter.

.. autoclass:: cognite.client.data_classes.filters.Not
    :members:
    :member-order: bysource

Or
^^^
The `Or` filter performs a logical OR operation on multiple filters, requiring at least one
specified condition to be true for an item to match the filter.

.. autoclass:: cognite.client.data_classes.filters.Or
    :members:
    :member-order: bysource

Standard Filters
----------------
Equals
^^^^^^
The `Equals` filter checks if a property is equal to a specified value.

.. autoclass:: cognite.client.data_classes.filters.Equals
    :members:
    :member-order: bysource

Exists
^^^^^^
The `Exists` filter checks if a property exists.

.. autoclass:: cognite.client.data_classes.filters.Exists
    :members:
    :member-order: bysource

ContainsAll
^^^^^^^^^^^
The `ContainsAll` filter checks if an item contains all specified values for a given property.

.. autoclass:: cognite.client.data_classes.filters.ContainsAll
    :members:
    :member-order: bysource

ContainsAny
^^^^^^^^^^^
The `ContainsAny` filter checks if an item contains any of the specified values for a given property.

.. autoclass:: cognite.client.data_classes.filters.ContainsAny
    :members:
    :member-order: bysource

In
^^
The `In` filter checks if a property's value is in a specified list.

.. autoclass:: cognite.client.data_classes.filters.In
    :members:
    :member-order: bysource

Overlaps
^^^^^^^^
The `Overlaps` filter checks if two ranges overlap.

.. autoclass:: cognite.client.data_classes.filters.Overlaps
    :members:
    :member-order: bysource

Prefix
^^^^^^
The `Prefix` filter checks if a string property starts with a specified prefix.

.. autoclass:: cognite.client.data_classes.filters.Prefix
    :members:
    :member-order: bysource

Range
^^^^^
The `Range` filter checks if a numeric property's value is within a specified range that can be
open-ended.

.. autoclass:: cognite.client.data_classes.filters.Range
    :members:
    :member-order: bysource

Search
^^^^^^
The `Search` filter performs a full-text search on a specified property.

.. autoclass:: cognite.client.data_classes.filters.Search
    :members:
    :member-order: bysource

InAssetSubtree
^^^^^^^^^^^^^^
The `InAssetSubtree` filter checks if an item belongs to a specified asset subtree.

.. autoclass:: cognite.client.data_classes.filters.InAssetSubtree
    :members:
    :member-order: bysource

Geo Filters
-----------
GeoJSON
^^^^^^^
The `GeoJSON` filter performs geometric queries using GeoJSON representations.

.. autoclass:: cognite.client.data_classes.filters.GeoJSON
    :members:
    :member-order: bysource

GeoJSONDisjoint
^^^^^^^^^^^^^^^
The `GeoJSONDisjoint` filter checks if two geometric shapes are disjoint.

.. autoclass:: cognite.client.data_classes.filters.GeoJSONDisjoint
    :members:
    :member-order: bysource

GeoJSONIntersects
^^^^^^^^^^^^^^^^^
The `GeoJSONIntersects` filter checks if two geometric shapes intersect.

.. autoclass:: cognite.client.data_classes.filters.GeoJSONIntersects
    :members:
    :member-order: bysource

GeoJSONWithin
^^^^^^^^^^^^^
The `GeoJSONWithin` filter checks if one geometric shape is within another.

.. autoclass:: cognite.client.data_classes.filters.GeoJSONWithin
    :members:
    :member-order: bysource

Data Modeling-Specific Filters
------------------------------
SpaceFilter
^^^^^^^^^^^^^^
The `SpaceFilter` filters instances from one or more specific space(s).

.. autoclass:: cognite.client.data_classes.filters.SpaceFilter
    :members:
    :member-order: bysource

HasData
^^^^^^^
The `HasData` filter checks if an instance has data for a given property.

.. autoclass:: cognite.client.data_classes.filters.HasData
    :members:
    :member-order: bysource

MatchAll
^^^^^^^^
The `MatchAll` filter matches all instances.

.. autoclass:: cognite.client.data_classes.filters.MatchAll
    :members:
    :member-order: bysource

Nested
^^^^^^
The `Nested` filter applies a filter to the node pointed to by a direct relation property.

.. autoclass:: cognite.client.data_classes.filters.Nested
    :members:
    :member-order: bysource
