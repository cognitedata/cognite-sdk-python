Data Organization
=================

Annotations
-----------

Retrieve an annotation by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.retrieve

Retrieve multiple annotations by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.retrieve_multiple

List annotation
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.list

Create an annotation
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.create

Suggest an annotation
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.suggest

Update annotations
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.update

Delete annotations
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.annotations.AnnotationsAPI.delete

Annotations Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.annotations
    :members:
    :show-inheritance:

Data sets
---------
Retrieve an data set by id
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.retrieve

Retrieve multiple data sets by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.retrieve_multiple

List data sets
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.list

Aggregate data sets
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.aggregate

Create data sets
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.create

Delete data sets
^^^^^^^^^^^^^^^^
This functionality is not yet available in the API.

Update data sets
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.data_sets.DataSetsAPI.update

Data Sets Data classes
^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.data_sets
    :members:
    :show-inheritance:

Labels
------

List labels
^^^^^^^^^^^
.. automethod:: cognite.client._api.labels.LabelsAPI.list

Retrieve labels
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.labels.LabelsAPI.retrieve

Create a label
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.labels.LabelsAPI.create

Delete labels
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.labels.LabelsAPI.delete

Labels Data classes
^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.labels
    :members:
    :show-inheritance:

Relationships
-------------
Retrieve a relationship by external id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.retrieve

Retrieve multiple relationships by external id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.retrieve_multiple

List relationships
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.list

Create a relationship
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.create

Update relationships
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.update

Upsert relationships
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.upsert

Delete relationships
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.relationships.RelationshipsAPI.delete

Relationship Data classes
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.relationships
    :members:
    :show-inheritance:
