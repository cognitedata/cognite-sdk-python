Sequences
=========

Metadata
--------
Retrieve a sequence by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.retrieve

Retrieve multiple sequences by id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.retrieve_multiple

List sequences
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.list

Aggregate sequences
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate

Aggregate Sequences Count
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_count

Aggregate Sequences Value Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_cardinality_values

Aggregate Sequences Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_cardinality_properties

Aggregate Sequences Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_unique_values

Aggregate Sequences Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.aggregate_unique_properties

Search for sequences
^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.search

Create a sequence
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.create

Delete sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.delete

Filter sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.filter


Update sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.update

Upsert sequences
^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesAPI.upsert


Rows
----
Retrieve data
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.retrieve

Insert rows into a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.insert

Insert a pandas dataframe into a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.insert_dataframe

Delete rows from a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.delete

Delete a range of rows from a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.sequences.SequencesDataAPI.delete_range

Sequence Data classes
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.sequences
    :members:
    :show-inheritance:
    :exclude-members: Sequence

    .. autoclass:: Sequence
        :noindex: