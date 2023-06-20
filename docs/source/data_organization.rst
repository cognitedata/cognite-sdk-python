Data Organization
=================

Annotations
-----------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.annotations.retrieve
   CogniteClient.annotations.retrieve_multiple
   CogniteClient.annotations.list
   CogniteClient.annotations.create
   CogniteClient.annotations.suggest
   CogniteClient.annotations.update
   CogniteClient.annotations.delete


Annotations Data classes
^^^^^^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

   Annotation
   AnnotationFilter
   AnnotationUpdate
   AnnotationList


Data sets
---------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.data_sets.retrieve
   CogniteClient.data_sets.retrieve_multiple
   CogniteClient.data_sets.list
   CogniteClient.data_sets.aggregate
   CogniteClient.data_sets.create
   CogniteClient.data_sets.update

Delete data sets
^^^^^^^^^^^^^^^^
This functionality is not yet available in the API.


Data Sets Data classes
^^^^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

    DataSet
    DataSetFilter
    DataSetUpdate
    DataSetAggregate
    DataSetList

Labels
------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.labels.list
   CogniteClient.labels.create
   CogniteClient.labels.delete


Labels Data classes
^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst
   
   LabelDefinition
   LabelDefinitionFilter
   LabelDefinitionList
   Label
   LabelFilter

Relationships
-------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.relationships.retrieve
   CogniteClient.relationships.retrieve_multiple
   CogniteClient.relationships.list
   CogniteClient.relationships.create
   CogniteClient.relationships.update
   CogniteClient.relationships.delete


Relationship Data classes
^^^^^^^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst
   
   Relationship
   relationships.RelationshipFilter
   RelationshipUpdate
   RelationshipList
