Transformations
===============

TransformationsAPI
------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.transformations.create
   CogniteClient.transformations.retrieve
   CogniteClient.transformations.retrieve_multiple
   CogniteClient.transformations.run
   CogniteClient.transformations.run_async
   CogniteClient.transformations.preview
   CogniteClient.transformations.cancel
   CogniteClient.transformations.list
   CogniteClient.transformations.update
   CogniteClient.transformations.delete


Transformation Schedules
------------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.transformations.schedules.create
   CogniteClient.transformations.schedules.retrieve
   CogniteClient.transformations.schedules.retrieve_multiple
   CogniteClient.transformations.schedules.list
   CogniteClient.transformations.schedules.update
   CogniteClient.transformations.schedules.delete


Transformation Notifications
----------------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.transformations.notifications.create
   CogniteClient.transformations.notifications.list
   CogniteClient.transformations.notifications.delete


Transformation Jobs
-------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst
   
   CogniteClient.transformations.jobs.retrieve
   CogniteClient.transformations.jobs.retrieve_multiple
   CogniteClient.transformations.jobs.list
   CogniteClient.transformations.jobs.list_metrics

Transformation Schema
----------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst
   
   CogniteClient.transformations.schema.retrieve


Data classes
------------
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

   SessionDetails
   Transformation
   TransformationUpdate
   TransformationList
   TagsFilter
   ContainsAny
   transformations.TransformationFilter
   TransformationPreviewResult
   TransformationSchedule
   TransformationScheduleUpdate
   TransformationScheduleList
   TransformationNotification
   TransformationNotificationList
   transformations.notifications.TransformationNotificationFilter
   TransformationJobStatus
   TransformationJobMetric
   TransformationJobMetricList
   TransformationJob
   TransformationJobList
   transformations.jobs.TransformationJobFilter
   TransformationSchemaType
   TransformationSchemaArrayType
   TransformationSchemaMapType
   TransformationSchemaColumn
   TransformationSchemaColumnList
   TransformationDestination
   RawTable
   SequenceRows
   ViewInfo
   EdgeType
   DataModelInfo
   Nodes
   Edges
   Instances
   OidcCredentials
   NonceCredentials
   TransformationBlockedInfo
