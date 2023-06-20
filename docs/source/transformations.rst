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

   transformations.SessionDetails
   Transformation
   TransformationUpdate
   TransformationList
   transformations.TagsFilter
   transformations.ContainsAny
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
   TransformationJobFilter
   transformations.schema.TransformationSchemaType
   transformations.schema.TransformationSchemaArrayType
   transformations.schema.TransformationSchemaMapType
   TransformationSchemaColumn
   TransformationSchemaColumnList
   TransformationDestination
   RawTable
   transformations.common.SequenceRows
   transformations.common.ViewInfo
   transformations.common.EdgeType
   transformations.common.DataModelInfo
   transformations.common.Nodes
   transformations.common.Edges
   transformations.common.Instances
   OidcCredentials
   transformations.common.NonceCredentials
   TransformationBlockedInfo
