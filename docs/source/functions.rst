Functions
=========
Functions API
-------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.functions.create
   CogniteClient.functions.delete
   CogniteClient.functions.list
   CogniteClient.functions.retrieve
   CogniteClient.functions.retrieve_multiple
   CogniteClient.functions.call
   CogniteClient.functions.limits
   CogniteClient.functions.activate
   CogniteClient.functions.status

Function calls
--------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.functions.calls.list
   CogniteClient.functions.calls.retrieve
   CogniteClient.functions.calls.get_response
   CogniteClient.functions.calls.get_logs
   CogniteClient.functions.calls.list

Function schedules
------------------
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst


   CogniteClient.functions.schedules.list
   CogniteClient.functions.schedules.create
   CogniteClient.functions.schedules.delete
   CogniteClient.functions.schedules.retrieve
   CogniteClient.functions.schedules.get_input_data

Data classes
^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst
   
   Function
   functions.FunctionFilter
   functions.FunctionCallsFilter
   FunctionSchedule
   functions.FunctionSchedulesFilter
   FunctionSchedulesList
   FunctionList
   FunctionCall
   FunctionCallList
   FunctionCallLogEntry
   FunctionCallLog
   FunctionsLimits
   FunctionsStatus
