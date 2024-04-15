Functions
=========
Functions API
-------------
Create function
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.create

Delete function
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.delete

List functions
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.list

Retrieve function
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.retrieve

Retrieve multiple functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.retrieve_multiple

Call function
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionsAPI.call


Function calls
--------------
List function calls
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionCallsAPI.list

Retrieve function call
^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionCallsAPI.retrieve

Retrieve function call response
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionCallsAPI.get_response

Retrieve function call logs
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionCallsAPI.get_logs

Function schedules
------------------
Retrieve function schedule
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionSchedulesAPI.retrieve

List function schedules
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionSchedulesAPI.list

Create function schedule
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionSchedulesAPI.create

Delete function schedule
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionSchedulesAPI.delete

Get function schedule input data
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.functions.FunctionSchedulesAPI.get_input_data

Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.functions
    :members:
    :show-inheritance:
