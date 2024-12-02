Data Workflows
======================

Workflows
------------
Upsert Workflows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.upsert

Retrieve Workflows
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.retrieve

List Workflows
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.list

Delete Workflows
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.delete


Workflow Versions
------------------
Upsert Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.upsert

Retrieve Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.retrieve

List Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.list

Delete Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.delete


Workflow Executions
--------------------
Run Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.run

Retrieve detailed Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.retrieve_detailed

List Workflow Executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.list

Cancel Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.cancel

Retry Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.retry


Workflow Tasks
------------------
Update status of async Task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTaskAPI.update


Workflow Triggers
-------------------
Upsert Trigger
^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.upsert

List Triggers
^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.list

List runs for a Trigger
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.list_runs

Delete Triggers
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.delete


Data Workflows data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.workflows
    :members:
    :show-inheritance:
