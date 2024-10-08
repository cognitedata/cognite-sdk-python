Data Workflows
======================

Workflows
------------
Upsert Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.upsert

Delete Workflow(s)
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.delete

Retrieve Workflow
^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.retrieve

List Workflows
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.list


Workflow Versions
------------------
Upsert Workflow Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.upsert

Delete Workflow Version(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.delete

Retrieve Workflow Version
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.retrieve

List Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.list


Workflow Executions
--------------------
List Workflow Executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.list

Retrieve Detailed Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.retrieve_detailed

Run Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.run

Trigger Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.trigger

Cancel Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.cancel

Retry Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.retry

Workflow Tasks
------------------
Update Status of Async Task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTaskAPI.update

Workflow Triggers
-------------------
Create or update triggers for workflow executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.upsert

Create triggers for workflow executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.create

Delete triggers for workflow executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.delete

Get triggers for workflow executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.get_triggers

Get trigger run history for a workflow trigger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTriggerAPI.get_trigger_run_history

Data Workflows data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.workflows
    :members:
    :show-inheritance:
