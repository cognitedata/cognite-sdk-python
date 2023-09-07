Workflow Orchestration
======================

.. warning::
    Workflow Orchestration is experimental and may be subject to breaking changes in future versions without notice.


Workflows
------------
Create Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowAPI.create

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
Create Workflow Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.create

Delete Workflow Version(s)
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.delete

Retrieve Workflow Version
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.retrieve
m
List Workflow Versions
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowVersionAPI.list


Workflow Executions
------------------
List Workflow Executions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.list

Retrieve Detailed Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.retrieve_detailed

Trigger Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.trigger

Workflow Tasks
------------------
Update Status of Async Task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTaskAPI.update


Workflow Orchestration data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.workflows
    :members:
    :show-inheritance:
