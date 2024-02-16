Data Workflows
======================

.. warning::
    Data Workflows is a new feature:
      * The API specification is in beta.
      * The SDK implementation is in alpha.

    Thus, breaking changes may occur without further notice, see :ref:`appendix-alpha-beta-features` for more information.


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

Trigger Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.trigger

Cancel Workflow Execution
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowExecutionAPI.cancel

Workflow Tasks
------------------
Update Status of Async Task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.workflows.WorkflowTaskAPI.update


Data Workflows data classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.workflows
    :members:
    :show-inheritance:
