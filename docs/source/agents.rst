======
Agents
======
Create or update an agent
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.agents.agents.AgentsAPI.upsert

Retrieve an agent by external id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.agents.agents.AgentsAPI.retrieve

List all agents
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.agents.agents.AgentsAPI.list

Delete an agent by external id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.agents.agents.AgentsAPI.delete

Chat with an agent by external id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.agents.agents.AgentsAPI.chat

.. warning::
   The server-side field ``agentId`` is deprecated. The SDK sends ``agentExternalId``
   to the API and will remove references to ``agentId`` in a future release.

Agent Data classes
^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.agents
    :members:
    :show-inheritance:
