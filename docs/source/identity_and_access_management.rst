Identity and access management
==============================
Tokens
^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.iam.token.inspect

Groups
^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.iam.groups.list
   CogniteClient.iam.groups.create
   CogniteClient.iam.groups.delete


Security categories
^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.iam.security_categories.list
   CogniteClient.iam.security_categories.create
   CogniteClient.iam.security_categories.delete


Sessions
^^^^^^^^^^^^^^^^^^^
.. currentmodule:: cognite.client

.. autosummary:: 
   :toctree: generated/
   :template: custom-accessor-template.rst

   CogniteClient.iam.sessions.list
   CogniteClient.iam.sessions.create
   CogniteClient.iam.sessions.revoke


Data classes
^^^^^^^^^^^^
.. currentmodule:: cognite.client.data_classes

.. autosummary:: 
   :toctree: generated/
   :template: custom-class-template.rst

   Group
   GroupList
   SecurityCategory
   SecurityCategoryList
   ProjectSpec
   TokenInspection
   CreatedSession
   Session
   SessionList
   ClientCredentials
