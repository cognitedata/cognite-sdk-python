Identity and access management
==============================
Compare access rights (capabilities)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Verify my capabilities
~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.IAMAPI.verify_capabilities

Compare capabilities
~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.IAMAPI.compare_capabilities

Tokens
^^^^^^
Inspect the token currently used by the client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.TokenAPI.inspect

Groups
^^^^^^
List groups
~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.list

Create groups
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.create

Delete groups
~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.GroupsAPI.delete


Security categories
^^^^^^^^^^^^^^^^^^^
List security categories
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.list

Create security categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.create

Delete security categories
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SecurityCategoriesAPI.delete


Sessions
^^^^^^^^^^^^^^^^^^^
List sessions
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SessionsAPI.list

Create a session
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SessionsAPI.create

Revoke a session
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.SessionsAPI.revoke


User Profiles
^^^^^^^^^^^^^^^^^^^
Get my own user profile
~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.UserProfilesAPI.me

List user profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.UserProfilesAPI.list

Retrieve one or more user profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.UserProfilesAPI.retrieve

Search for user profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: cognite.client._api.iam.UserProfilesAPI.search


Data classes
^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.iam
    :members:
    :show-inheritance:

.. automodule:: cognite.client.data_classes.user_profiles
    :members:
    :show-inheritance:
