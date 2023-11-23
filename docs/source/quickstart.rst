Quickstart
==========
Instantiate a new client
------------------------
Use this code to instantiate a client in order to execute API calls to Cognite Data Fusion (CDF).
The :code:`client_name` is a user-defined string intended to give the client a unique identifier. You
can provide the :code:`client_name` by passing it directly to the :ref:`ClientConfig <class_client_ClientConfig>` constructor.

The Cognite API uses OpenID Connect (OIDC) to authenticate.
Use one of the credential providers such as OAuthClientCredentials to authenticate:

.. note::
    The following example sets a global client configuration which will be used if no config is
    explicitly passed to :ref:`cognite_client:CogniteClient`.
    All examples in this documentation assume that such a global configuration has been set.

.. code:: python

    from cognite.client import CogniteClient, ClientConfig, global_config
    from cognite.client.credentials import OAuthClientCredentials

    # This value will depend on the cluster your CDF project runs on
    cluster = "api"
    base_url = f"https://{cluster}.cognitedata.com"
    tenant_id = "my-tenant-id"
    client_id = "my-client-id"
    # client secret should not be stored in-code, so we load it from an environment variable
    client_secret = os.environ["MY_CLIENT_SECRET"]
    creds = OAuthClientCredentials(
      token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
      client_id=client_id,
      client_secret=client_secret,
      scopes=[f"{base_url}/.default"]
    )

    cnf = ClientConfig(
      client_name="my-special-client",
      base_url=base_url,
      project="my-project",
      credentials=creds
    )

    global_config.default_client_config = cnf
    c = CogniteClient()

Examples for all OAuth credential providers can be found in the :ref:`credential_providers:Credential Providers` section.

You can also make your own credential provider:

.. code:: python

    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import Token

    def token_provider():
        ...

    cnf = ClientConfig(
      client_name="my-special-client",
      base_url="https://<cluster>.cognitedata.com",
      project="my-project",
      credentials=Token(token_provider)
    )
    c = CogniteClient(cnf)

Discover time series
--------------------
For the next examples, you will need to supply ids for the time series that you want to retrieve. You can find some ids by listing the available time series.
Limits for listing resources default to 25, so the following code will return the first 25 time series resources.

.. code:: python

    from cognite.client import CogniteClient

    c = CogniteClient()
    ts_list = c.time_series.list()

Create an asset hierarchy
-------------------------
CDF organizes digital information about the physical world. Assets are digital representations of physical objects or
groups of objects, and assets are organized into an asset hierarchy. For example, an asset can represent a water pump
which is part of a subsystem on an oil platform.

At the top of an asset hierarchy is a root asset (e.g., the oil platform). Each project can have multiple root assets.
Note that all assets must have a name (a non-empty string).

To create a root asset (an asset without a parent), omit the parent ID when you post the asset to the API.
To make an asset a child of an existing asset, you must specify a parent ID (or parent external ID):

.. code:: python

    from cognite.client import CogniteClient
    from cognite.client.data_classes import Asset

    c = CogniteClient()
    my_asset = Asset(name="my first child asset", parent_id=123)
    c.assets.create(my_asset)

To post an entire asset hierarchy, you can describe the relations within your asset hierarchy
using the ``external_id`` and ``parent_external_id`` attributes on the ``Asset`` object. You can post
an arbitrary number of assets, and the SDK will split the request into multiple requests. To make sure that the
assets are created in the correct order, you can use the ``create_hierarchy()`` function, which takes care of the
topological sorting for you, before splitting the request into smaller chunks. However, note that the ``create_hierarchy()``
function requires the ``external_id`` property to be set for all assets.

This example shows how to post a three levels deep asset hierarchy consisting of three assets.

.. code:: python

    from cognite.client import CogniteClient
    from cognite.client.data_classes import Asset

    c = CogniteClient()
    root = Asset(name="root", external_id="1")
    child = Asset(name="child", external_id="2", parent_external_id="1")
    descendant = Asset(name="descendant", external_id="3", parent_external_id="2")
    c.assets.create_hierarchy([root, child, descendant])

Wrap the ``create_hierarchy()`` call in a try-except to get information if creating the assets fails:

- Which assets were created. (The request yielded a 201.)
- Which assets may have been created. (The request yielded 5xx.)
- Which assets were not created. (The request yielded 4xx, or was a descendant of another asset which may or may not have been created.)

.. code:: python

    from cognite.client.exceptions import CogniteAPIError
    try:
        c.assets.create_hierarchy([root, child, descendant])
    except CogniteAPIError as err:
        created = err.successful
        maybe_created = err.unknown
        not_created = err.failed

Prior to creating the Assets, it might be useful to do some validation on the assets you have. To do this without
potentially sending API requests, import and use :class:`~cognite.client.data_classes.assets.AssetHierarchy`:

.. code:: python

    from cognite.client.data_classes import AssetHierarchy
    hierarchy = AssetHierarchy(assets)
    # Get a report written to the terminal listing any issues:
    hierarchy.validate_and_report()
    # If there are issues, you may inspect them directly:
    if not hierarchy.is_valid():
        hierarchy.orphans
        hierarchy.invalid
        hierarchy.unsure_parents
        hierarchy.duplicates
        hierarchy.cycles  # Requires no other basic issues

Note that validation will run automatically for you when calling ``create_hierarchy()``. You may choose to catch
``CogniteAssetHierarchyError`` and inspect any raised issues:

.. code:: python

    from cognite.client.exceptions import CogniteAssetHierarchyError
    try:
        c.assets.create_hierarchy(assets)
    except CogniteAssetHierarchyError as err:
        # You may inspect the following attributes:
        err.orphans
        err.invalid
        err.unsure_parents
        err.duplicates
        err.cycles  # Requires no other basic issues

Retrieve all events related to an asset subtree
-----------------------------------------------
Assets are used to connect related data together, even if the data comes from different sources; Time series of data
points, events and files are all connected to one or more assets. A pump asset can be connected to a time series
measuring pressure within the pump, as well as events recording maintenance operations, and a file with a 3D diagram
of the pump.

To retrieve all events related to a given subtree of assets, we first fetch the subtree under a given asset using the
:code:`.subtree()` method. This returns an :code:`AssetList` object, which has a :code:`.events()` method. This method will
return events related to any asset in the :code:`AssetList`.

.. code:: python

    from cognite.client import CogniteClient
    from cognite.client.data_classes import Asset

    c = CogniteClient()
    subtree_root_asset = "some-external-id"
    subtree = c.assets.retrieve(external_id=subtree_root_asset).subtree()
    related_events = subtree.events()

You can use the same pattern to retrieve all time series or files related to a set of assets.

.. code:: python

    related_files = subtree.files()
    related_time_series = subtree.time_series()
