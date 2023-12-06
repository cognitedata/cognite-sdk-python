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
For this, you will need to supply ids for the time series that you want to retrieve. You can find
some ids by listing the available time series. Limits for listing resources default to 25, so
the following code will return the first 25 time series resources.

.. code:: python

    from cognite.client import CogniteClient

    c = CogniteClient()
    ts_list = c.time_series.list()

List available spaces in your Data Modeling project
---------------------------------------------------
In the following example, we list all spaces in the project.

.. code:: python

    from cognite.client import CogniteClient

    c = CogniteClient()
    spaces = c.data_modeling.spaces.list()