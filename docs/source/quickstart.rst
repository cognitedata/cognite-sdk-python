Quickstart
==========

The SDK provides two client classes that you can use to easily interact with Cognite Data Fusion (CDF):

* **AsyncCogniteClient** - The primary async client using native ``async/await`` patterns
* **CogniteClient** - The synchronous client for backward compatibility

Both clients share the same configuration and credential providers. For the purpose of
this quickstart we'll demonstrate the most common/recommended patterns. More details and usage examples can be found in each respective
section: :ref:`AsyncCogniteClient <class_client_AsyncCogniteClient>`, :ref:`CogniteClient <class_client_CogniteClient>`, :ref:`ClientConfig <class_client_ClientConfig>`,
:ref:`GlobalConfig <class_client_GlobalConfig>`, and :ref:`credential_providers:Credential Providers`.

.. warning::
    Ensure that credentials are stored and handled securely by not hard-coding it or storing them in a text file. All the below examples
    are using and referencing environment variables to store this sensitive information.

Instantiate a new client from a configuration file
--------------------------------------------------
Use this code to instantiate a client using a configuration file in order to execute API calls to Cognite Data Fusion (CDF).

.. note::
    How you read in the configuration file is up to you as both ``AsyncCogniteClient.load`` and ``CogniteClient.load``
    accept both a dictionary and a YAML/JSON string. So for the purposes of this example, we will use the yaml library to read in a yaml file and
    substitute environment variables in the file string to ensure that sensitive information is not stored in the file.

See :ref:`AsyncCogniteClient <class_client_AsyncCogniteClient>`, :ref:`CogniteClient <class_client_CogniteClient>`, :ref:`ClientConfig <class_client_ClientConfig>`,
:ref:`GlobalConfig <class_client_GlobalConfig>`, and :ref:`credential_providers:Credential Providers`
for more information on the configuration options.

.. code:: yaml

    # cognite-sdk-config.yaml
    client:
      project: "my-project"
      client_name: "my-special-client"
      base_url: "https://${MY_CLUSTER}.cognitedata.com"
      credentials:
        client_credentials:
          token_url: "https://login.microsoftonline.com/${MY_TENANT_ID}/oauth2/v2.0/token"
          client_id: "${MY_CLIENT_ID}"
          client_secret: "${MY_CLIENT_SECRET}"
          scopes: ["https://api.cognitedata.com/.default"]
    global:
      max_retries: 10
      max_retry_backoff: 10

.. testsetup:: client_config_file

    >>> getfixture("set_envs")  # Fixture defined in conftest.py
    >>> getfixture("quickstart_client_config_file")  # Fixture defined in conftest.py

.. doctest:: client_config_file

    >>> import os
    >>> from pathlib import Path
    >>> from string import Template

    >>> import yaml

    >>> from cognite.client import CogniteClient, global_config

    >>> file_path = Path("cognite-sdk-config.yaml")

    >>> # Read in yaml file and substitute environment variables in the file string
    >>> env_sub_template = Template(file_path.read_text())
    >>> file_env_parsed = env_sub_template.substitute(dict(os.environ))

    >>> # Load yaml file string into a dictionary to parse global and client configurations
    >>> cognite_config = yaml.safe_load(file_env_parsed)

    >>> # If you want to set a global configuration it must be done before creating the client
    >>> global_config.apply_settings(cognite_config["global"])
    >>> client = CogniteClient.load(cognite_config["client"])

.. testcode:: client_config_file
    :hide:

    >>> global_config.max_retries
    10
    >>> global_config.max_retry_backoff
    10
    >>> client.config.project
    'my-project'
    >>> client.config.client_name
    'my-special-client'
    >>> client.config.credentials.client_id
    'my-client-id'
    >>> client.config.credentials.client_secret
    'my-client-secret'
    >>> client.config.credentials.token_url
    'https://login.microsoftonline.com/my-tenant-id/oauth2/v2.0/token'
    >>> client.config.credentials.scopes
    ['https://api.cognitedata.com/.default']

Instantiate a new client using ClientConfig
-------------------------------------------

Use this code to instantiate a client using the ClientConfig and global_config in order to execute API calls to Cognite Data Fusion (CDF).

Use this code to instantiate a client in order to execute API calls to Cognite Data Fusion (CDF).
The :code:`client_name` is a user-defined string intended to give the client a unique identifier. You
can provide the :code:`client_name` by passing it directly to the :ref:`ClientConfig <class_client_ClientConfig>` constructor.

The Cognite API uses OpenID Connect (OIDC) to authenticate.
Use one of the credential providers such as OAuthClientCredentials to authenticate:

.. note::
    The following example sets a global client configuration which will be used if no config is
    explicitly passed to ``AsyncCogniteClient`` or ``CogniteClient``.
    All examples in this documentation going forward assume that such a global configuration has been set.

.. testsetup:: client_config

    >>> getfixture("set_envs")  # Fixture defined in conftest.py

.. doctest:: client_config

    >>> from cognite.client import CogniteClient, ClientConfig, global_config
    >>> from cognite.client.credentials import OAuthClientCredentials

    >>> # This value will depend on the cluster your CDF project runs on
    >>> cluster = "api"
    >>> base_url = f"https://{cluster}.cognitedata.com"
    >>> tenant_id = "my-tenant-id"
    >>> client_id = "my-client-id"
    >>> # client secret should not be stored in-code, so we load it from an environment variable
    >>> client_secret = os.environ["MY_CLIENT_SECRET"]
    >>> creds = OAuthClientCredentials(
    ...   token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
    ...   client_id=client_id,
    ...   client_secret=client_secret,
    ...   scopes=[f"{base_url}/.default"]
    ... )

    >>> cnf = ClientConfig(
    ...   client_name="my-special-client",
    ...   base_url=base_url,
    ...   project="my-project",
    ...   credentials=creds
    ... )

    >>> global_config.default_client_config = cnf
    >>> client = CogniteClient()

.. testcode:: client_config
    :hide:

    >>> client.config.project
    'my-project'
    >>> client.config.client_name
    'my-special-client'
    >>> client.config.credentials.client_id
    'my-client-id'
    >>> client.config.credentials.client_secret
    'my-client-secret'
    >>> client.config.credentials.token_url
    'https://login.microsoftonline.com/my-tenant-id/oauth2/v2.0/token'
    >>> client.config.credentials.scopes
    ['https://api.cognitedata.com/.default']


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
    client = CogniteClient(cnf)

Instantiate an async client
---------------------------
The ``AsyncCogniteClient`` provides native ``async/await`` support for all API operations.
This is recommended for web applications, concurrent operations, and Pyodide/browser environments.

.. code:: python

    import asyncio
    from cognite.client import AsyncCogniteClient, ClientConfig
    from cognite.client.credentials import OAuthClientCredentials

    # Configuration is the same as for the sync client
    creds = OAuthClientCredentials(
        token_url="https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token",
        client_id="my-client-id",
        client_secret=os.environ["MY_CLIENT_SECRET"],
        scopes=["https://api.cognitedata.com/.default"]
    )
    cnf = ClientConfig(
        client_name="my-async-client",
        base_url="https://api.cognitedata.com",
        project="my-project",
        credentials=creds
    )

    async def main():
        client = AsyncCogniteClient(cnf)

        # All API methods are now awaitable
        assets = await client.assets.list(limit=10)

        # Run concurrent operations with asyncio.gather
        ts_task = client.time_series.list(limit=10)
        events_task = client.events.list(limit=10)
        time_series, events = await asyncio.gather(ts_task, events_task)

    asyncio.run(main())

List available spaces in your Data Modeling project
---------------------------------------------------
In the following example, we list all spaces in the project.

**Async:**

.. code:: python

    from cognite.client import AsyncCogniteClient

    async def list_spaces():
        client = AsyncCogniteClient()
        spaces = await client.data_modeling.spaces.list(limit=None)

**Sync:**

.. code:: python

    from cognite.client import CogniteClient

    client = CogniteClient()
    spaces = client.data_modeling.spaces.list(limit=None)
