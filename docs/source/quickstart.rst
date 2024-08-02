Quickstart
==========
Instantiate a new client from a configuration file
--------------------------------------------------
Use this code to instantiate a client in order to execute API calls to Cognite Data Fusion (CDF) using a configuration file.

.. note::
    How you read in the configuration file is up to you as the :ref:`CogniteClient <class_client_CogniteClient>` load method only
    accepts a dictionary or a YAML/JSON string. So for the purposes of this example, we will use the yaml library to read in a yaml file and
    substitute environment variables in the file string to ensure that sensitive information is not stored in the file.

See :ref:`CogniteClient <class_client_CogniteClient>`, :ref:`ClientConfig <class_client_ClientConfig>`,
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

.. code:: python

    import os
    from pathlib import Path
    from string import Template

    import yaml

    from cognite.client import CogniteClient, global_config

    file_path = Path("cognite-sdk-config.yaml")

    # Read in yaml file and substitute environment variables in the file string
    with file_path.open("r") as file_raw:
        env_sub_template = Template(file_raw.read())
    try:
        file_env_parsed = env_sub_template.substitute(dict(os.environ))
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error substituting environment variable: {e}")

    # Load yaml file string into a dictionary
    cognite_config = yaml.safe_load(file_env_parsed)

    # If you want to set a global configuration it must be done before creating the client
    global_config.apply_settings(cognite_config["global"])
    client = CogniteClient.load(cognite_config["client"])

Instantiate a new client
------------------------

Alternatively, you can create a client directly using the ClientConfig class.

Use this code to instantiate a client in order to execute API calls to Cognite Data Fusion (CDF).
The :code:`client_name` is a user-defined string intended to give the client a unique identifier. You
can provide the :code:`client_name` by passing it directly to the :ref:`ClientConfig <class_client_ClientConfig>` constructor.

The Cognite API uses OpenID Connect (OIDC) to authenticate.
Use one of the credential providers such as OAuthClientCredentials to authenticate:

.. note::
    The following example sets a global client configuration which will be used if no config is
    explicitly passed to :ref:`cognite_client:CogniteClient`.
    All examples in this documentation going forward assume that such a global configuration has been set.

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
    client = CogniteClient()

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

Discover time series
--------------------
For this, you will need to supply ids for the time series that you want to retrieve. You can find
some ids by listing the available time series. Limits for listing resources default to 25, so
the following code will return the first 25 time series resources.

.. code:: python

    from cognite.client import CogniteClient

    client = CogniteClient()
    ts_list = client.time_series.list()

List available spaces in your Data Modeling project
---------------------------------------------------
In the following example, we list all spaces in the project.

.. code:: python

    from cognite.client import CogniteClient

    client = CogniteClient()
    spaces = client.data_modeling.spaces.list()
