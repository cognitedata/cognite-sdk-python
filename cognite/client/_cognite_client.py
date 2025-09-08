from __future__ import annotations

import asyncio
import functools
from typing import Any

from requests import Response

from cognite.client._async_cognite_client import AsyncCogniteClient
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive
from cognite.client.utils._auxiliary import get_current_sdk_version, load_resource_to_dict


def _sync_wrapper(async_method):
    """Decorator to convert async methods to sync by running them in asyncio.run."""
    @functools.wraps(async_method)
    def wrapper(self, *args, **kwargs):
        # Check if we're already in an async context
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, which means we can't use asyncio.run
            # This shouldn't happen in normal usage, but just in case
            raise RuntimeError(
                "Cannot call sync methods from within an async context. "
                "Use the AsyncCogniteClient directly instead."
            )
        except RuntimeError:
            # No running loop, we can use asyncio.run
            pass
        
        return asyncio.run(async_method(self, *args, **kwargs))
    return wrapper


class _ResponseAdapter:
    """Adapter to convert httpx.Response to requests.Response interface."""
    
    def __init__(self, httpx_response):
        self._httpx_response = httpx_response
        self._json_cache = None
    
    @property
    def status_code(self):
        return self._httpx_response.status_code
    
    @property
    def headers(self):
        return dict(self._httpx_response.headers)
    
    @property
    def content(self):
        return self._httpx_response.content
    
    @property
    def text(self):
        return self._httpx_response.text
    
    def json(self, **kwargs):
        if self._json_cache is None:
            self._json_cache = self._httpx_response.json(**kwargs)
        return self._json_cache
    
    @property
    def request(self):
        # Create a minimal request object for compatibility
        class RequestAdapter:
            def __init__(self, httpx_request):
                self.method = httpx_request.method
                self.url = str(httpx_request.url)
                self.headers = dict(httpx_request.headers)
        
        return RequestAdapter(self._httpx_response.request)
    
    @property
    def history(self):
        # httpx doesn't have the same history concept as requests
        return []
    
    def __getattr__(self, name):
        # Fallback to httpx response for any other attributes
        return getattr(self._httpx_response, name)


class _SyncAPIWrapper:
    """Generic sync wrapper for async APIs."""
    
    def __init__(self, async_api):
        self._async_api = async_api
    
    def __call__(self, **kwargs):
        """Sync wrapper for async __call__ method."""
        return _sync_wrapper(self._async_api.__call__)(self, **kwargs)
    
    def __iter__(self):
        """Sync wrapper for async iterator."""
        async_iter = self._async_api.__aiter__()
        
        # Convert async iterator to sync iterator
        def sync_iter():
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                while True:
                    try:
                        item = loop.run_until_complete(async_iter.__anext__())
                        yield item
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()
        
        return sync_iter()
    
    def __getattr__(self, name):
        """Dynamically wrap any async method from the underlying API."""
        attr = getattr(self._async_api, name)
        if callable(attr) and hasattr(attr, '__call__'):
            # Check if it's an async method by looking for coroutine function
            import inspect
            if inspect.iscoroutinefunction(attr):
                return _sync_wrapper(attr)(self)
            else:
                # If it's not async, just return it as-is
                return attr
        else:
            # If it's not callable, return the attribute directly
            return attr


class _SyncAssetAPIWrapper(_SyncAPIWrapper):
    """Sync wrapper for AsyncAssetsAPI with asset-specific methods."""
    
    @_sync_wrapper
    async def retrieve_subtree(self, **kwargs):
        return await self._async_api.retrieve_subtree(**kwargs)
    
    @_sync_wrapper
    async def create_hierarchy(self, **kwargs):
        return await self._async_api.create_hierarchy(**kwargs)


class CogniteClient:
    """Main entrypoint into Cognite Python SDK.

    This is a sync wrapper around AsyncCogniteClient that maintains compatibility
    with the original synchronous interface.

    All services are made available through this object. See examples below.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    _API_VERSION = "v1"

    def __init__(self, config: ClientConfig | None = None) -> None:
        self._async_client = AsyncCogniteClient(config)
        
        # Sync API endpoints (wrap async versions) - ALL APIs
        self.annotations = _SyncAPIWrapper(self._async_client.annotations)
        self.assets = _SyncAssetAPIWrapper(self._async_client.assets)
        self.data_modeling = _SyncAPIWrapper(self._async_client.data_modeling)
        self.data_sets = _SyncAPIWrapper(self._async_client.data_sets)
        self.datapoints = _SyncAPIWrapper(self._async_client.datapoints)
        self.datapoints_subscriptions = _SyncAPIWrapper(self._async_client.datapoints_subscriptions)
        self.diagrams = _SyncAPIWrapper(self._async_client.diagrams)
        self.documents = _SyncAPIWrapper(self._async_client.documents)
        self.entity_matching = _SyncAPIWrapper(self._async_client.entity_matching)
        self.events = _SyncAPIWrapper(self._async_client.events)
        self.extraction_pipelines = _SyncAPIWrapper(self._async_client.extraction_pipelines)
        self.files = _SyncAPIWrapper(self._async_client.files)
        self.functions = _SyncAPIWrapper(self._async_client.functions)
        self.geospatial = _SyncAPIWrapper(self._async_client.geospatial)
        self.iam = _SyncAPIWrapper(self._async_client.iam)
        self.labels = _SyncAPIWrapper(self._async_client.labels)
        self.organization = _SyncAPIWrapper(self._async_client.organization)
        self.raw = _SyncAPIWrapper(self._async_client.raw)
        self.relationships = _SyncAPIWrapper(self._async_client.relationships)
        self.sequences = _SyncAPIWrapper(self._async_client.sequences)
        self.synthetic_time_series = _SyncAPIWrapper(self._async_client.synthetic_time_series)
        self.templates = _SyncAPIWrapper(self._async_client.templates)
        self.three_d = _SyncAPIWrapper(self._async_client.three_d)
        self.time_series = _SyncAPIWrapper(self._async_client.time_series)
        self.units = _SyncAPIWrapper(self._async_client.units)
        self.user_profiles = _SyncAPIWrapper(self._async_client.user_profiles)
        self.vision = _SyncAPIWrapper(self._async_client.vision)
        self.workflows = _SyncAPIWrapper(self._async_client.workflows)

    @_sync_wrapper
    async def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a GET request to an arbitrary path in the API."""
        httpx_response = await self._async_client.get(url, params=params, headers=headers)
        return _ResponseAdapter(httpx_response)

    @_sync_wrapper
    async def post(
        self,
        url: str,
        json: dict[str, Any],
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a POST request to an arbitrary path in the API."""
        httpx_response = await self._async_client.post(url, json=json, params=params, headers=headers)
        return _ResponseAdapter(httpx_response)

    @_sync_wrapper
    async def put(self, url: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a PUT request to an arbitrary path in the API."""
        httpx_response = await self._async_client.put(url, json=json, headers=headers)
        return _ResponseAdapter(httpx_response)

    @_sync_wrapper
    async def delete(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a DELETE request to an arbitrary path in the API."""
        httpx_response = await self._async_client.delete(url, params=params, headers=headers)
        return _ResponseAdapter(httpx_response)

    @property
    def version(self) -> str:
        """Returns the current SDK version.

        Returns:
            str: The current SDK version
        """
        return get_current_sdk_version()

    @property
    def config(self) -> ClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ClientConfig: The configuration object.
        """
        return self._async_client._config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            credentials (CredentialProvider): Credentials. e.g. Token, ClientCredentials.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """
        return cls(ClientConfig.default(project, cdf_cluster, credentials, client_name=client_name))

    @classmethod
    def default_oauth_client_credentials(
        cls,
        project: str,
        cdf_cluster: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration using a client credentials flow.

        The default configuration creates the URLs based on the project and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/
        * Token URL: "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            tenant_id (str): The Azure tenant ID.
            client_id (str): The Azure client ID.
            client_secret (str): The Azure client secret.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """

        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)

        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def default_oauth_interactive(
        cls,
        project: str,
        cdf_cluster: str,
        tenant_id: str,
        client_id: str,
        client_name: str | None = None,
    ) -> CogniteClient:
        """
        Create a CogniteClient with default configuration using the interactive flow.

        The default configuration creates the URLs based on the tenant_id and cluster:

        * Base URL: "https://{cdf_cluster}.cognitedata.com/
        * Authority URL: "https://login.microsoftonline.com/{tenant_id}"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            project (str): The CDF project.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            tenant_id (str): The Azure tenant ID.
            client_id (str): The Azure client ID.
            client_name (str | None): A user-defined name for the client. Used to identify the number of unique applications/scripts running on top of CDF. If this is not set, the getpass.getuser() is used instead, meaning the username you are logged in with is used.

        Returns:
            CogniteClient: A CogniteClient instance with default configurations.
        """
        credentials = OAuthInteractive.default_for_azure_ad(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> CogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the CogniteClient class.

        Returns:
            CogniteClient: A cognite client object.

        Examples:

            Create a cognite client object from a dictionary input:

                >>> from cognite.client import CogniteClient
                >>> import os
                >>> config = {
                ...     "client_name": "abcd",
                ...     "project": "cdf-project",
                ...     "base_url": "https://api.cognitedata.com/",
                ...     "credentials": {
                ...         "client_credentials": {
                ...             "client_id": "abcd",
                ...             "client_secret": os.environ["OAUTH_CLIENT_SECRET"],
                ...             "token_url": "https://login.microsoftonline.com/xyz/oauth2/v2.0/token",
                ...             "scopes": ["https://api.cognitedata.com/.default"],
                ...         },
                ...     },
                ... }
                >>> client = CogniteClient.load(config)
        """
        loaded = load_resource_to_dict(config)
        return cls(config=ClientConfig.load(loaded))

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        # Create and run cleanup coroutine
        async def cleanup():
            await self._async_client.__aexit__(exc_type, exc_val, exc_tb)
        
        try:
            asyncio.run(cleanup())
        except RuntimeError:
            # If we're already in an event loop, we can't run cleanup
            # This is a limitation but shouldn't happen in normal usage
            pass
