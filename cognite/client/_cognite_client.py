from __future__ import annotations

from typing import Any

import asyncio
import httpx
from requests import Response

from cognite.client._api.ai import AIAPI
from cognite.client._api.annotations import AnnotationsAPI
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.documents import DocumentsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.extractionpipelines import ExtractionPipelinesAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.functions import FunctionsAPI
from cognite.client._api.geospatial import GeospatialAPI
from cognite.client._api.hosted_extractors import HostedExtractorsAPI
from cognite.client._api.iam import IAMAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.postgres_gateway import PostgresGatewaysAPI
from cognite.client._api.raw import RawAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._api.simulators import SimulatorsAPI
from cognite.client._api.templates import TemplatesAPI
from cognite.client._api.three_d import ThreeDAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api.units import UnitAPI
from cognite.client._api.vision import VisionAPI
from cognite.client._api.workflows import WorkflowAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive
from cognite.client.utils._auxiliary import get_current_sdk_version, load_resource_to_dict


class AsyncCogniteClient:
    """Async entrypoint into Cognite Python SDK.

    All services are made available through this object. Use with async/await.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    _API_VERSION = "v1"

    def __init__(self, config: ClientConfig | None = None) -> None:
        if (client_config := config or global_config.default_client_config) is None:
            raise ValueError(
                "No ClientConfig has been provided, either pass it directly to CogniteClient "
                "or set global_config.default_client_config."
            )
        else:
            self._config = client_config

        # APIs using base_url / resource path:
        self.ai = AIAPI(self._config, self._API_VERSION, self)
        self.assets = AssetsAPI(self._config, self._API_VERSION, self)
        self.events = EventsAPI(self._config, self._API_VERSION, self)
        self.files = FilesAPI(self._config, self._API_VERSION, self)
        self.iam = IAMAPI(self._config, self._API_VERSION, self)
        self.data_sets = DataSetsAPI(self._config, self._API_VERSION, self)
        self.sequences = SequencesAPI(self._config, self._API_VERSION, self)
        self.time_series = TimeSeriesAPI(self._config, self._API_VERSION, self)
        self.geospatial = GeospatialAPI(self._config, self._API_VERSION, self)
        self.raw = RawAPI(self._config, self._API_VERSION, self)
        self.three_d = ThreeDAPI(self._config, self._API_VERSION, self)
        self.labels = LabelsAPI(self._config, self._API_VERSION, self)
        self.relationships = RelationshipsAPI(self._config, self._API_VERSION, self)
        self.entity_matching = EntityMatchingAPI(self._config, self._API_VERSION, self)
        self.templates = TemplatesAPI(self._config, self._API_VERSION, self)
        self.vision = VisionAPI(self._config, self._API_VERSION, self)
        self.extraction_pipelines = ExtractionPipelinesAPI(self._config, self._API_VERSION, self)
        self.hosted_extractors = HostedExtractorsAPI(self._config, self._API_VERSION, self)
        self.postgres_gateway = PostgresGatewaysAPI(self._config, self._API_VERSION, self)
        self.transformations = TransformationsAPI(self._config, self._API_VERSION, self)
        self.diagrams = DiagramsAPI(self._config, self._API_VERSION, self)
        self.annotations = AnnotationsAPI(self._config, self._API_VERSION, self)
        self.functions = FunctionsAPI(self._config, self._API_VERSION, self)
        self.data_modeling = DataModelingAPI(self._config, self._API_VERSION, self)
        self.documents = DocumentsAPI(self._config, self._API_VERSION, self)
        self.workflows = WorkflowAPI(self._config, self._API_VERSION, self)
        self.units = UnitAPI(self._config, self._API_VERSION, self)
        self.simulators = SimulatorsAPI(self._config, self._API_VERSION, self)
        # APIs just using base_url:
        self._api_client = APIClient(self._config, api_version=None, cognite_client=self)

    async def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> httpx.Response:
        """Perform a GET request to an arbitrary path in the API."""
        return await self._api_client._aget(url, params=params, headers=headers)

    async def post(
        self,
        url: str,
        json: dict[str, Any],
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Perform a POST request to an arbitrary path in the API."""
        return await self._api_client._apost(url, json=json, params=params, headers=headers)

    async def put(self, url: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> httpx.Response:
        """Perform a PUT request to an arbitrary path in the API."""
        return await self._api_client._aput(url, json=json, headers=headers)

    async def delete(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> httpx.Response:
        """Perform a DELETE request to an arbitrary path in the API."""
        return await self._api_client._adelete(url, params=params, headers=headers)

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
        return self._config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> AsyncCogniteClient:
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
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
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
    ) -> AsyncCogniteClient:
        """
        Create an AsyncCogniteClient with default configuration using a client credentials flow.

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
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
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
    ) -> AsyncCogniteClient:
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
            AsyncCogniteClient: An AsyncCogniteClient instance with default configurations.
        """
        credentials = OAuthInteractive.default_for_azure_ad(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> AsyncCogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing configuration values defined in the CogniteClient class.

        Returns:
            AsyncCogniteClient: An async cognite client object.

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

    async def __aenter__(self) -> AsyncCogniteClient:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - cleanup resources."""
        # Close async HTTP connections
        if hasattr(self._api_client, '_http_client') and hasattr(self._api_client._http_client, 'async_client'):
            await self._api_client._http_client.async_client.aclose()
        if hasattr(self._api_client, '_http_client_with_retry') and hasattr(self._api_client._http_client_with_retry, 'async_client'):
            await self._api_client._http_client_with_retry.async_client.aclose()


# SYNC WRAPPER CLASS - Backward compatibility layer
class CogniteClient:
    """Synchronous wrapper for AsyncCogniteClient - maintains backward compatibility.
    
    This is a thin wrapper that uses asyncio.run() to provide a sync interface
    over the async implementation underneath.
    """
    
    def __init__(self, config: ClientConfig | None = None) -> None:
        self._async_client = AsyncCogniteClient(config)
        # Create sync wrappers for all APIs
        self._create_sync_api_wrappers()

    def _create_sync_api_wrappers(self) -> None:
        """Create sync wrappers for all async APIs."""
        api_names = [
            'ai', 'annotations', 'assets', 'data_modeling', 'data_sets', 'diagrams', 
            'documents', 'entity_matching', 'events', 'extraction_pipelines', 'files',
            'functions', 'geospatial', 'hosted_extractors', 'iam', 'labels',
            'postgres_gateway', 'raw', 'relationships', 'sequences', 'simulators',
            'templates', 'three_d', 'time_series', 'transformations', 'units',
            'vision', 'workflows'
        ]
        
        for api_name in api_names:
            if hasattr(self._async_client, api_name):
                async_api = getattr(self._async_client, api_name)
                sync_api = _SyncAPIWrapper(async_api)
                setattr(self, api_name, sync_api)

    def _sync_wrapper(self, async_method):
        """Helper to wrap async methods."""
        def wrapper(*args, **kwargs):
            try:
                loop = asyncio.get_running_loop()
                raise RuntimeError(
                    "Cannot call sync methods from within an async context. "
                    "Use AsyncCogniteClient directly instead."
                )
            except RuntimeError:
                pass
            return asyncio.run(async_method(*args, **kwargs))
        return wrapper

    def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a GET request to an arbitrary path in the API."""
        async def _async_get():
            httpx_response = await self._async_client.get(url, params=params, headers=headers)
            return _ResponseAdapter(httpx_response)
        return self._sync_wrapper(_async_get)()

    def post(
        self,
        url: str,
        json: dict[str, Any],
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a POST request to an arbitrary path in the API."""
        async def _async_post():
            httpx_response = await self._async_client.post(url, json=json, params=params, headers=headers)
            return _ResponseAdapter(httpx_response)
        return self._sync_wrapper(_async_post)()

    def put(self, url: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a PUT request to an arbitrary path in the API."""
        async def _async_put():
            httpx_response = await self._async_client.put(url, json=json, headers=headers)
            return _ResponseAdapter(httpx_response)
        return self._sync_wrapper(_async_put)()

    def delete(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Response:
        """Perform a DELETE request to an arbitrary path in the API."""
        async def _async_delete():
            httpx_response = await self._async_client.delete(url, params=params, headers=headers)
            return _ResponseAdapter(httpx_response)
        return self._sync_wrapper(_async_delete)()

    @property
    def version(self) -> str:
        """Returns the current SDK version."""
        return self._async_client.version

    @property
    def config(self) -> ClientConfig:
        """Returns the configuration for the current client."""
        return self._async_client.config

    @classmethod
    def default(
        cls,
        project: str,
        cdf_cluster: str,
        credentials: CredentialProvider,
        client_name: str | None = None,
    ) -> CogniteClient:
        """Create a CogniteClient with default configuration."""
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
        """Create a CogniteClient with OAuth client credentials."""
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
        """Create a CogniteClient with OAuth interactive flow."""
        credentials = OAuthInteractive.default_for_azure_ad(tenant_id, client_id, cdf_cluster)
        return cls.default(project, cdf_cluster, credentials, client_name)

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> CogniteClient:
        """Load a cognite client object from a YAML/JSON string or dict."""
        loaded = load_resource_to_dict(config)
        return cls(config=ClientConfig.load(loaded))

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        async def _cleanup():
            await self._async_client.__aexit__(exc_type, exc_val, exc_tb)
        try:
            asyncio.run(_cleanup())
        except RuntimeError:
            pass  # Already in async context


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
        class RequestAdapter:
            def __init__(self, httpx_request):
                self.method = httpx_request.method
                self.url = str(httpx_request.url)
                self.headers = dict(httpx_request.headers)
        return RequestAdapter(self._httpx_response.request)
    
    @property
    def history(self):
        return []
    
    def __getattr__(self, name):
        return getattr(self._httpx_response, name)


class _SyncAPIWrapper:
    """Generic sync wrapper for async API classes."""
    
    def __init__(self, async_api):
        self._async_api = async_api
    
    def __getattr__(self, name):
        """Dynamically wrap any async method from the underlying API."""
        attr = getattr(self._async_api, name)
        
        if callable(attr):
            import inspect
            if inspect.iscoroutinefunction(attr):
                # Wrap async method with sync wrapper
                def sync_method(*args, **kwargs):
                    try:
                        asyncio.get_running_loop()
                        raise RuntimeError("Cannot call sync methods from async context")
                    except RuntimeError:
                        pass
                    return asyncio.run(attr(*args, **kwargs))
                return sync_method
            else:
                return attr
        else:
            return attr
    
    def __iter__(self):
        """Convert async iterator to sync iterator."""
        def sync_iter():
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async_iter = self._async_api.__aiter__()
                while True:
                    try:
                        item = loop.run_until_complete(async_iter.__anext__())
                        yield item
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()
        return sync_iter()

    def __call__(self, **kwargs):
        """Handle callable APIs."""
        def sync_call():
            return asyncio.run(self._async_api(**kwargs))
        try:
            asyncio.get_running_loop()
            raise RuntimeError("Cannot call sync methods from async context")
        except RuntimeError:
            pass
        return sync_call()
