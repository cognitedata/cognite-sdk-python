import os
import warnings

import cognite.client as cc
from cognite.client._http_client import _RetryTracker
from cognite.client.config import ClientConfig
from cognite.client.credentials import CredentialProvider


def patch_sdk_for_pyodide():
    from pyodide_http import patch_all

    patch_all()
    os.environ["COGNITE_DISABLE_PYPI_VERSION_CHECK"] = "1"
    cc.config.global_config.disable_gzip = True
    cc._http_client.HTTPClient._old__init__ = cc._http_client.HTTPClient.__init__
    cc._http_client.HTTPClient.__init__ = http_client__init__
    cc.config.FusionNotebookConfig = FusionNotebookConfig
    cc.utils._concurrency.ConcurrencySettings.executor_type = "mainthread"
    cc.utils._concurrency.ConcurrencySettings.priority_executor_type = "mainthread"
    cc._api.assets._AssetPoster = NotImplementedAssetPoster
    warnings.filterwarnings(
        action="ignore", category=UserWarning, message="Your installation of 'protobuf' is missing compiled C binaries"
    )
    if os.getenv("COGNITE_FUSION_NOTEBOOK") is not None:
        cc.config.global_config.default_client_config = FusionNotebookConfig()


def http_client__init__(self, config, session, retry_tracker_factory=_RetryTracker):
    import pyodide_http

    self._old__init__(config, session, retry_tracker_factory)
    self.session.mount("https://", pyodide_http._requests.PyodideHTTPAdapter())
    self.session.mount("http://", pyodide_http._requests.PyodideHTTPAdapter())


class NotImplementedAssetPoster:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("AssetsAPI.create_hierarchy is not pyodide/web-browser compatible yet! Stay tuned!")


class EnvVarToken(CredentialProvider):
    def __init__(self, key="COGNITE_TOKEN"):
        self.key = key

    def __token_factory(self):
        return os.environ[self.key]

    def authorization_header(self):
        return ("Authorization", f"Bearer {self.__token_factory()}")


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name="DSHubLite",
        api_subversion=None,
        headers=None,
        timeout=None,
        file_transfer_timeout=None,
        debug=False,
    ):
        super().__init__(
            client_name=client_name,
            api_subversion=api_subversion,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            debug=debug,
            project=os.environ["COGNITE_PROJECT"],
            credentials=EnvVarToken(),
            base_url=os.environ["COGNITE_BASE_URL"],
            max_workers=1,
        )
