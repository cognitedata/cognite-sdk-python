"""
TODO: Remove

How do we want the default / example notebook to be for a brand new user?

>>> # Current thinking to allow passing e.g. client_name, headers or timeout
>>> from cognite.client import CogniteClient, FusionNotebookConfig
>>>
>>> client = CogniteClient(FusionNotebookConfig())
>>> client.assets.list(limit=10)
"""


import os
import sys
from typing import Dict, Optional, Tuple

from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider

global_config.disable_gzip = True


def running_in_browser():
    return sys.platform == "emscripten" and "pyodide" in sys.modules


class EnvVarToken(CredentialProvider):
    """Credential provider that always reads token from an environment variable just-in-time,
    allowing refreshing the value by another entity.

    Args:
        key (str | None): The name of the env.var. to read from. Default: 'COGNITE_TOKEN'

    Raises:
        KeyError: If the env.var. is not set.
    """

    def __init__(self, key: str = None) -> None:
        self.key = "COGNITE_TOKEN" if key is None else key

    def __token_factory(self) -> str:
        return os.environ[self.key]

    def authorization_header(self) -> Tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name: str,
        api_subversion: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
        debug: bool = False,
    ) -> None:
        # Magic 🪄:
        client_name = client_name or "DSHubLite"
        project = os.environ["COGNITE_PROJECT"]
        credentials = EnvVarToken()  # Even more magic 🧙
        base_url = os.environ["COGNITE_BASE_URL"]
        max_workers = 1
        super().__init__(
            client_name,
            project,
            credentials,
            api_subversion,
            base_url,
            max_workers,
            headers,
            timeout,
            file_transfer_timeout,
            debug,
        )
