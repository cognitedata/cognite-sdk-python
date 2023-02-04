import contextlib
import json
import os
import sys
from base64 import urlsafe_b64decode
from pathlib import Path
from typing import Any, Dict, Optional

from msal import PublicClientApplication

from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider, OAuthInteractive


def patch_sdk_for_pyodide() -> None:
    global_config.disable_gzip = True


def running_in_browser() -> bool:
    return sys.platform == "emscripten" and "pyodide" in sys.modules


def _load_payload_from_jwt(token: str) -> Dict[str, Any]:
    """A JWT has the structure: `<header>.<payload>.<signature>`. This method returns payload as a dict"""
    _, payload, _ = token.split(".")
    if remainder := len(input_bytes := payload.encode()) % 4:
        input_bytes += b"=" * (4 - remainder)
    return json.loads(urlsafe_b64decode(input_bytes))


class OAuthInteractiveFusionNotebook(OAuthInteractive):
    def __init__(self, redirect_port: int = 53000) -> None:
        token = os.environ["COGNITE_TOKEN"]
        payload = _load_payload_from_jwt(token)
        client_id = payload["appid"]
        authority_url = f"https://login.microsoftonline.com/{payload['tid']}"

        # Note: Death to name mangling:
        self._OAuthCredentialProviderWithTokenRefresh__access_token = token
        self._OAuthCredentialProviderWithTokenRefresh__token_refresh_lock = contextlib.nullcontext()
        self._OAuthCredentialProviderWithTokenRefresh__access_token_expires_at = payload["exp"]

        self._OAuthInteractive__authority_url = authority_url
        self._OAuthInteractive__client_id = client_id
        self._OAuthInteractive__scopes = [f"{payload['aud']}/.default"]
        self._OAuthInteractive__redirect_port = redirect_port
        self._OAuthInteractive__app = PublicClientApplication(client_id=client_id, authority=authority_url)

    @staticmethod
    def _create_serializable_token_cache(cache_path: Path) -> None:
        return None


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name: str = "DSHubLite",
        credentials: CredentialProvider = None,
        api_subversion: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        file_transfer_timeout: Optional[int] = None,
        debug: bool = False,
    ) -> None:
        # Magic 🪄:
        project = os.environ["COGNITE_PROJECT"]
        credentials = credentials or OAuthInteractiveFusionNotebook()  # Even more magic 🧙
        base_url = _load_payload_from_jwt(os.environ["COGNITE_TOKEN"])["aud"]
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
