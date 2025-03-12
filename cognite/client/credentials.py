from __future__ import annotations

import json
import logging
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from types import MappingProxyType
from typing import Any

from authlib.integrations.httpx_client import OAuth2Client

from cognite.client.exceptions import CogniteAuthError
from cognite.client.utils._auxiliary import load_resource_to_dict
from cognite.client.utils._importing import local_import

_TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT = 60  # We use the same default as authlib, just being explicit here

logger = logging.getLogger(__name__)


class CredentialProvider(ABC):
    @abstractmethod
    def authorization_header(self) -> tuple[str, str]:
        raise NotImplementedError

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> CredentialProvider:
        """Load a credential provider object from a YAML/JSON string or dict.

        Note:
            The dictionary must contain exactly one top level key, which is the type of the credential provider and must be one of the
            following strings: ``"token"``, ``"client_credentials"``, ``"interactive"``, ``"device_code"``, ``"client_certificate"``.
            The value of the key is a dictionary containing the configuration for the credential provider.

        Args:
            config (dict[str, Any] | str): A dictionary or YAML/JSON string containing the configuration for the credential provider.

        Returns:
            CredentialProvider: Initialized credential provider of the specified type.

        Examples:

            Get a token credential provider:

                >>> from cognite.client.credentials import CredentialProvider
                >>> config = {"token": "my secret token"}
                >>> credentials = CredentialProvider.load(config)

            Get a client credential provider:

                >>> import os
                >>> config = {
                ...     "client_credentials": {
                ...         "client_id": "abcd",
                ...         "client_secret": os.environ["OAUTH_CLIENT_SECRET"],
                ...         "token_url": "https://login.microsoftonline.com/xyz/oauth2/v2.0/token",
                ...         "scopes": ["https://api.cognitedata.com/.default"],
                ...     }
                ... }
                >>> credentials = CredentialProvider.load(config)
        """
        loaded = load_resource_to_dict(config)

        if len(loaded) != 1:
            raise ValueError(
                f"Credential provider configuration must be a dictionary containing exactly one of the following "
                f"supported types as the top level key: {sorted(_SUPPORTED_CREDENTIAL_TYPES)}."
            )

        credential_type, credential_config = next(iter(loaded.items()))

        if credential_type not in _SUPPORTED_CREDENTIAL_TYPES:
            raise ValueError(
                f"Invalid credential provider type given, the valid options are: {sorted(_SUPPORTED_CREDENTIAL_TYPES)}."
            )

        if credential_type == "token" and (isinstance(credential_config, str) or callable(credential_config)):
            credential_config = {"token": credential_config}
        return _SUPPORTED_CREDENTIAL_TYPES[credential_type].load(credential_config)  # type: ignore [attr-defined]


class Token(CredentialProvider):
    def __init__(self, token: str | Callable[[], str]) -> None:
        if isinstance(token, str):
            self.__token_factory = lambda: token
        elif callable(token):
            lock = threading.Lock()

            def safe_token() -> str:
                with lock:
                    return token()

            self.__token_factory = safe_token
        else:
            raise TypeError("token must be a str or callable")

    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class _OAuthCredentialProviderWithTokenRefresh(CredentialProvider):
    @staticmethod
    def _resolve_token_cache_path(token_cache_path: Path | None, client_id: str) -> Path:
        return token_cache_path or Path(tempfile.gettempdir()) / f"cognitetokencache.{client_id}.json"

    def __init__(
        self,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        token_cache_path: Path | None = None,
    ) -> None:
        self.token_expiry_leeway_seconds = token_expiry_leeway_seconds
        self._lock = threading.Lock()
        self.__access_token: str | None = None
        self.__expires_at: float | None = None
        self._token_cache_path: Path | None = token_cache_path
        if self._token_cache_path and self._token_cache_path.exists():
            try:
                with self._token_cache_path.open("r") as f:
                    data = json.load(f)
                self.__access_token = data.get("access_token")
                self.__expires_at = data.get("expires_at")
            except Exception:
                pass

    @abstractmethod
    def _refresh_access_token(self) -> tuple[str, float]:
        raise NotImplementedError

    def __should_refresh(self) -> bool:
        return (
            self.__access_token is None
            or self.__expires_at is None
            or self.__expires_at < time.time() + self.token_expiry_leeway_seconds
        )

    def authorization_header(self) -> tuple[str, str]:
        with self._lock:
            if self.__should_refresh():
                self.__access_token, self.__expires_at = self._refresh_access_token()
                self._maybe_cache_token()
        return "Authorization", f"Bearer {self.__access_token}"

    def _maybe_cache_token(self) -> None:
        if not self._token_cache_path:
            return
        try:
            self._token_cache_path.write_text(
                json.dumps({"access_token": self.__access_token, "expires_at": self.__expires_at})
            )
        except Exception as e:
            logger.warning(f"Failed to write token cache to {self._token_cache_path}: {e}")



class OAuthDeviceCode(_OAuthCredentialProviderWithTokenRefresh):
    def __init__(
        self,
        device_authorization_url: str,
        token_url: str,
        client_id: str,
        scopes: list[str],
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> None:
        cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        super().__init__(token_expiry_leeway_seconds, cache_path)
        self.device_authorization_url = device_authorization_url
        self.token_url = token_url
        self.client_id = client_id
        self.scopes = scopes
        self.token_custom_args = token_custom_args
        self.client = OAuth2Client(client_id=client_id, scope=" ".join(scopes))

    def _refresh_access_token(self) -> tuple[str, float]:
        data = {"client_id": self.client_id, "scope": " ".join(self.scopes)}
        data.update(self.token_custom_args)
        resp = self.client.post(self.device_authorization_url, data=data, headers={"Accept": "application/json"})
        device_flow = resp.json()
        if "verification_uri" in device_flow:
            print(f"Visit {device_flow['verification_uri']} and enter code: {device_flow.get('user_code')}")  # noqa: T201
        interval = device_flow.get("interval", 5)
        expires_at = time.time() + device_flow.get("expires_in", 900)
        while time.time() < expires_at:
            time.sleep(interval)
            try:
                token = self.client.fetch_token(
                    self.token_url,
                    grant_type="urn:ietf:params:oauth:grant-type:device_code",
                    device_code=device_flow["device_code"],
                )
                if "access_token" in token:
                    return token["access_token"], time.time() + float(token["expires_in"])
            except Exception:
                continue
        raise CogniteAuthError("Device code flow timed out")

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> OAuthDeviceCode:
        loaded = load_resource_to_dict(config)
        return cls(
            device_authorization_url=loaded.pop("device_authorization_url"),
            token_url=loaded.pop("token_url"),
            client_id=loaded.pop("client_id"),
            scopes=loaded.pop("scopes"),
            token_cache_path=Path(loaded.pop("token_cache_path")) if "token_cache_path" in loaded else None,
            token_expiry_leeway_seconds=int(
                loaded.pop("token_expiry_leeway_seconds", _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT)
            ),
            **loaded,
        )

    @classmethod
    def default_for_azure_ad(
        cls,
        tenant_id: str,
        client_id: str,
        cdf_cluster: str,
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **kwargs: Any,
    ) -> OAuthDeviceCode:
        return cls(
            device_authorization_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/devicecode",
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            scopes=[
                f"https://{cdf_cluster}.cognitedata.com/IDENTITY",
                f"https://{cdf_cluster}.cognitedata.com/user_impersonation",
                "openid",
                "profile",
            ],
            token_cache_path=token_cache_path,
            token_expiry_leeway_seconds=token_expiry_leeway_seconds,
            **kwargs,
        )


class OAuthInteractive(_OAuthCredentialProviderWithTokenRefresh):
    def __init__(
        self,
        authorize_url: str,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: list[str],
        redirect_uri: str,
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
    ) -> None:
        cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        super().__init__(token_expiry_leeway_seconds, cache_path)
        self.authorize_url = authorize_url
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.redirect_uri = redirect_uri
        self.client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            scope=" ".join(scopes),
            redirect_uri=redirect_uri,
        )

    def _refresh_access_token(self) -> tuple[str, float]:
        auth_url, state = self.client.create_authorization_url(self.authorize_url)
        print(f"Visit {auth_url}")
        redirect_response = input("Paste redirect URL: ")
        token = self.client.fetch_token(self.token_url, authorization_response=redirect_response)
        return token["access_token"], time.time() + float(token["expires_in"])

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> OAuthInteractive:
        loaded = load_resource_to_dict(config)
        return cls(
            authorize_url=loaded["authorize_url"],
            token_url=loaded["token_url"],
            client_id=loaded["client_id"],
            client_secret=loaded["client_secret"],
            scopes=loaded["scopes"],
            redirect_uri=loaded["redirect_uri"],
            token_cache_path=Path(loaded["token_cache_path"]) if "token_cache_path" in loaded else None,
            token_expiry_leeway_seconds=int(
                loaded.get("token_expiry_leeway_seconds", _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT)
            ),
        )

    @classmethod
    def default_for_azure_ad(
        cls,
        tenant_id: str,
        client_id: str,
        cdf_cluster: str,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> OAuthInteractive:
        """
        Create an OAuthClientCredentials instance for Azure with default token URL and scopes.

        The default configuration creates the URLs based on the tenant id and cluster:

        * Authority URL: "https://login.microsoftonline.com/{tenant_id}"
        * Scopes: [f"https://{cdf_cluster}.cognitedata.com/.default"]

        Args:
            tenant_id (str): The Azure tenant id
            client_id (str): Your application's client id.
            cdf_cluster (str): The CDF cluster where the CDF project is located.
            token_expiry_leeway_seconds (int): The token is refreshed at the earliest when this number of seconds is left before expiry. Default: 60 sec
            **token_custom_args (Any): Optional additional arguments to pass as query parameters to the token fetch request.

        Returns:
            OAuthInteractive: An OAuthInteractive instance
        """
        raise NotImplementedError
        # OLD IMPLEMENTATION:
        # return cls(
        #     authority_url=f"https://login.microsoftonline.com/{tenant_id}",
        #     client_id=client_id,
        #     scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        #     token_expiry_leeway_seconds=token_expiry_leeway_seconds,
        #     **token_custom_args,
        # )


class OAuthClientCredentials(_OAuthCredentialProviderWithTokenRefresh):
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: list[str],
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> None:
        cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        super().__init__(token_expiry_leeway_seconds, cache_path)
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.token_custom_args = token_custom_args
        self.client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            scope=" ".join(scopes),
        )

    def _refresh_access_token(self) -> tuple[str, float]:
        token = self.client.fetch_token(self.token_url, **self.token_custom_args)
        return token["access_token"], time.time() + float(token["expires_in"])

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> OAuthClientCredentials:
        loaded = load_resource_to_dict(config).copy()
        return cls(
            token_url=loaded.pop("token_url"),
            client_id=loaded.pop("client_id"),
            client_secret=loaded.pop("client_secret"),
            scopes=loaded.pop("scopes"),
            token_cache_path=Path(loaded.pop("token_cache_path")) if "token_cache_path" in loaded else None,
            token_expiry_leeway_seconds=int(
                loaded.pop("token_expiry_leeway_seconds", _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT)
            ),
            **loaded,
        )

    @classmethod
    def default_for_azure_ad(
        cls,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        cdf_cluster: str,
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
        **token_custom_args: Any,
    ) -> OAuthClientCredentials:
        return cls(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
            token_cache_path=token_cache_path,
            token_expiry_leeway_seconds=token_expiry_leeway_seconds,
            **token_custom_args,
        )


class OAuthClientCertificate(_OAuthCredentialProviderWithTokenRefresh):
    def __init__(
        self,
        token_url: str,
        client_id: str,
        cert_thumbprint: str,
        certificate: str,
        scopes: list[str],
        token_cache_path: Path | None = None,
        token_expiry_leeway_seconds: int = _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT,
    ) -> None:
        jwt = local_import("jwt")

        cache_path = self._resolve_token_cache_path(token_cache_path, client_id)
        super().__init__(token_expiry_leeway_seconds, cache_path)
        self.token_url = token_url
        self.client_id = client_id
        self.cert_thumbprint = cert_thumbprint
        self.certificate = certificate
        self.scopes = scopes

        now = int(time.time())
        payload = {
            "iss": client_id,
            "sub": client_id,
            "aud": token_url,
            "exp": now + 600,
            "iat": now,
        }
        try:
            self.client_assertion = jwt.encode(
                payload, self.certificate, algorithm="RS256", headers={"x5t": cert_thumbprint}
            )
        except Exception as e:
            raise CogniteAuthError("JWT creation failed") from e

        self.client = OAuth2Client(client_id=client_id, scope=" ".join(scopes))

    def _refresh_access_token(self) -> tuple[str, float]:
        token = self.client.fetch_token(
            self.token_url,
            client_assertion=self.client_assertion,
            client_assertion_type="urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        )
        return token["access_token"], time.time() + float(token["expires_in"])

    @classmethod
    def load(cls, config: dict[str, Any] | str) -> OAuthClientCertificate:
        loaded = load_resource_to_dict(config)
        return cls(
            token_url=loaded["token_url"],
            client_id=loaded["client_id"],
            cert_thumbprint=loaded["cert_thumbprint"],
            certificate=loaded["certificate"],
            scopes=loaded["scopes"],
            token_cache_path=Path(loaded["token_cache_path"]) if "token_cache_path" in loaded else None,
            token_expiry_leeway_seconds=int(
                loaded.get("token_expiry_leeway_seconds", _TOKEN_EXPIRY_LEEWAY_SECONDS_DEFAULT)
            ),
        )


_SUPPORTED_CREDENTIAL_TYPES = MappingProxyType(
    {
        "token": Token,
        "client_credentials": OAuthClientCredentials,
        "interactive": OAuthInteractive,
        "device_code": OAuthDeviceCode,
        "client_certificate": OAuthClientCertificate,
    }
)
