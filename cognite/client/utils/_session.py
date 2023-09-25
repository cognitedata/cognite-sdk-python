from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client.credentials import OAuthClientCertificate
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.exceptions import CogniteAuthError

if TYPE_CHECKING:
    from cognite.client import CogniteClient


def create_session_and_return_nonce(
    client: CogniteClient,
    client_credentials: dict | ClientCredentials | None = None,
) -> str:
    if client_credentials is None:
        if isinstance(client._config.credentials, OAuthClientCertificate):
            raise CogniteAuthError("Client certificate credentials is not supported with the Functions API")
    elif isinstance(client_credentials, dict):
        client_credentials = ClientCredentials(client_credentials["client_id"], client_credentials["client_secret"])
    return client.iam.sessions.create(client_credentials).nonce
