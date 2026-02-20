"""
===============================================================================
8816a2865e5d5598efb02dc405b9124c
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import ClientCredentials, CreatedSession, Session, SessionList
from cognite.client.data_classes.iam import SessionStatus, SessionType
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSessionsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def create(
        self,
        client_credentials: ClientCredentials | None = None,
        session_type: SessionType | Literal["DEFAULT"] = "DEFAULT",
    ) -> CreatedSession:
        """
        `Create a session. <https://developer.cognite.com/api#tag/Sessions/operation/createSessions>`_

        Args:
            client_credentials: The client credentials to create the session. This is required if session_type is set to 'CLIENT_CREDENTIALS'.
            session_type: The type of session to create. Can be either 'CLIENT_CREDENTIALS', 'TOKEN_EXCHANGE', 'ONESHOT_TOKEN_EXCHANGE' or 'DEFAULT'. Defaults to 'DEFAULT' which will use -this- AsyncCogniteClient object to create the session. If this client was created using a token, 'TOKEN_EXCHANGE' will be used, and if this client was created using client credentials, 'CLIENT_CREDENTIALS' will be used.

        Session Types:

            * **client_credentials**: Credentials for a session using client credentials from an identity provider.
            * **token_exchange**: Credentials for a session using token exchange to reuse the user's credentials.
            * **one_shot_token_exchange**: Credentials for a session using one-shot token exchange to reuse the user's credentials. One-shot sessions are short-lived sessions that are not refreshed and do not require support for token exchange from the identity provider.

        Returns:
            The object with token inspection details.
        """
        return run_sync(
            self.__async_client.iam.sessions.create(client_credentials=client_credentials, session_type=session_type)
        )

    @overload
    def revoke(self, id: int) -> Session: ...

    @overload
    def revoke(self, id: Sequence[int]) -> SessionList: ...

    def revoke(self, id: int | Sequence[int]) -> Session | SessionList:
        """
        `Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://developer.cognite.com/api#tag/Sessions/operation/revokeSessions>`_

        Args:
            id: Id or list of session ids

        Returns:
            LIST capability, then only the session IDs will be present in the response.
        """
        return run_sync(self.__async_client.iam.sessions.revoke(id=id))

    @overload
    def retrieve(self, id: int) -> Session: ...

    @overload
    def retrieve(self, id: Sequence[int]) -> SessionList: ...

    def retrieve(self, id: int | Sequence[int]) -> Session | SessionList:
        """
        `Retrieves sessions with given IDs. <https://developer.cognite.com/api#tag/Sessions/operation/getSessionsByIds>`_

        The request will fail if any of the IDs does not belong to an existing session.

        Args:
            id: Id or list of session ids

        Returns:
            Session or list of sessions.
        """
        return run_sync(self.__async_client.iam.sessions.retrieve(id=id))

    def list(self, status: SessionStatus | None = None, limit: int = DEFAULT_LIMIT_READ) -> SessionList:
        """
        `List all sessions in the current project. <https://developer.cognite.com/api#tag/Sessions/operation/listSessions>`_

        Args:
            status: If given, only sessions with the given status are returned.
            limit: Max number of sessions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            a list of sessions in the current project.
        """
        return run_sync(self.__async_client.iam.sessions.list(status=status, limit=limit))
