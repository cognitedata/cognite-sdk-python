from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ClientCredentials, CreatedSession, Session, SessionList
from cognite.client.data_classes.iam import SessionStatus, SessionType
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SessionsAPI(APIClient):
    _RESOURCE_PATH = "/sessions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_LIMIT = 100
        self._DELETE_LIMIT = (
            100  # There isn't an API limit so this is a self-inflicted limit due to no support for large payloads
        )

    def create(
        self,
        client_credentials: ClientCredentials | None = None,
        session_type: SessionType | Literal["DEFAULT"] = "DEFAULT",
    ) -> CreatedSession:
        """`Create a session. <https://developer.cognite.com/api#tag/Sessions/operation/createSessions>`_

        Args:
            client_credentials (ClientCredentials | None): The client credentials to create the session. This is required
                if session_type is set to 'CLIENT_CREDENTIALS'.
            session_type (SessionType | Literal['DEFAULT']): The type of session to create. Can be
                either 'CLIENT_CREDENTIALS', 'TOKEN_EXCHANGE', 'ONESHOT_TOKEN_EXCHANGE' or 'DEFAULT'.
                Defaults to 'DEFAULT' which will use -this- CogniteClient object to create the session.
                If this client was created using a token, 'TOKEN_EXCHANGE' will be used, and if
                this client was created using client credentials, 'CLIENT_CREDENTIALS' will be used.

        Session Types:

            * **client_credentials**: Credentials for a session using client credentials from an identity provider.
            * **token_exchange**: Credentials for a session using token exchange to reuse the user's credentials.
            * **one_shot_token_exchange**: Credentials for a session using one-shot token exchange to reuse the user's credentials. One-shot sessions are short-lived sessions that are not refreshed and do not require support for token exchange from the identity provider.

        Returns:
            CreatedSession: The object with token inspection details.
        """
        if client_credentials is None and isinstance(creds := self._config.credentials, OAuthClientCredentials):
            client_credentials = ClientCredentials(creds.client_id, creds.client_secret)

        session_type_up = session_type.upper()
        if session_type_up == "DEFAULT":  # For backwards compatibility after session_type was introduced
            items = {"tokenExchange": True} if client_credentials is None else client_credentials.dump(camel_case=True)

        elif session_type_up == "CLIENT_CREDENTIALS":
            if client_credentials is None:
                raise ValueError(
                    "For session_type='CLIENT_CREDENTIALS', either `client_credentials` must be provided OR "
                    "this client must be using OAuthClientCredentials"
                )
            items = client_credentials.dump(camel_case=True)

        elif session_type_up == "TOKEN_EXCHANGE":
            items = {"tokenExchange": True}

        elif session_type_up == "ONESHOT_TOKEN_EXCHANGE":
            items = {"oneshotTokenExchange": True}
        else:
            raise ValueError(f"Session type not understood: {session_type}")
        return CreatedSession.load(self._post(self._RESOURCE_PATH, {"items": [items]}).json()["items"][0])

    @overload
    def revoke(self, id: int) -> Session: ...

    @overload
    def revoke(self, id: Sequence[int]) -> SessionList: ...

    def revoke(self, id: int | Sequence[int]) -> Session | SessionList:
        """`Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://developer.cognite.com/api#tag/Sessions/operation/revokeSessions>`_

        Args:
            id (int | Sequence[int]): Id or list of session ids

        Returns:
            Session | SessionList: List of revoked sessions. If the user does not have the sessionsAcl:LIST capability, then only the session IDs will be present in the response.
        """

        ident_sequence = IdentifierSequence.load(ids=id, external_ids=None)

        revoked_sessions_res = cast(
            list,
            self._delete_multiple(
                identifiers=ident_sequence,
                wrap_ids=True,
                returns_items=True,
                delete_endpoint="/revoke",
            ),
        )

        revoked_sessions = SessionList._load(revoked_sessions_res)
        return revoked_sessions[0] if ident_sequence.is_singleton() else revoked_sessions

    @overload
    def retrieve(self, id: int) -> Session: ...

    @overload
    def retrieve(self, id: Sequence[int]) -> SessionList: ...

    def retrieve(self, id: int | Sequence[int]) -> Session | SessionList:
        """`Retrieves sessions with given IDs. <https://developer.cognite.com/api#tag/Sessions/operation/getSessionsByIds>`_

        The request will fail if any of the IDs does not belong to an existing session.

        Args:
            id (int | Sequence[int]): Id or list of session ids

        Returns:
            Session | SessionList: Session or list of sessions.
        """

        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        return self._retrieve_multiple(
            list_cls=SessionList,
            resource_cls=Session,
            identifiers=identifiers,
        )

    def list(self, status: SessionStatus | None = None, limit: int = DEFAULT_LIMIT_READ) -> SessionList:
        """`List all sessions in the current project. <https://developer.cognite.com/api#tag/Sessions/operation/listSessions>`_

        Args:
            status (SessionStatus | None): If given, only sessions with the given status are returned.
            limit (int): Max number of sessions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SessionList: a list of sessions in the current project.
        """
        filter = {"status": status.upper()} if status is not None else None
        return self._list(list_cls=SessionList, resource_cls=Session, method="GET", filter=filter, limit=limit)
