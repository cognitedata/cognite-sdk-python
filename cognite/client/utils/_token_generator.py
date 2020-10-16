import datetime
import os

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from cognite.client.exceptions import CogniteAPIKeyError


class TokenGenerator:
    def __init__(self):
        if not self.environment_vars_set():
            raise CogniteAPIKeyError("Missing required environment vars for token creation")

        self._idp_client_id = os.getenv("COGNITE_IDP_CLIENT_ID")
        self._idp_client_secret = os.getenv("COGNITE_IDP_CLIENT_SECRET")
        self._idp_token_endpoint = os.getenv("COGNITE_IDP_TOKEN_ENDPOINT")

        cognite_base_url = os.getenv("COGNITE_BASE_URL", "https://api.cognitedata.com")
        self._idp_scope = [os.getenv("COGNITE_IDP_SCOPE", f"{cognite_base_url}/.default")]
        self._generate_access_token()

    def return_access_token(self):
        if self._expires_at < datetime.datetime.now().timestamp():
            self._generate_access_token()

        if self._access_token is None:
            raise CogniteAPIKeyError(
                "Could not generate access token from provided token related environment variables"
            )

        return self._access_token

    def _generate_access_token(self):
        client = BackendApplicationClient(client_id=self._idp_client_id)
        oauth = OAuth2Session(client=client)
        token_result = oauth.fetch_token(
            token_url=self._idp_token_endpoint,
            client_id=self._idp_client_id,
            client_secret=self._idp_client_secret,
            scope=self._idp_scope,
        )

        self._access_token = token_result["access_token"]
        self._expires_at = token_result["expires_at"]

    @staticmethod
    def environment_vars_set():
        local_idp_client_id = os.getenv("COGNITE_IDP_CLIENT_ID")
        local_idp_client_secret = os.getenv("COGNITE_IDP_CLIENT_SECRET")
        local_idp_token_endpoint = os.getenv("COGNITE_IDP_TOKEN_ENDPOINT")

        return (
            local_idp_client_id is not None
            and local_idp_client_secret is not None
            and local_idp_token_endpoint is not None
        )
