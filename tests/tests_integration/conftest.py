import os

import pytest
from msal import PublicClientApplication

from cognite.client import CogniteClient

REDIRECT_PORT = 53000


def make_cognite_client_with_interactive_flow() -> CogniteClient:
    authority_url = os.environ["COGNITE_AUTHORITY_URL"]
    client_id = os.environ["COGNITE_CLIENT_ID"]
    scopes = os.environ.get("COGNITE_TOKEN_SCOPES", "").split(",")
    app = PublicClientApplication(client_id=client_id, authority=authority_url)
    creds = app.acquire_token_interactive(scopes=scopes, port=REDIRECT_PORT)
    return CogniteClient(token=creds["access_token"])


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    login_flow = os.environ["LOGIN_FLOW"].lower()
    if login_flow == "client_credentials":
        return CogniteClient()
    elif login_flow == "interactive":
        return make_cognite_client_with_interactive_flow()
    else:
        raise ValueError("Environment variable LOGIN_FLOW must be set to either 'client_credentials' or 'interactive'")
