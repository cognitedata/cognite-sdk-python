import os
import random

import pytest
from msal import PublicClientApplication

from cognite.client import ClientConfig, CogniteClient


def make_cognite_client_with_interactive_flow() -> CogniteClient:
    authority_url = os.environ["COGNITE_AUTHORITY_URL"]
    client_id = os.environ["COGNITE_CLIENT_ID"]
    scopes = os.environ.get("COGNITE_TOKEN_SCOPES", "").split(",")
    app = PublicClientApplication(client_id=client_id, authority=authority_url)
    redirect_port = random.randint(53000, 60000)  # random port so we can run the test suite in parallel
    creds = app.acquire_token_interactive(scopes=scopes, port=redirect_port)
    cnf = ClientConfig(
        client_name=os.environ["COGNITE_CLIENT_NAME"],
        project=os.environ["COGNITE_PROJECT"],
        base_url=os.environ["COGNITE_BASE_URL"],
        token=creds["access_token"],
    )
    return CogniteClient(cnf)


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    login_flow = os.environ["LOGIN_FLOW"].lower()
    if login_flow == "client_credentials":
        cnf = ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            token_url=os.environ["COGNITE_TOKEN_URL"],
            token_scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
            token_client_id=os.environ["COGNITE_CLIENT_ID"],
            token_client_secret=os.environ["COGNITE_CLIENT_SECRET"],
        )
        return CogniteClient(cnf)
    elif login_flow == "interactive":
        return make_cognite_client_with_interactive_flow()
    else:
        raise ValueError("Environment variable LOGIN_FLOW must be set to either 'client_credentials' or 'interactive'")
