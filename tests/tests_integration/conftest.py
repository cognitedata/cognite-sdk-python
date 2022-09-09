import os
import random

import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.beta import CogniteClient as CogniteBetaClient
from cognite.client.credentials import ClientCredentials, OAuthClientCredentials, OAuthInteractive


def _get_credentials() -> ClientCredentials:
    login_flow = os.environ["LOGIN_FLOW"].lower()
    if login_flow == "client_credentials":
        return OAuthClientCredentials(
            token_url=os.environ["COGNITE_TOKEN_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            client_secret=os.environ["COGNITE_CLIENT_SECRET"],
            scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
        )
    elif login_flow == "interactive":
        return OAuthInteractive(
            authority_url=os.environ["COGNITE_AUTHORITY_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
            redirect_port=random.randint(53000, 60000),  # random port so we can run the test suite in parallel
        )

    else:
        raise ValueError("Environment variable LOGIN_FLOW must be set to either 'client_credentials' or 'interactive'")


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=_get_credentials(),
        )
    )


@pytest.fixture(scope="session")
def cognite_beta_client() -> CogniteBetaClient:
    return CogniteBetaClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=_get_credentials(),
        )
    )
