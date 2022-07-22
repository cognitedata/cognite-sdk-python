import atexit
import os
import pathlib
import random

import pytest
from msal import PublicClientApplication, SerializableTokenCache

from cognite.client import CogniteClient

CACHE_FILENAME = "cache.bin"
CACHE_PATH = pathlib.Path(__file__).resolve().parent.parent.parent / "cache.bin"


def create_cache():
    cache = SerializableTokenCache()
    if CACHE_PATH.exists():
        cache.deserialize(open(CACHE_PATH, "r").read())
    atexit.register(lambda: open(CACHE_PATH, "w").write(cache.serialize()) if cache.has_state_changed else None)
    return cache


def make_cognite_client_with_interactive_flow() -> CogniteClient:
    authority_url = os.environ["COGNITE_AUTHORITY_URL"]
    client_id = os.environ["COGNITE_CLIENT_ID"]
    scopes = os.environ.get("COGNITE_TOKEN_SCOPES", "").split(",")
    app = PublicClientApplication(client_id=client_id, authority=authority_url, token_cache=create_cache())
    redirect_port = random.randint(53000, 60000)
    # random port so we can run the test suite in parallel
    creds = (
        app.acquire_token_silent(scopes, account=accounts[0])
        if (accounts := app.get_accounts())
        else app.acquire_token_interactive(
            scopes=scopes,
            port=redirect_port,
        )
    )
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
