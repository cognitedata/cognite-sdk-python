import os

from cognite.client.exceptions import CogniteAPIKeyError
from cognite.client.utils import _auxiliary


def _azure_environment_vars_set():
    azure_api_client_id = os.getenv("AZURE_API_CLIENT_ID")
    azure_api_client_secret = os.getenv("AZURE_API_CLIENT_SECRET")
    oidc_authority = os.getenv("OIDC_AUTHORITY")

    return azure_api_client_id is not None and azure_api_client_secret is not None and oidc_authority is not None


def _generate_access_token():
    # Microsoft Authentication Library (MSAL) for Python
    # https://github.com/AzureAD/microsoft-authentication-library-for-python
    msal = _auxiliary.local_import("msal")
    azure_api_client_id = os.getenv("AZURE_API_CLIENT_ID")
    azure_api_client_secret = os.getenv("AZURE_API_CLIENT_SECRET")
    oidc_authority = os.getenv("OIDC_AUTHORITY")
    cognite_base_url = os.getenv("COGNITE_BASE_URL", "https://api.cognitedata.com")

    app = msal.ConfidentialClientApplication(
        azure_api_client_id, client_credential=azure_api_client_secret, authority=oidc_authority
    )
    # When using the client-credentials flow with msal the only valid scope is .default
    scopes = [f"{cognite_base_url}/.default"]
    print(app)
    response = app.acquire_token_for_client(scopes=scopes)
    print(response)
    access_token = response.get("access_token")
    print(access_token)
    if access_token is None:
        print("should be here")
        raise CogniteAPIKeyError(
            "Could not generate azure access token from provided azure related environment variables"
        )

    return access_token
