import os
from unittest.mock import patch

import pytest

from cognite.client import utils
from cognite.client.exceptions import CogniteAPIKeyError
from tests.utils import unset_env_var


@pytest.fixture
def azure_environment_vars():
    azure_api_client_id = "azure-client-id"
    azure_api_client_secret = "azure-client-secret"
    oidc_authority = "oidc-authrity"

    tmp_azure_api_client_id = os.environ["AZURE_API_CLIENT_ID"]
    tmp_azure_api_client_secret = os.environ["AZURE_API_CLIENT_SECRET"]
    tmp_oidc_authority = os.environ["OIDC_AUTHORITY"]

    os.environ["AZURE_API_CLIENT_ID"] = azure_api_client_id
    os.environ["AZURE_API_CLIENT_SECRET"] = azure_api_client_secret
    os.environ["OIDC_AUTHORITY"] = oidc_authority

    yield azure_api_client_id, azure_api_client_secret, oidc_authority

    os.environ["AZURE_API_CLIENT_ID"] = tmp_azure_api_client_id
    os.environ["AZURE_API_CLIENT_SECRET"] = tmp_azure_api_client_secret
    os.environ["OIDC_AUTHORITY"] = tmp_oidc_authority


class TestAzureTokenGenerationVarsSet:
    def test_all_azure_environment_vars_set(self, azure_environment_vars):
        assert True == utils._azure_token_generation._azure_environment_vars_set()

    @pytest.mark.parametrize("missing", ["AZURE_API_CLIENT_ID", "AZURE_API_CLIENT_SECRET", "OIDC_AUTHORITY"])
    def test_missing_azure_environment_vars(self, missing, azure_environment_vars):
        with unset_env_var(missing):
            assert False == utils._azure_token_generation._azure_environment_vars_set()


class TestGenerateAccessToken:
    @patch("msal.ConfidentialClientApplication")
    def test_access_token_generated(self, msal_client_mock):
        msal_client_mock().acquire_token_for_client.return_value = {"access_token": "azure_token"}
        token = utils._azure_token_generation._generate_access_token()
        assert "azure_token" == token

    @patch("msal.ConfidentialClientApplication")
    def test_access_token_not_generated(self, msal_client_mock):
        msal_client_mock().acquire_token_for_client.return_value = {}

        with pytest.raises(
            CogniteAPIKeyError,
            match="Could not generate azure access token from provided azure related environment variables",
        ):
            utils._azure_token_generation._generate_access_token()
