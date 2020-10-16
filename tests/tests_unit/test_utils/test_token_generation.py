import os

import pytest

from cognite.client import utils
from tests.utils import unset_env_var


@pytest.fixture
def token_environment_vars():
    local_client_id = "azure-client-id"
    local_client_secret = "azure-client-secret"
    local_token_endpoint = "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token"

    tmp_client_id = os.getenv("COGNITE_IDP_CLIENT_ID")
    tmp_client_secret = os.getenv("COGNITE_IDP_CLIENT_SECRET")
    tmp_token_endpoint = os.getenv("COGNITE_IDP_TOKEN_ENDPOINT")

    os.environ["COGNITE_IDP_CLIENT_ID"] = local_client_id
    os.environ["COGNITE_IDP_CLIENT_SECRET"] = local_client_secret
    os.environ["COGNITE_IDP_TOKEN_ENDPOINT"] = local_token_endpoint

    yield local_client_id, local_client_secret, local_token_endpoint

    os.environ["COGNITE_IDP_CLIENT_ID"] = tmp_client_id
    os.environ["COGNITE_IDP_CLIENT_SECRET"] = tmp_client_secret
    os.environ["COGNITE_IDP_TOKEN_ENDPOINT"] = tmp_token_endpoint


class TestTokenGenerationVarsSet:
    def test_all_token_environment_vars_set(self, token_environment_vars):
        assert True == utils._token_generator.TokenGenerator.environment_vars_set()

    @pytest.mark.parametrize(
        "missing", ["COGNITE_IDP_CLIENT_ID", "COGNITE_IDP_CLIENT_SECRET", "COGNITE_IDP_TOKEN_ENDPOINT"]
    )
    def test_missing_token_environment_vars(self, missing, token_environment_vars):
        with unset_env_var(missing):
            assert False == utils._token_generator.TokenGenerator.environment_vars_set()
