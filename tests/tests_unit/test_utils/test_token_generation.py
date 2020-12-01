import datetime
from unittest.mock import Mock, patch

import pytest
from oauthlib.oauth2 import InvalidClientIdError

from cognite.client import utils
from cognite.client.exceptions import CogniteAPIKeyError


def default_token_generator_args():
    return {
        "client_id": "azure-client-id",
        "client_secret": "azure-client-secret",
        "token_url": "https://login.microsoftonline.com/testingabc123/oauth2/v2.0/token",
        "token_scopes": ["https://greenfield.cognitedata.com/.default"],
        "token_custom_args": {},
    }


def setup_token_generator(generator_args=default_token_generator_args()):
    return utils._token_generator.TokenGenerator(
        generator_args["token_url"],
        generator_args["client_id"],
        generator_args["client_secret"],
        generator_args["token_scopes"],
        generator_args["token_custom_args"],
    )


class TestTokenGeneration:
    @patch("cognite.client.utils._token_generator.BackendApplicationClient")
    @patch("cognite.client.utils._token_generator.OAuth2Session")
    def test_all_token_environment_vars_set(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session.fetch_token.return_value = {}
        generator = setup_token_generator()
        assert True == generator.token_params_set()

    @pytest.mark.parametrize("missing", ["token_url", "client_id", "client_secret", "token_scopes"])
    def test_missing_token_environment_vars(self, missing):
        generator_args = default_token_generator_args()
        generator_args[missing] = None
        generator = setup_token_generator(generator_args)
        assert False == generator.token_params_set()

    @patch("cognite.client.utils._token_generator.BackendApplicationClient")
    @patch("cognite.client.utils._token_generator.OAuth2Session")
    def test_access_token_generated(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.return_value = {
            "access_token": "azure_token",
            "expires_at": datetime.datetime.now().timestamp() + 1000,
        }
        generator = setup_token_generator()
        assert "azure_token" == generator.return_access_token()

    @patch("cognite.client.utils._token_generator.BackendApplicationClient")
    @patch("cognite.client.utils._token_generator.OAuth2Session")
    def test_access_token_not_generated(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.return_value = {}
        generator = setup_token_generator()
        with pytest.raises(
            CogniteAPIKeyError, match="Could not generate access token from provided token generation arguments"
        ):
            generator.return_access_token()

    @patch("cognite.client.utils._token_generator.BackendApplicationClient")
    @patch("cognite.client.utils._token_generator.OAuth2Session")
    def test_access_token_not_generated_due_to_error(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.side_effect = InvalidClientIdError()
        with pytest.raises(
            CogniteAPIKeyError,
            match="Error generating access token: invalid_request, 400, Invalid client_id parameter value.",
        ):
            args = default_token_generator_args()
            utils._token_generator.TokenGenerator(
                args["token_url"],
                args["client_id"],
                args["client_secret"],
                args["token_scopes"],
                args["token_custom_args"],
            )

    def test_access_token_not_generated_missing_args(self):
        generator_args = default_token_generator_args()
        generator_args["client_secret"] = None
        generator = setup_token_generator(generator_args)
        with pytest.raises(
            CogniteAPIKeyError, match="Could not generate access token - missing token generation arguments"
        ):
            generator.return_access_token()

    @patch("cognite.client.utils._token_generator.BackendApplicationClient")
    @patch("cognite.client.utils._token_generator.OAuth2Session")
    def test_access_token_expired(self, mock_oauth_session, mock_backend_client):
        mock_backend_client().return_value = Mock()
        mock_oauth_session().fetch_token.side_effect = [
            {"access_token": "azure_token_expired", "expires_at": datetime.datetime.now().timestamp() - 1000},
            {"access_token": "azure_token_refreshed", "expires_at": datetime.datetime.now().timestamp() + 1000},
        ]
        generator = setup_token_generator()
        assert "azure_token_expired" == generator._access_token
        assert "azure_token_refreshed" == generator.return_access_token()
