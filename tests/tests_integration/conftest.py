import os
import random
from pathlib import Path

import pytest
from dotenv import load_dotenv

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCertificate, OAuthClientCredentials, OAuthInteractive
from cognite.client.data_classes import DataSet, DataSetWrite
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from tests.utils import REPO_ROOT


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    return make_cognite_client(beta=False)


@pytest.fixture(scope="session")
def cognite_client_alpha() -> CogniteClient:
    load_dotenv(REPO_ROOT / "alpha.env")
    if "COGNITE_ALPHA_PROJECT" not in os.environ:
        # TODO: If we are in CI, we should fail the test instead of skipping
        pytest.skip("ALPHA environment variables not set. Skipping ALPHA tests.")
    return CogniteClient.default_oauth_client_credentials(
        project=os.environ["COGNITE_ALPHA_PROJECT"],
        cdf_cluster=os.environ["COGNITE_ALPHA_CLUSTER"],
        client_id=os.environ["COGNITE_ALPHA_CLIENT_ID"],
        client_secret=os.environ["COGNITE_ALPHA_CLIENT_SECRET"],
        tenant_id=os.environ["COGNITE_ALPHA_TENANT_ID"],
    )


@pytest.fixture(scope="session")
def alpha_test_space(cognite_client_alpha: CogniteClient) -> Space:
    return cognite_client_alpha.data_modeling.spaces.apply(SpaceApply(space="sp_python_sdk_instance_id_tests"))


@pytest.fixture(scope="session")
def alpha_test_dataset(cognite_client_alpha: CogniteClient) -> DataSet:
    ds = DataSetWrite(name="ds_python_sdk_instance_id_tests", external_id="ds_python_sdk_instance_id_tests")
    retrieved = cognite_client_alpha.data_sets.retrieve(external_id=ds.external_id)
    if retrieved:
        return retrieved
    return cognite_client_alpha.data_sets.create(ds)


@pytest.fixture(scope="session")
def cognite_client_beta() -> CogniteClient:
    return make_cognite_client(beta=True)


def make_cognite_client(beta: bool = False) -> CogniteClient:
    login_flow = os.environ["LOGIN_FLOW"].lower()
    if login_flow == "client_credentials":
        credentials = OAuthClientCredentials(
            token_url=os.environ["COGNITE_TOKEN_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            client_secret=os.environ["COGNITE_CLIENT_SECRET"],
            scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
        )
    elif login_flow == "interactive":
        credentials = OAuthInteractive(
            authority_url=os.environ["COGNITE_AUTHORITY_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
            redirect_port=random.randint(53000, 60000),  # random port so we can run the test suite in parallel
        )
    elif login_flow == "client_certificate":
        credentials = OAuthClientCertificate(
            authority_url=os.environ["COGNITE_AUTHORITY_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            cert_thumbprint=os.environ["COGNITE_CERT_THUMBPRINT"],
            certificate=Path(os.environ["COGNITE_CERTIFICATE"]).read_text(),
            scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
        )
    else:
        raise ValueError(
            "Environment variable LOGIN_FLOW must be set to 'client_credentials', 'client_certificate' or 'interactive'"
        )

    beta_configuration = dict(api_subversion="beta") if beta else dict()

    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=credentials,
            **beta_configuration,
        )
    )
