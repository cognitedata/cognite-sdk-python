"""
This module is used to populate CDF with data modeling components such as data models, views, and containers which is
used in the integration tests.
"""
from pathlib import Path

import yaml

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space
from cognite.client.data_classes.data_modeling.data_models import DataModel


def dump(client: CogniteClient):
    data_models = client.data_modeling.models.list(limit=-1)
    with (Path.cwd() / "data_models.yml").open("w") as f:
        yaml.dump(data_models.dump(), f)

    views = client.data_modeling.views.list(limit=-1)
    with (Path.cwd() / "views.yml").open("w") as f:
        yaml.dump(views.dump(), f)

    containers = client.data_modeling.containers.list(limit=-1)
    with (Path.cwd() / "containers.yml").open("w") as f:
        yaml.dump(containers.dump(), f)


def copy_pygen_test_data(pygen: CogniteClient, client: CogniteClient):
    sdk_integration = Space(
        space="sdkIntegrationTests",
        description="Space used for integration testing in the SDK",
        name="SDK Integration Testing",
    )
    immutable_space = Space(
        space="IntegrationTestsImmutable",
        description="Space used for integration testing in the SDK",
        name="SDK Integration Testing copy from Pygen",
    )
    empty_data_model = DataModel(space=sdk_integration.space, external_id="integrationTestEmptyModel", version="v0")
    client.data_modeling.models.apply(empty_data_model)
    print("Empty data model added")

    _ = client.data_modeling.spaces.apply(immutable_space)
    print("Space added")

    containers = pygen.data_modeling.containers.list(-1)
    client.data_modeling.containers.apply(containers)
    print("Containers added")

    views = pygen.data_modeling.views.list(-1)
    client.data_modeling.views.apply(views)

    print("Views added")

    data_models = pygen.data_modeling.models.list(-1, space=immutable_space.space)
    client.data_modeling.models.apply(data_models)
    print("Data Models added")


if __name__ == "__main__":
    # # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    # dump(local_client.get_pygen_access())
    copy_pygen_test_data(local_client.get_pygen_access(), local_client.get_interactive())
