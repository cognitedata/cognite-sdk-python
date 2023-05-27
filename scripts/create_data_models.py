from pathlib import Path

import yaml

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space


def dump(client: CogniteClient):
    data_models = client.data_modeling.data_models.list(limit=-1)
    with (Path.cwd() / "data_models.yml").open("w") as f:
        yaml.dump(data_models.dump(), f)

    views = client.data_modeling.views.list(limit=-1)
    with (Path.cwd() / "views.yml").open("w") as f:
        yaml.dump(views.dump(), f)

    containers = client.data_modeling.containers.list(limit=-1)
    with (Path.cwd() / "containers.yml").open("w") as f:
        yaml.dump(containers.dump(), f)


def copy_pygen_test_data(pygen: CogniteClient, client: CogniteClient):
    Space(
        space="sdkIntegrationTests",
        description="Space used for integration testing in the SDK",
        name="SDK Integration Testing",
    )
    Space(
        space="IntegrationTestsImmutable",
        description="Space used for integration testing in the SDK",
        name="SDK Integration Testing copy from Pygen",
    )

    # _ = client.data_modeling.spaces.apply(immutable_space)
    # print("Space added")
    #
    # containers = pygen.data_modeling.containers.list(-1)
    # client.data_modeling.containers.apply(containers)
    # print("Containers added")

    # views = pygen.data_modeling.views.list(-1)
    # client.data_modeling.views.apply(views)
    #
    # print("Views added")

    # data_models = pygen.data_modeling.data_models.list(-1)
    # client.data_modeling.data_models.apply(data_models)
    # print("Data Models added")


if __name__ == "__main__":
    # # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    # dump(local_client.get_pygen_access())
    copy_pygen_test_data(local_client.get_pygen_access(), local_client.get_interactive())
