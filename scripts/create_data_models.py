"""
This module is used to populate CDF with data modeling components such as data models, views, and containers which is
used in the integration tests.
"""
from pathlib import Path

import yaml

import cognite.client.data_classes.data_modeling as m
from cognite.client import CogniteClient


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
    # sdk_integration = Space(
    #     space="sdkIntegrationTests",
    #     description="Space used for integration testing in the SDK",
    #     name="SDK Integration Testing",
    # )
    space = m.SpaceApply(
        space="IntegrationTestsImmutable",
        description="Space used for integration testing in the SDK",
        name="SDK Integration Testing copy from Pygen",
    )
    # empty_data_model = DataModel(space=sdk_integration.space, external_id="integrationTestEmptyModel", version="v0")
    # client.data_modeling.data_models.apply(empty_data_model)
    # print("Empty data model added")
    #
    # _ = client.data_modeling.spaces.apply(immutable_space)
    # print("Space added")
    #
    # containers = pygen.data_modeling.containers.list(-1)
    # client.data_modeling.containers.apply(containers)
    # print("Containers added")
    #
    client.data_modeling.views.list(-1)
    # client.data_modeling.views.apply(views)
    #
    # print("Views added")
    #
    # data_models = pygen.data_modeling.data_models.list(-1, space=immutable_space.space)
    # client.data_modeling.data_models.apply(data_models)
    # print("Data Models added")
    # edges = pygen.data_modeling.instances.list(instance_type="edge", limit=-1)
    # print(edges)

    # for view in views:
    #     source = view.as_reference()
    #
    #     nodes = pygen.data_modeling.instances.list(instance_type="node", limit=-1, sources=[source])
    #     apply_nodes = [n.as_apply(source, 1) for n in nodes]
    #     created_nodes = client.data_modeling.instances.apply(apply_nodes)
    #     print(source)
    #     print(created_nodes)
    actor_view = pygen.data_modeling.views.retrieve((space.space, "Person"))
    ingested_edges = pygen.data_modeling.instances.list(
        instance_type="node", limit=-1, sources=actor_view[0].as_reference()
    )
    if len(ingested_edges) > 0:
        print("Skipping edges")
        return
    # for view in views:
    edges = pygen.data_modeling.instances.list(instance_type="edge", limit=-1)
    apply_edges = [n.as_apply(None, None) for n in edges]
    created_edges = client.data_modeling.instances.apply(apply_edges)
    print(created_edges)


if __name__ == "__main__":
    # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    # dump(local_client.get_pygen_access())
    copy_pygen_test_data(local_client.get_pygen_access(), local_client.get_interactive())
