from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.transformations import (
    Transformation,
    TransformationFilter,
    TransformationUpdate,
    TransformationWrite,
)


@pytest.fixture
def transformation_resource() -> dict[str, Any]:
    return {
        "id": 1,
        "externalId": "my-transformation",
        "name": "My transformation",
        "query": "SELECT 1",
        "destination": {"type": "assets"},
        "conflictMode": "upsert",
        "isPublic": True,
        "ignoreNullFields": False,
        "createdTime": 1,
        "lastUpdatedTime": 2,
        "owner": "someone",
        "ownerIsCurrentUser": True,
        "dataSetId": 123,
        "dataDomainExternalId": "my-domain",
    }


class TestTransformationDataDomainExternalId:
    def test_load_sets_data_domain_external_id(self, transformation_resource: dict[str, Any]) -> None:
        transformation = Transformation._load(transformation_resource)

        assert transformation.data_domain_external_id == "my-domain"

    def test_load_defaults_to_none_when_absent(self, transformation_resource: dict[str, Any]) -> None:
        del transformation_resource["dataDomainExternalId"]

        transformation = Transformation._load(transformation_resource)

        assert transformation.data_domain_external_id is None

    def test_dump_round_trip(self, transformation_resource: dict[str, Any]) -> None:
        transformation = Transformation._load(transformation_resource)

        dumped = transformation.dump(camel_case=True)

        assert dumped["dataDomainExternalId"] == "my-domain"

    def test_as_write_preserves_data_domain_external_id(self, transformation_resource: dict[str, Any]) -> None:
        transformation = Transformation._load(transformation_resource)

        written = transformation.as_write()

        assert isinstance(written, TransformationWrite)
        assert written.data_domain_external_id == "my-domain"

    def test_copy_preserves_data_domain_external_id(self, transformation_resource: dict[str, Any]) -> None:
        transformation = Transformation._load(transformation_resource)

        copied = transformation.copy()

        assert copied.data_domain_external_id == "my-domain"

    def test_transformation_write_dump_load_round_trip(self) -> None:
        write = TransformationWrite(external_id="my-transformation", name="My transformation")
        write.data_domain_external_id = "my-domain"

        loaded = TransformationWrite._load(write.dump(camel_case=True))

        assert loaded.data_domain_external_id == "my-domain"

    def test_update_setter_dumps_expected_shape(self) -> None:
        update = TransformationUpdate(id=1).data_domain_external_id.set("my-domain")

        dumped = update.dump(camel_case=True)

        assert dumped["update"]["dataDomainExternalId"] == {"set": "my-domain"}

    def test_filter_dumps_data_domain_external_ids(self) -> None:
        filter = TransformationFilter(data_domain_external_ids=["my-domain", "other-domain"])

        dumped = filter.dump(camel_case=True)

        assert dumped["dataDomainExternalIds"] == ["my-domain", "other-domain"]
