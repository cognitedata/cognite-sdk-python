from datetime import datetime

import pytest

from cognite.client.data_classes.data_modeling.cdm.v1 import Cognite3DModelApply, CogniteSourceableNodeApply
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorFileApply


class TestSourceable:
    def test_dump_load(self) -> None:
        today = datetime.today()
        source = CogniteSourceableNodeApply(
            "sp_data_space",
            "my_source",
            source_id="source_id",
            source=("sp_data_space", "sap"),
            source_context="imagination",
            source_created_time=today,
            source_updated_time=today,
            source_created_user="Anders",
            source_updated_user="Anders",
        )

        assert source.dump() == {
            "space": "sp_data_space",
            "externalId": "my_source",
            "instanceType": "node",
            "sources": [
                {
                    "source": {
                        "space": "cdf_cdm",
                        "externalId": "CogniteSourceable",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "source": {"space": "sp_data_space", "externalId": "sap"},
                        "sourceContext": "imagination",
                        "sourceId": "source_id",
                        "sourceCreatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceUpdatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceCreatedUser": "Anders",
                        "sourceUpdatedUser": "Anders",
                    },
                }
            ],
        }


class TestModel3D:
    def test_dump(self) -> None:
        my_model = Cognite3DModelApply(
            "sp_data_space",
            "my_model",
            name="The model",
            description="A model",
            model_type="PointCloud",
            aliases=["alias1", "alias2"],
            tags=["tag1", "tag2"],
        )

        dumped = my_model.dump()
        assert dumped == {
            "space": "sp_data_space",
            "externalId": "my_model",
            "instanceType": "node",
            "sources": [
                {
                    "source": {
                        "space": "cdf_cdm",
                        "externalId": "Cognite3DModel",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "name": "The model",
                        "description": "A model",
                        "type": "PointCloud",
                        "aliases": ["alias1", "alias2"],
                        "tags": ["tag1", "tag2"],
                        "thumbnail": None,
                    },
                }
            ],
        }
        dumped_and_loaded = Cognite3DModelApply.load(dumped)
        assert dumped_and_loaded == my_model


def test_extractor_file_apply_warns_on_system_managed_fields() -> None:
    file_apply = CogniteExtractorFileApply(
        space="sp_data_space",
        external_id="my_file",
        is_uploaded=True,
        uploaded_time=datetime.now(),
    )
    with pytest.warns(UserWarning, match="system-managed and cannot be modified"):
        dumped = file_apply.dump()

    sources = dumped["sources"]
    assert len(sources) == 1
    properties = sources[0]["properties"]
    assert properties["isUploaded"] is True
    assert isinstance(properties["uploadedTime"], str)


def test_extractor_file_apply_skips_nulls() -> None:
    file_apply = CogniteExtractorFileApply(
        space="sp_data_space",
        external_id="my_file",
        is_uploaded=None,
        uploaded_time=None,
    )
    dumped = file_apply.dump()

    sources = dumped["sources"]
    assert len(sources) == 1
    properties = sources[0]["properties"]
    assert "isUploaded" not in properties
    assert "uploadedTime" not in properties
