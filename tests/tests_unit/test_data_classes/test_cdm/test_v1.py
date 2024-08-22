from datetime import datetime

from cognite.client.data_classes.cdm.v1 import Cognite3DModelApply, CogniteSourceableNodeApply


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
            type_="PointCloud",
            aliases=["alias1", "alias2"],
            tags=["tag1", "tag2"],
        )

        assert my_model.dump() == {
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
                    },
                }
            ],
        }
