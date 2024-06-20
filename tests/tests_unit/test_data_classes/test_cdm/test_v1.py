from datetime import datetime

from cognite.client.data_classes.cdm.v1 import SourceableWrite


class TestSourceable:
    def test_dump_load(self) -> None:
        today = datetime.today()
        source = SourceableWrite(
            "sp_data_space",
            "my_source",
            source="imagination",
            source_id="source_id",
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
                        "space": "cdf_cdm_experimental",
                        "externalId": "Sourceable",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "source": "imagination",
                        "sourceId": "source_id",
                        "sourceCreatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceUpdatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceCreatedUser": "Anders",
                        "sourceUpdatedUser": "Anders",
                    },
                }
            ],
        }
