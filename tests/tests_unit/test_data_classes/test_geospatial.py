from __future__ import annotations

from cognite.client.data_classes.geospatial import Feature, FeatureList


class TestFeature:
    def test_load_feature(self) -> None:
        feature = Feature.load(
            {
                "firstName": "name",
                "temperature_Hot": 12.0,
                "levelH2O": 12.0,
                "externalId": "f1",
                "createdTime": 1654612200225,
                "lastUpdatedTime": 1654612200225,
                "dataSetId": 12,
            }
        )
        assert feature.firstName == "name"  # type:ignore[attr-defined]
        assert feature.temperature_Hot == 12.0  # type:ignore[attr-defined]
        assert feature.levelH2O == 12.0  # type:ignore[attr-defined]
        assert feature.external_id == "f1"
        assert feature.created_time == 1654612200225
        assert feature.last_updated_time == 1654612200225
        assert feature.data_set_id == 12

    def test_dump_feature(self) -> None:
        feature = Feature(
            external_id="f1",
            firstName="name",
            temperature_Hot=12.0,
            levelH2O=12.0,
            data_set_id=12,
            created_time=123,
            last_updated_time=123,
        )
        dumped = feature.dump(camel_case=True)
        assert dumped == {
            "externalId": "f1",
            "firstName": "name",
            "temperature_Hot": 12.0,
            "levelH2O": 12.0,
            "dataSetId": 12,
            "lastUpdatedTime": 123,
            "createdTime": 123,
        }
        dumped = feature.dump(camel_case=False)
        assert dumped == {
            "external_id": "f1",
            "firstName": "name",
            "temperature_Hot": 12.0,
            "levelH2O": 12.0,
            "data_set_id": 12,
            "last_updated_time": 123,
            "created_time": 123,
        }


class TestFeatureList:
    def test_load_feature_list(self) -> None:
        features = FeatureList.load(
            [
                {
                    "firstName": "name",
                    "temperature_Hot": 12.0,
                    "levelH2O": 12.0,
                    "externalId": "f1",
                    "createdTime": 1654612200225,
                    "lastUpdatedTime": 1654612200225,
                    "dataSetId": 12,
                }
            ]
        )
        assert len(features) == 1
        feature = features[0]
        assert feature.firstName == "name"  # type:ignore[attr-defined]
        assert feature.temperature_Hot == 12.0  # type:ignore[attr-defined]
        assert feature.levelH2O == 12.0  # type:ignore[attr-defined]
        assert feature.external_id == "f1"
        assert feature.created_time == 1654612200225
        assert feature.last_updated_time == 1654612200225

    def test_dump_feature_list(self) -> None:
        feature_list = FeatureList(
            [
                Feature(
                    external_id="f1",
                    firstName="name",
                    temperature_Hot=12.0,
                    levelH2O=12.0,
                    data_set_id=12,
                    created_time=123,
                    last_updated_time=123,
                )
            ]
        )
        dumped = feature_list.dump(camel_case=True)
        assert dumped == [
            {
                "externalId": "f1",
                "firstName": "name",
                "temperature_Hot": 12.0,
                "levelH2O": 12.0,
                "dataSetId": 12,
                "lastUpdatedTime": 123,
                "createdTime": 123,
            }
        ]
        dumped = feature_list.dump(camel_case=False)
        assert dumped == [
            {
                "external_id": "f1",
                "firstName": "name",
                "temperature_Hot": 12.0,
                "levelH2O": 12.0,
                "data_set_id": 12,
                "last_updated_time": 123,
                "created_time": 123,
            }
        ]
