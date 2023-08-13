from cognite.client.data_classes.geospatial import Feature, FeatureList


class TestFeature:
    def test_load_feature(self):
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
        assert feature.firstName == "name"
        assert feature.temperature_Hot == 12.0
        assert feature.levelH2O == 12.0
        assert feature.external_id == "f1"
        assert feature.created_time == 1654612200225
        assert feature.last_updated_time == 1654612200225
        assert feature.data_set_id == 12

    def test_dump_feature(self):
        feature = Feature(external_id="f1", firstName="name", temperature_Hot=12.0, levelH2O=12.0, data_set_id=12)
        dumped = feature.dump(camel_case=True)
        assert dumped == {
            "externalId": "f1",
            "firstName": "name",
            "temperature_Hot": 12.0,
            "levelH2O": 12.0,
            "dataSetId": 12,
        }
        dumped = feature.dump(camel_case=False)
        assert dumped == {
            "external_id": "f1",
            "firstName": "name",
            "temperature_Hot": 12.0,
            "levelH2O": 12.0,
            "data_set_id": 12,
        }


class TestFeatureList:
    def test_load_feature_list(self):
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
        assert feature.firstName == "name"
        assert feature.temperature_Hot == 12.0
        assert feature.levelH2O == 12.0
        assert feature.external_id == "f1"
        assert feature.created_time == 1654612200225
        assert feature.last_updated_time == 1654612200225

    def test_dump_feature_list(self):
        feature_list = FeatureList(
            [Feature(external_id="f1", firstName="name", temperature_Hot=12.0, levelH2O=12.0, data_set_id=12)]
        )
        dumped = feature_list.dump(camel_case=True)
        assert dumped == [
            {"externalId": "f1", "firstName": "name", "temperature_Hot": 12.0, "levelH2O": 12.0, "dataSetId": 12}
        ]
        dumped = feature_list.dump(camel_case=False)
        assert dumped == [
            {"external_id": "f1", "firstName": "name", "temperature_Hot": 12.0, "levelH2O": 12.0, "data_set_id": 12}
        ]
