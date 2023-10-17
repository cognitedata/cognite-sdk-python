import math
import uuid

import pytest

from cognite.client.data_classes.geospatial import Feature, FeatureList, FeatureType
from cognite.client.utils._importing import local_import


@pytest.fixture()
def test_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    yield FeatureType(
        external_id=external_id,
        properties={
            "position": {"type": "GEOMETRY", "srid": "4326", "optional": "true"},
            "volume": {"type": "DOUBLE"},
            "temperature": {"type": "DOUBLE"},
            "pressure": {"type": "DOUBLE"},
            "weight": {"type": "DOUBLE", "optional": "true"},
            "description": {"type": "STRING", "optional": "true"},
            "dataSetId": {"type": "LONG", "optional": "true"},
            "assetIds": {"type": "LONGARRAY", "optional": "true"},
        },
        search_spec={"vol_press_idx": {"properties": ["volume", "pressure"]}},
    )


@pytest.fixture
def test_features(test_feature_type):
    external_ids = [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(4)]
    yield FeatureList(
        [
            Feature(
                external_id=external_ids[0],
                position={"wkt": "POINT(2.2768485 48.8589506)"},
                temperature=12.4,
                volume=1212.0,
                pressure=2121.0,
                assetIds=[1, 2],
            ),
            Feature(
                external_id=external_ids[1],
                position={"wkt": "POLYGON((10.689 -25.092, 38.814 -35.639, 13.502 -39.155, 10.689 -25.092))"},
                temperature=13.4,
                volume=1212.0,
                pressure=2121.0,
            ),
            Feature(
                external_id=external_ids[2],
                position={"wkt": "LINESTRING (30 10, 10 30, 40 40)"},
                temperature=13.4,
                volume=1212.0,
                pressure=2121.0,
                data_set_id=12,
            ),
            Feature(
                external_id=external_ids[3],
                position={"wkt": "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"},
                temperature=13.4,
                volume=1212.0,
                pressure=2121.0,
                data_set_id=12,
            ),
        ]
    )


class TestGeospatialAPI:
    @pytest.mark.dsl
    def test_to_pandas(self, test_feature_type, test_features):
        df = test_features.to_pandas(camel_case=True)
        assert set(list(df)) == {"externalId", "dataSetId", "position", "volume", "temperature", "pressure", "assetIds"}

    @pytest.mark.dsl
    def test_to_geopandas(self, test_feature_type, test_features):
        gdf = test_features.to_geopandas(geometry="position", camel_case=True)
        assert set(gdf) == {"externalId", "dataSetId", "position", "volume", "temperature", "pressure", "assetIds"}
        geopandas = local_import("geopandas")
        assert type(gdf.dtypes["position"]) is geopandas.array.GeometryDtype

    @pytest.mark.dsl
    def test_from_geopandas(self, test_feature_type, test_features):
        gdf = test_features.to_geopandas(geometry="position", camel_case=True)
        fl = FeatureList.from_geopandas(test_feature_type, gdf)
        assert type(fl) is FeatureList
        assert len(fl) == 4
        for idx, f in enumerate(fl):
            for attr_name in test_feature_type.properties:
                if attr_name.startswith("_") or attr_name in ["description", "dataSetId", "assetIds", "weight"]:
                    continue
                assert hasattr(f, attr_name)
            assert not hasattr(f, "description")
            assert not hasattr(f, "weight")
            if idx == 0:
                assert hasattr(f, "asset_ids")
            if idx > 1:
                assert hasattr(f, "data_set_id")

    @pytest.mark.dsl
    def test_from_geopandas_missing_column(self, test_feature_type):
        pd = local_import("pandas")
        df = pd.DataFrame([{"externalId": "12", "volume": 12.0}])
        geopandas = local_import("geopandas")
        gdf = geopandas.GeoDataFrame(df)
        with pytest.raises(ValueError) as error:
            FeatureList.from_geopandas(test_feature_type, gdf)
        assert str(error.value) == "Missing value for property temperature"

    @pytest.mark.dsl
    def test_from_geopandas_nan_values(self, test_feature_type):
        pd = local_import("pandas")
        df = pd.DataFrame(
            [
                {"externalId": "12", "temperature": 11.0, "pressure": 10.0, "volume": 12.0, "weight": 10.0},
                {
                    "externalId": "13",
                    "temperature": 0.0,
                    "pressure": 1.0,
                    "volume": 11.0,
                    "weight": math.nan,
                    "description": "string",
                    "assetIds": [1, 2],
                },
            ]
        )
        geopandas = local_import("geopandas")
        gdf = geopandas.GeoDataFrame(df)
        features = FeatureList.from_geopandas(test_feature_type, gdf)
        assert features[0].weight == 10.0
        assert not hasattr(features[1], "weight")
        assert features[1].description == "string"
        assert features[1].asset_ids == [1, 2]
