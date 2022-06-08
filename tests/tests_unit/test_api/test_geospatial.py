import uuid

import pytest

from cognite.client import utils
from cognite.client.data_classes.geospatial import Feature, FeatureList, FeatureType


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
            "description": {"type": "STRING", "optional": "true"}
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
            ),
            Feature(
                external_id=external_ids[3],
                position={"wkt": "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"},
                temperature=13.4,
                volume=1212.0,
                pressure=2121.0,
            ),
        ]
    )


class TestGeospatialAPI:
    @pytest.mark.dsl
    def test_to_pandas(self, test_feature_type, test_features):
        df = test_features.to_pandas()
        assert set(list(df)) == set(["externalId", "position", "volume", "temperature", "pressure"])

    @pytest.mark.dsl
    def test_to_geopandas(self, test_feature_type, test_features):
        gdf = test_features.to_geopandas(geometry="position")
        assert set(gdf) == set(["externalId", "position", "volume", "temperature", "pressure"])
        geopandas = utils._auxiliary.local_import("geopandas")
        assert type(gdf.dtypes["position"]) == geopandas.array.GeometryDtype

    @pytest.mark.dsl
    def test_from_geopandas(self, test_feature_type, test_features):
        gdf = test_features.to_geopandas(geometry="position")
        fl = FeatureList.from_geopandas(test_feature_type, gdf)
        assert type(fl) == FeatureList
        assert len(fl) == 4
        for f in fl:
            for attr in test_feature_type.properties.items():
                attr_name = attr[0]
                if attr_name.startswith("_") or attr_name == "description":
                    continue
                assert hasattr(f, attr_name)

    @pytest.mark.dsl
    def test_from_geopandas_missing_column(self, test_feature_type):
        pd = utils._auxiliary.local_import("pandas")
        df = pd.DataFrame([{"externalId": "12", "volume": 12.0}])
        geopandas = utils._auxiliary.local_import("geopandas")
        gdf = geopandas.GeoDataFrame(df)
        with pytest.raises(ValueError) as error:
            FeatureList.from_geopandas(test_feature_type, gdf)
        assert str(error.value) == "Missing value for property temperature"
