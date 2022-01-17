import pytest
import uuid

from cognite.client import utils

from cognite.client.data_classes.geospatial import (
    FeatureType,
    Feature,
    FeatureList,
)

@pytest.fixture()
def test_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    yield FeatureType(
        external_id=external_id,
        properties={
            "position": {"type": "POINT", "srid": "4326", "optional": "true"},
            "volume": {"type": "DOUBLE"},
            "temperature": {"type": "DOUBLE"},
            "pressure": {"type": "DOUBLE"},
        },
        search_spec={"vol_press_idx": {"properties": ["volume", "pressure"]}})


@pytest.fixture
def two_test_features(test_feature_type):
    external_ids = [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(2)]
    yield FeatureList([
        Feature(
            external_id=external_ids[i],
            position={"wkt": "POINT(2.2768485 48.8589506)"},
            temperature=12.4 + i,
            volume=1212.0,
            pressure=2121.0,
        )
        for i in range(2)
    ])


class TestGeospatialAPI:

    def test_to_pandas(self, test_feature_type, two_test_features):
        df = two_test_features.to_pandas()
        assert set(list(df)) == set(["externalId", "position", "volume", "temperature", "pressure"])

    def test_to_geopandas(self, test_feature_type, two_test_features):
        gdf = two_test_features.to_geopandas(geometry="position")
        assert set(gdf) == set([
            "externalId",
            "position",
            "volume",
            "temperature",
            "pressure"
        ])
        geopandas = utils._auxiliary.local_import("geopandas")
        assert type(gdf.dtypes["position"]) == geopandas.array.GeometryDtype

    def test_from_geopandas(self, test_feature_type, two_test_features):
        gdf = two_test_features.to_geopandas(geometry="position")
        fl = FeatureList.from_geopandas(test_feature_type, gdf)
        assert type(fl) == FeatureList
        assert len(fl) == 2
        for f in fl:
            for attr in test_feature_type.properties.items():
                attr_name = attr[0]
                if attr_name.startswith("_"):
                    continue
                assert hasattr(f, attr_name)