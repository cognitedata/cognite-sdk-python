import random
import sys
import time
import uuid

import pytest
from cognite.client import utils
from cognite.client.exceptions import CogniteAPIError

from cognite.client import CogniteClient
from cognite.client.data_classes.geospatial import (
    CoordinateReferenceSystem,
    Feature,
    FeatureList,
    FeatureType,
    FeatureTypeUpdate,
    OrderSpec,
    PropertyAndSearchSpec,
)

COGNITE_CLIENT = CogniteClient(max_workers=1)

# sdk integration tests run concurrently on 3 python versions so this makes the CI builds independent from each other
FIXED_SRID = 121111 + sys.version_info.minor


@pytest.fixture()
def test_crs():
    wkt = """GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,
    AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],
    PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,
    AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]"""
    proj_string = """+proj=longlat +a=6377276.345 +b=6356075.41314024 +no_defs"""
    crs = COGNITE_CLIENT.geospatial.create_coordinate_reference_systems(
        crs=CoordinateReferenceSystem(srid=FIXED_SRID, wkt=wkt, proj_string=proj_string)
    )
    yield crs[0]
    COGNITE_CLIENT.geospatial.delete_coordinate_reference_systems(srids=[FIXED_SRID])


@pytest.fixture(params=[True, False])
def allow_crs_transformation(request):
    yield request.param


@pytest.fixture()
def test_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
        FeatureType(
            external_id=external_id,
            properties={
                "position": {"type": "POINT", "srid": "4326", "optional": "true"},
                "volume": {"type": "DOUBLE"},
                "temperature": {"type": "DOUBLE"},
                "pressure": {"type": "DOUBLE"},
            },
            search_spec={"vol_press_idx": {"properties": ["volume", "pressure"]}},
        )
    )
    yield feature_type
    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture()
def large_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type_spec = FeatureType(
        external_id=external_id, properties={f"attr{i}": {"type": "LONG"} for i in range(0, 80)}
    )
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(feature_type_spec)
    yield feature_type
    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture()
def another_test_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
        FeatureType(external_id=external_id, properties={"volume": {"type": "DOUBLE"}})
    )
    yield feature_type
    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture
def test_feature(test_feature_type):
    external_id = f"F_{uuid.uuid4().hex[:10]}"
    feature = COGNITE_CLIENT.geospatial.create_features(
        test_feature_type,
        Feature(
            external_id=external_id,
            position={"wkt": "POINT(2.2768485 48.8589506)"},
            temperature=12.4,
            volume=1212.0,
            pressure=2121.0,
        ),
    )
    yield feature
    COGNITE_CLIENT.geospatial.delete_features(test_feature_type, external_id=external_id)


@pytest.fixture
def two_test_features(test_feature_type):
    external_ids = [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(2)]
    features = [
        Feature(
            external_id=external_ids[i],
            position={"wkt": "POINT(2.2768485 48.8589506)"},
            temperature=12.4 + i,
            volume=1212.0,
            pressure=2121.0,
        )
        for i in range(2)
    ]
    feature = COGNITE_CLIENT.geospatial.create_features(test_feature_type, features)
    yield feature
    COGNITE_CLIENT.geospatial.delete_features(test_feature_type, external_id=external_ids)


@pytest.fixture
def another_test_feature(test_feature_type):
    external_id = f"F_{uuid.uuid4().hex[:10]}"
    feature = COGNITE_CLIENT.geospatial.create_features(
        test_feature_type, Feature(external_id=external_id, temperature=-10.8, pressure=123.456, volume=654.2)
    )
    yield feature
    COGNITE_CLIENT.geospatial.delete_features(test_feature_type, external_id=external_id)


@pytest.fixture
def many_features(large_feature_type):
    specs = [
        Feature(
            external_id=f"F_{uuid.uuid4().hex[:10]}", **{f"attr{i}": random.randint(10000, 20000) for i in range(0, 80)}
        )
        for _ in range(0, 2000)
    ]
    features = COGNITE_CLIENT.geospatial.create_features(large_feature_type, specs)
    yield features
    external_ids = [f.external_id for f in features]
    COGNITE_CLIENT.geospatial.delete_features(large_feature_type, external_id=external_ids)


# we clean up the old feature types from a previous failed run
def clean_old_feature_types():
    try:
        res = COGNITE_CLIENT.geospatial.list_feature_types()
        for ft in res:
            feature_type_age_in_milliseconds = time.time() * 1000 - ft.last_updated_time
            one_hour_in_milliseconds = 3600 * 1000
            if feature_type_age_in_milliseconds > one_hour_in_milliseconds:
                print(f"Deleting old feature type {ft.external_id}")
                COGNITE_CLIENT.geospatial.delete_feature_types(external_id=ft.external_id, recursive=True)
    except:
        pass


# we clean up the old custom CRS from a previous failed run
@pytest.fixture(autouse=True, scope="module")
def clean_old_custom_crs():
    try:
        COGNITE_CLIENT.geospatial.delete_coordinate_reference_systems(srids=[121111])  # clean up
    except:
        pass
    try:
        COGNITE_CLIENT.geospatial.delete_coordinate_reference_systems(srids=[FIXED_SRID])  # clean up
    except:
        pass


class TestGeospatialAPI:
    def test_create_features(self, test_feature_type, allow_crs_transformation):
        external_id = f"F_{uuid.uuid4().hex[:10]}"
        COGNITE_CLIENT.geospatial.create_features(
            test_feature_type,
            Feature(
                external_id=external_id,
                position={"wkt": "POINT(50 50)"},
                temperature=12.4,
                volume=1212.0,
                pressure=2121.0,
            ),
            allow_crs_transformation=allow_crs_transformation,
        )
        COGNITE_CLIENT.geospatial.delete_features(test_feature_type, external_id=external_id)

    def test_retrieve_single_feature_type_by_external_id(self, test_feature_type):
        assert (
            test_feature_type.external_id
            == COGNITE_CLIENT.geospatial.retrieve_feature_types(external_id=test_feature_type.external_id).external_id
        )

    def test_list_feature_types(self, test_feature_type):
        res = COGNITE_CLIENT.geospatial.list_feature_types()
        assert 0 < len(res) < 100

    def test_retrieve_single_feature_by_external_id(self, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.retrieve_features(
            feature_type=test_feature_type, external_id=test_feature.external_id
        )
        assert res.external_id == test_feature.external_id

    def test_update_single_feature(self, allow_crs_transformation, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.update_features(
            feature_type=test_feature_type,
            feature=Feature(external_id=test_feature.external_id, temperature=6.237, pressure=12.21, volume=34.43),
            allow_crs_transformation=allow_crs_transformation,
        )
        assert res.external_id == test_feature.external_id
        assert res.temperature == 6.237

    def test_search_single_feature(self, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"property": "temperature", "gt": 12.0}}, limit=10
        )
        assert res[0].external_id == test_feature.external_id
        assert res[0].temperature == 12.4
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"property": "temperature", "lt": 12.0}}, limit=10
        )
        assert len(res) == 0

    def test_retrieve_multiple_feature_types_by_external_id(self, test_feature_type, another_test_feature_type):
        assert (
            len(
                COGNITE_CLIENT.geospatial.retrieve_feature_types(
                    external_id=[test_feature_type.external_id, another_test_feature_type.external_id]
                )
            )
            == 2
        )

    def test_retrieve_multiple_features_by_external_id(self, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.retrieve_features(
            feature_type=test_feature_type, external_id=[test_feature.external_id, another_test_feature.external_id]
        )
        assert res[0].external_id == test_feature.external_id
        assert res[1].external_id == another_test_feature.external_id

    def test_search_multiple_features(self, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"property": "temperature", "gt": -20.0, "lt": 20.0}}
        )
        assert len(res) == 2
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"property": "temperature", "gt": 0.0, "lt": 20.0}}
        )
        assert len(res) == 1
        assert res[0].external_id == test_feature.external_id

    def test_search_wrong_crs(self, test_feature_type, test_feature):
        try:
            COGNITE_CLIENT.geospatial.search_features(
                feature_type=test_feature_type,
                filter={"stWithin": {"property": "location", "value": {"wkt": "", "srid": 3857}}},
                limit=10,
            )
            raise pytest.fail("searching features using a geometry in invalid crs should have raised an exception")
        except CogniteAPIError:
            pass

    def test_get_coordinate_reference_system(self):
        res = COGNITE_CLIENT.geospatial.get_coordinate_reference_systems(srids=4326)
        assert res[0].srid == 4326

    def test_get_multiple_coordinate_reference_systems(self):
        res = COGNITE_CLIENT.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        assert set(map(lambda x: x.srid, res)) == {4326, 4327}

    def test_list_coordinate_reference_systems(self):
        res = COGNITE_CLIENT.geospatial.list_coordinate_reference_systems()
        all = res
        assert len(all) > 8000
        res = COGNITE_CLIENT.geospatial.list_coordinate_reference_systems(only_custom=True)
        assert len(res) < len(all)

    def test_list_custom_coordinate_reference_systems(self, test_crs):
        res = COGNITE_CLIENT.geospatial.list_coordinate_reference_systems(only_custom=True)
        assert test_crs.srid in set(map(lambda x: x.srid, res))

    def test_recursive_delete_feature_types(self):
        external_id = f"FT_{uuid.uuid4().hex[:10]}"
        feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
            FeatureType(external_id=external_id, properties={"temperature": {"type": "DOUBLE"}})
        )
        COGNITE_CLIENT.geospatial.create_features(
            feature_type, Feature(external_id=f"F_{uuid.uuid4().hex[:10]}", temperature=12.4)
        )
        COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id, recursive=True)

    def test_search_with_output_selection(self, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={}, properties={"temperature": {}, "volume": {}}
        )
        assert len(res) == 2
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[1], "pressure")

    def test_search_with_output_srid_selection(
        self, allow_crs_transformation, test_feature_type, test_feature, another_test_feature
    ):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type,
            filter={},
            properties={"position": {"srid": "3857"}},
            allow_crs_transformation=allow_crs_transformation,
        )
        assert len(res) == 2
        assert hasattr(res[0], "position")
        assert res[0].position["wkt"] == "POINT(253457.6156334287 6250962.062720415)"
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[0], "volume")

    def test_search_with_order_by(self, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={}, order_by=[OrderSpec(property="temperature", direction="ASC")]
        )
        assert res[0].temperature == -10.8
        assert res[1].temperature == 12.4

        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={}, order_by=[OrderSpec(property="temperature", direction="DESC")]
        )
        assert res[0].temperature == 12.4
        assert res[1].temperature == -10.8

    def test_update_feature_types(self, test_feature_type):
        res = COGNITE_CLIENT.geospatial.update_feature_types(
            update=FeatureTypeUpdate(
                external_id=test_feature_type.external_id,
                add=PropertyAndSearchSpec(
                    properties={"altitude": {"type": "DOUBLE", "optional": True}},
                    search_spec={
                        "altitude_idx": {"properties": ["altitude"]},
                        "pos_alt_idx": {"properties": ["position", "altitude"]},
                    },
                ),
            ),
        )
        assert len(res) == 1
        assert len(res[0].properties) == 8
        assert len(res[0].search_spec) == 6

    def test_stream_features(self, large_feature_type, many_features):
        features = COGNITE_CLIENT.geospatial.stream_features(feature_type=large_feature_type, filter={})
        feature_list = FeatureList(list(features))
        assert len(feature_list) == len(many_features)

    def test_to_pandas(self, test_feature_type, two_test_features):
        df = two_test_features.to_pandas()
        assert list(df) == [
            "externalId",
            "position",
            "volume",
            "temperature",
            "pressure",
            "createdTime",
            "lastUpdatedTime",
        ]

    def test_to_geopandas(self, test_feature_type, two_test_features):
        gdf = two_test_features.to_geopandas(geometry="position")
        assert list(gdf) == [
            "externalId",
            "position",
            "volume",
            "temperature",
            "pressure",
            "createdTime",
            "lastUpdatedTime",
        ]
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

    def test_aggregate(self, test_feature_type, two_test_features):
        res = COGNITE_CLIENT.geospatial.aggregate_features(
            feature_type=test_feature_type, filter={}, property="temperature", aggregates=["min", "max"]
        )
        assert res[0].min == 12.4
        assert res[0].max == 13.4

        res = COGNITE_CLIENT.geospatial.aggregate_features(
            feature_type=test_feature_type,
            filter={"range": {"property": "temperature", "gt": 12.0, "lt": 13.0}},
            property="temperature",
            aggregates=["count"],
        )
        assert res[0].count == 1

        res = COGNITE_CLIENT.geospatial.aggregate_features(
            feature_type=test_feature_type,
            filter={},
            property="temperature",
            aggregates=["min", "max"],
            group_by=["externalId"],
        )
        assert len(res) == 2
