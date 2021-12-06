import random
import sys
import time
import uuid

import pytest
from cognite.client.exceptions import CogniteAPIError

from cognite.client import CogniteClient
from cognite.client.data_classes.geospatial import (
    AttributeAndSearchSpec,
    CoordinateReferenceSystem,
    Feature,
    FeatureList,
    FeatureType,
    FeatureTypeUpdate,
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


@pytest.fixture(params=[None, "sdk_test"])
def cognite_domain(request):
    yield request.param


@pytest.fixture()
def test_feature_type(cognite_domain):
    COGNITE_CLIENT.geospatial.set_current_cognite_domain(cognite_domain)
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
        FeatureType(
            external_id=external_id,
            attributes={
                "position": {"type": "POINT", "srid": "4326", "optional": "true"},
                "volume": {"type": "DOUBLE"},
                "temperature": {"type": "DOUBLE"},
                "pressure": {"type": "DOUBLE"},
            },
            search_spec={"vol_press_idx": {"attributes": ["volume", "pressure"]}},
            cognite_domain=cognite_domain,
        )
    )
    yield feature_type
    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture()
def large_feature_type():
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type_spec = FeatureType(
        external_id=external_id, attributes={f"attr{i}": {"type": "LONG"} for i in range(0, 80)}
    )
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(feature_type_spec)
    yield feature_type
    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture()
def another_test_feature_type(cognite_domain):
    COGNITE_CLIENT.geospatial.set_current_cognite_domain(cognite_domain)
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
        FeatureType(external_id=external_id, attributes={"volume": {"type": "DOUBLE"}}, cognite_domain=cognite_domain)
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
        for domain in [None, "sdk_test", "smoke_test"]:
            COGNITE_CLIENT.geospatial.set_current_cognite_domain(domain)
            res = COGNITE_CLIENT.geospatial.list_feature_types()
            for ft in res:
                feature_type_age_in_milliseconds = time.time() * 1000 - ft.last_updated_time
                one_hour_in_milliseconds = 3600 * 1000
                if feature_type_age_in_milliseconds > one_hour_in_milliseconds:
                    print(
                        f"Deleting old feature type {ft.external_id} in domain {'default' if domain is None else domain}"
                    )
                    COGNITE_CLIENT.geospatial.delete_feature_types(external_id=ft.external_id, force=True)
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
    def test_retrieve_single_feature_type_by_external_id(self, cognite_domain, test_feature_type):
        assert (
            test_feature_type.external_id
            == COGNITE_CLIENT.geospatial.retrieve_feature_types(external_id=test_feature_type.external_id).external_id
        )
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_list_feature_types(self, cognite_domain, test_feature_type):
        res = COGNITE_CLIENT.geospatial.list_feature_types()
        assert 0 < len(res) < 100
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_retrieve_single_feature_by_external_id(self, cognite_domain, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.retrieve_features(
            feature_type=test_feature_type, external_id=test_feature.external_id
        )
        assert res.external_id == test_feature.external_id
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_update_single_feature(self, cognite_domain, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.update_features(
            feature_type=test_feature_type,
            feature=Feature(external_id=test_feature.external_id, temperature=6.237, pressure=12.21, volume=34.43),
        )
        assert res.external_id == test_feature.external_id
        assert res.temperature == 6.237
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_search_single_feature(self, cognite_domain, test_feature_type, test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"attribute": "temperature", "gt": 12.0}}, limit=10
        )
        assert res[0].external_id == test_feature.external_id
        assert res[0].temperature == 12.4
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"attribute": "temperature", "lt": 12.0}}, limit=10
        )
        assert len(res) == 0
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_retrieve_multiple_feature_types_by_external_id(
        self, cognite_domain, test_feature_type, another_test_feature_type
    ):
        assert (
            len(
                COGNITE_CLIENT.geospatial.retrieve_feature_types(
                    external_id=[test_feature_type.external_id, another_test_feature_type.external_id]
                )
            )
            == 2
        )
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_retrieve_multiple_features_by_external_id(
        self, cognite_domain, test_feature_type, test_feature, another_test_feature
    ):
        res = COGNITE_CLIENT.geospatial.retrieve_features(
            feature_type=test_feature_type, external_id=[test_feature.external_id, another_test_feature.external_id]
        )
        assert res[0].external_id == test_feature.external_id
        assert res[1].external_id == another_test_feature.external_id
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_search_multiple_features(self, cognite_domain, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"attribute": "temperature", "gt": -20.0, "lt": 20.0}}
        )
        assert len(res) == 2
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={"range": {"attribute": "temperature", "gt": 0.0, "lt": 20.0}}
        )
        assert len(res) == 1
        assert res[0].external_id == test_feature.external_id
        assert COGNITE_CLIENT.geospatial.get_current_cognite_domain() == cognite_domain

    def test_search_wrong_domain(self, cognite_domain, test_feature_type, test_feature, another_test_feature):
        COGNITE_CLIENT.geospatial.set_current_cognite_domain(None if cognite_domain == "sdk_test" else "sdk_test")
        try:
            COGNITE_CLIENT.geospatial.search_features(
                feature_type=test_feature_type,
                filter={"range": {"attribute": "temperature", "gt": -20.0, "lt": 20.0}},
                limit=10,
            )
            raise pytest.fail("Domain settings is messed up... search_features(...) should have raised an exception")
        except CogniteAPIError:
            COGNITE_CLIENT.geospatial.set_current_cognite_domain(cognite_domain)

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

    def test_force_delete_feature_types(self):
        external_id = f"FT_{uuid.uuid4().hex[:10]}"
        feature_type = COGNITE_CLIENT.geospatial.create_feature_types(
            FeatureType(external_id=external_id, attributes={"temperature": {"type": "DOUBLE"}})
        )
        COGNITE_CLIENT.geospatial.create_features(
            feature_type, Feature(external_id=f"F_{uuid.uuid4().hex[:10]}", temperature=12.4)
        )
        COGNITE_CLIENT.geospatial.delete_feature_types(external_id=external_id, force=True)

    def test_search_with_output_selection(self, cognite_domain, test_feature_type, test_feature, another_test_feature):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={}, attributes={"temperature": {}, "volume": {}}
        )
        assert len(res) == 2
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[1], "pressure")

    def test_search_with_output_srid_selection(
        self, cognite_domain, test_feature_type, test_feature, another_test_feature
    ):
        res = COGNITE_CLIENT.geospatial.search_features(
            feature_type=test_feature_type, filter={}, attributes={"position": {"srid": "3857"}}
        )
        assert len(res) == 2
        assert hasattr(res[0], "position")
        assert res[0].position["wkt"] == "POINT(253457.6156334287 6250962.062720415)"
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[0], "volume")

    def test_update_feature_types(self, cognite_domain, test_feature_type):
        res = COGNITE_CLIENT.geospatial.update_feature_types(
            update=FeatureTypeUpdate(
                external_id=test_feature_type.external_id,
                add=AttributeAndSearchSpec(
                    attributes={"altitude": {"type": "DOUBLE", "optional": True}},
                    search_spec={
                        "altitude_idx": {"attributes": ["altitude"]},
                        "pos_alt_idx": {"attributes": ["position", "altitude"]},
                    },
                ),
            ),
        )
        assert len(res) == 1
        assert len(res[0].attributes) == 8
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
