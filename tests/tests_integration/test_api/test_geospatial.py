import math
import random
import re
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetWrite, FeatureTypeWrite, FeatureWrite
from cognite.client.data_classes.geospatial import (
    CoordinateReferenceSystem,
    Feature,
    FeatureList,
    FeatureType,
    FeatureTypePatch,
    GeospatialGeometryTransformComputeFunction,
    GeospatialGeometryValueComputeFunction,
    OrderSpec,
    Patches,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._importing import local_import
from tests.utils import get_or_raise, set_request_limit

FIXED_SRID = 121111 + random.randint(0, 1_000)

GEOSPATIAL_TEST_RESOURCES = Path(__file__).parent / "geospatial_test_resources"


@pytest.fixture(scope="session")
def test_crs(cognite_client: CogniteClient) -> Iterator[CoordinateReferenceSystem]:
    wkt = """GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,
    AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],
    PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,
    AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]"""
    proj_string = """+proj=longlat +a=6377276.345 +b=6356075.41314024 +no_defs"""

    # The request often fails with a 409 since we run this test suite with a high degree of parallelism.
    crs = Retrying(
        retry=retry_if_exception(lambda exc: isinstance(exc, CogniteAPIError) and exc.code == 409),
        wait=wait_exponential_jitter(),
        stop=stop_after_attempt(3),
        reraise=True,
    )(
        cognite_client.geospatial.create_coordinate_reference_systems,
        crs=CoordinateReferenceSystem(srid=FIXED_SRID, wkt=wkt, proj_string=proj_string),
    )
    yield crs[0]
    cognite_client.geospatial.delete_coordinate_reference_systems(srids=[FIXED_SRID])


@pytest.fixture(params=[True, False])
def allow_crs_transformation(request: FixtureRequest) -> Iterator[bool]:
    yield request.param


@pytest.fixture(scope="session", autouse=True)
def cleanup_old_feature_types(cognite_client: CogniteClient) -> None:
    res = cognite_client.geospatial.list_feature_types()
    old_feature_types = [ft.external_id for ft in res if ((time.time() * 1000 - ft.created_time) / 1000 / 60 / 60) > 1]
    for i in range(0, len(old_feature_types), 10):
        try:
            cognite_client.geospatial.delete_feature_types(old_feature_types[i : i + 10], recursive=True)
        except CogniteNotFoundError:
            ...


def _new_feature_type(cognite_client: CogniteClient) -> Iterator[FeatureType]:
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = cognite_client.geospatial.create_feature_types(
        FeatureTypeWrite(
            external_id=external_id,
            properties={
                "position": {"type": "GEOMETRY", "srid": "4326", "optional": "true"},
                "volume": {"type": "DOUBLE"},
                "temperature": {"type": "DOUBLE"},
                "pressure": {"type": "DOUBLE"},
                "raster": {"srid": 3857, "type": "RASTER", "storage": "embedded", "optional": True},
            },
            search_spec={"vol_press_idx": {"properties": ["volume", "pressure"]}},
        )
    )
    yield feature_type
    cognite_client.geospatial.delete_feature_types(external_id=external_id, recursive=True)


@pytest.fixture(scope="session")
def test_feature_type(cognite_client: CogniteClient) -> Iterator[FeatureType]:
    yield from _new_feature_type(cognite_client)


@pytest.fixture
def test_feature_type_test_scoped(cognite_client: CogniteClient) -> Iterator[FeatureType]:
    yield from _new_feature_type(cognite_client)


@pytest.fixture(scope="session")
def large_feature_type(cognite_client: CogniteClient) -> Iterator[FeatureType]:
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type_spec = FeatureTypeWrite(
        external_id=external_id, properties={f"attr{i}": {"type": "LONG"} for i in range(80)}
    )
    yield cognite_client.geospatial.create_feature_types(feature_type_spec)
    cognite_client.geospatial.delete_feature_types(external_id=external_id)


@pytest.fixture(scope="session")
def another_test_feature_type(cognite_client: CogniteClient) -> Iterator[FeatureType]:
    external_id = f"FT_{uuid.uuid4().hex[:10]}"
    feature_type = cognite_client.geospatial.create_feature_types(
        FeatureTypeWrite(external_id=external_id, properties={"volume": {"type": "DOUBLE"}})
    )
    yield feature_type
    cognite_client.geospatial.delete_feature_types(external_id=external_id)


def _new_feature(cognite_client: CogniteClient, test_feature_type: FeatureType) -> Iterator[Feature]:
    external_id = f"F_{uuid.uuid4().hex[:10]}"
    feature = cognite_client.geospatial.create_features(
        test_feature_type.external_id,
        FeatureWrite(
            external_id=external_id,
            position={"wkt": "POINT(2.2768485 48.8589506)"},
            temperature=12.4,
            volume=1212.0,
            pressure=2121.0,
        ),
    )
    yield feature
    cognite_client.geospatial.delete_features(test_feature_type.external_id, external_id=external_id)


@pytest.fixture(scope="session")
def test_feature(cognite_client: CogniteClient, test_feature_type: FeatureType) -> Iterator[Feature]:
    yield from _new_feature(cognite_client, test_feature_type)


@pytest.fixture
def test_feature_test_scoped(
    cognite_client: CogniteClient, test_feature_type_test_scoped: FeatureType
) -> Iterator[Feature]:
    yield from _new_feature(cognite_client, test_feature_type_test_scoped)


@pytest.fixture
def test_feature_with_raster(
    cognite_client: CogniteClient, test_feature_type_test_scoped: FeatureType, test_feature_test_scoped: Feature
) -> Feature:
    cognite_client.geospatial.put_raster(
        feature_type_external_id=test_feature_type_test_scoped.external_id,
        feature_external_id=get_or_raise(test_feature_test_scoped.external_id),
        raster_property_name="raster",
        raster_format="XYZ",
        raster_srid=3857,
        file=GEOSPATIAL_TEST_RESOURCES / "raster-grid-example.xyz",
    )
    return test_feature_test_scoped


def _new_feature_list(cognite_client: CogniteClient, test_feature_type: FeatureType) -> Iterator[FeatureList]:
    external_ids = [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(4)]
    features = [
        FeatureWrite(
            external_id=external_ids[0],
            position={"wkt": "POINT(2.2768485 48.8589506)"},
            temperature=12.4,
            volume=1212.0,
            pressure=2121.0,
        ),
        FeatureWrite(
            external_id=external_ids[1],
            position={"wkt": "POLYGON((10.689 -25.092, 38.814 -35.639, 13.502 -39.155, 10.689 -25.092))"},
            temperature=13.4,
            volume=1212.0,
            pressure=2121.0,
        ),
        FeatureWrite(
            external_id=external_ids[2],
            position={"wkt": "LINESTRING (30 10, 10 30, 40 40)"},
            temperature=3.4,
            volume=1212.0,
            pressure=2121.0,
        ),
        FeatureWrite(
            external_id=external_ids[3],
            position={"wkt": "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"},
            temperature=23.4,
            volume=1212.0,
            pressure=2121.0,
        ),
    ]
    feature = cognite_client.geospatial.create_features(
        feature_type_external_id=test_feature_type.external_id, feature=features, chunk_size=2
    )
    yield feature
    cognite_client.geospatial.delete_features(
        feature_type_external_id=test_feature_type.external_id, external_id=external_ids
    )


@pytest.fixture(scope="session")
def test_feature_list(cognite_client: CogniteClient, test_feature_type: FeatureType) -> Iterator[FeatureList]:
    yield from _new_feature_list(cognite_client, test_feature_type)


@pytest.fixture
def test_feature_list_test_scoped(
    cognite_client: CogniteClient, test_feature_type_test_scoped: FeatureType
) -> Iterator[FeatureList]:
    yield from _new_feature_list(cognite_client, test_feature_type_test_scoped)


@pytest.fixture
def another_test_feature(cognite_client: CogniteClient, test_feature_type: FeatureType) -> Iterator[Feature]:
    external_id = f"F_{uuid.uuid4().hex[:10]}"
    feature = cognite_client.geospatial.create_features(
        test_feature_type.external_id,
        FeatureWrite(external_id=external_id, temperature=-10.8, pressure=123.456, volume=654.2),
    )
    yield feature
    cognite_client.geospatial.delete_features(test_feature_type.external_id, external_id=external_id)


@pytest.fixture
def many_features(cognite_client: CogniteClient, large_feature_type: FeatureType) -> Iterator[FeatureList]:
    specs = [
        FeatureWrite(
            external_id=f"F_{uuid.uuid4().hex[:10]}", **{f"attr{i}": random.randint(10000, 20000) for i in range(80)}
        )
        for _ in range(2000)
    ]
    features = cognite_client.geospatial.create_features(large_feature_type.external_id, specs)
    yield features
    external_ids = [get_or_raise(f.external_id) for f in features]
    cognite_client.geospatial.delete_features(get_or_raise(large_feature_type.external_id), external_id=external_ids)


# we clean up the old custom CRS from a previous failed run
@pytest.fixture(autouse=True, scope="module")
def clean_old_custom_crs(cognite_client: CogniteClient) -> None:
    try:
        cognite_client.geospatial.delete_coordinate_reference_systems(srids=[121111])  # clean up
    except Exception:
        pass
    try:
        cognite_client.geospatial.delete_coordinate_reference_systems(srids=[FIXED_SRID])  # clean up
    except Exception:
        pass


@pytest.fixture(autouse=True, scope="session")
def new_asset(cognite_client: CogniteClient) -> Iterator[Asset]:
    asset = cognite_client.assets.create(AssetWrite(name="any", description="haha", metadata={"a": "b"}))
    yield asset
    cognite_client.assets.delete(id=asset.id)


class TestGeospatialAPI:
    def test_create_features(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        allow_crs_transformation: bool,
        new_asset: Asset,
    ) -> None:
        external_id = f"F_{uuid.uuid4().hex[:10]}"
        cognite_client.geospatial.create_features(
            test_feature_type_test_scoped.external_id,
            FeatureWrite(
                external_id=external_id,
                position={"wkt": "POINT(50 50)"},
                temperature=12.4,
                volume=1212.0,
                pressure=2121.0,
            ),
            allow_crs_transformation=allow_crs_transformation,
        )
        cognite_client.geospatial.delete_features(test_feature_type_test_scoped.external_id, external_id=external_id)

    def test_retrieve_single_feature_type_by_external_id(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType
    ) -> None:
        assert (
            test_feature_type.external_id
            == cognite_client.geospatial.retrieve_feature_types(external_id=test_feature_type.external_id).external_id
        )

    def test_list_feature_types(self, cognite_client: CogniteClient, test_feature_type: FeatureType) -> None:
        res = cognite_client.geospatial.list_feature_types()
        assert 0 < len(res) < 100

    def test_retrieve_single_feature_by_external_id(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        res = cognite_client.geospatial.retrieve_features(
            feature_type_external_id=get_or_raise(test_feature_type.external_id),
            external_id=get_or_raise(test_feature.external_id),
        )
        assert res.external_id == test_feature.external_id

    def test_update_single_feature(
        self,
        cognite_client: CogniteClient,
        allow_crs_transformation: bool,
        test_feature_type_test_scoped: FeatureType,
        test_feature_test_scoped: Feature,
        new_asset: Asset,
    ) -> None:
        res = cognite_client.geospatial.update_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature=FeatureWrite(
                external_id=get_or_raise(test_feature_test_scoped.external_id),
                temperature=6.237,
                pressure=12.21,
                volume=34.43,
                # asset_ids=[new_asset.id],
            ),
            allow_crs_transformation=allow_crs_transformation,
        )
        assert res.external_id == test_feature_test_scoped.external_id
        assert res.temperature == 6.237  # type: ignore[attr-defined]
        # assert res.asset_ids == [new_asset.id]

    @pytest.mark.skip("flaky")
    def test_update_multiple_features(
        self,
        cognite_client: CogniteClient,
        allow_crs_transformation: bool,
        test_feature_type_test_scoped: FeatureType,
        test_feature_list_test_scoped: FeatureList,
    ) -> None:
        results = cognite_client.geospatial.update_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature=[
                Feature(
                    external_id=test_feat.external_id,
                    created_time=123,
                    last_updated_time=123,
                    data_set_id=None,
                    temperature=6.237,
                    pressure=12.21,
                    volume=34.43,
                )
                for test_feat in test_feature_list_test_scoped
            ],
            allow_crs_transformation=allow_crs_transformation,
            chunk_size=2,
        )
        for res, test_feat in zip(results, test_feature_list_test_scoped):
            assert res.external_id == test_feat.external_id
            assert res.temperature == 6.237

    def test_search_single_feature(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"range": {"property": "temperature", "gt": 12.0}},
            limit=10,
        )
        assert res[0].external_id == test_feature.external_id
        assert res[0].temperature == 12.4  # type: ignore[attr-defined]
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"range": {"property": "temperature", "lt": 12.0}},
            limit=10,
        )
        assert len(res) == 0

    def test_search_feature_dimensionality_mismatch(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        polygon_z = "POLYGONZ((2.276 48.858 3,2.278 48.859 3,2.2759 48.859 3,2.276 48.858 3))"
        polygon = "POLYGON((2.276 48.858,2.278 48.859,2.275 48.859,2.276 48.858))"
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"stWithin": {"property": "position", "value": {"wkt": polygon}}},
            limit=10,
        )
        assert res[0].external_id == test_feature.external_id

        with pytest.raises(CogniteAPIError):
            res = cognite_client.geospatial.search_features(
                feature_type_external_id=test_feature_type.external_id,
                filter={"stWithin": {"property": "position", "value": {"wkt": polygon_z}}},
                limit=10,
            )

    def test_search_feature_dimensionality_mismatch_flag_set(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        polygon_z = "POLYGONZ((2.276 48.858 3,2.278 48.859 3,2.2759 48.859 3,2.276 48.858 3))"
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"stWithin": {"property": "position", "value": {"wkt": polygon_z}}},
            limit=10,
            allow_dimensionality_mismatch=True,
        )
        assert len(res) == 1
        assert res[0].external_id == test_feature.external_id

    def test_retrieve_multiple_feature_types_by_external_id(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, another_test_feature_type: FeatureType
    ) -> None:
        res = cognite_client.geospatial.retrieve_feature_types(
            external_id=[test_feature_type.external_id, another_test_feature_type.external_id]
        )
        assert len(res) == 2

    def test_retrieve_multiple_features_by_external_id(
        self,
        cognite_client: CogniteClient,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.retrieve_features(
            feature_type_external_id=test_feature_type.external_id,
            external_id=[get_or_raise(test_feature.external_id), get_or_raise(another_test_feature.external_id)],
        )
        assert res[0].external_id == test_feature.external_id
        assert res[1].external_id == another_test_feature.external_id

    def test_search_multiple_features(
        self,
        cognite_client: CogniteClient,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"range": {"property": "temperature", "gt": -20.0, "lt": 20.0}},
        )
        assert len(res) == 2
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"range": {"property": "temperature", "gt": 0.0, "lt": 20.0}},
        )
        assert len(res) == 1
        assert res[0].external_id == test_feature.external_id

    def test_search_wrong_crs(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        with pytest.raises(CogniteAPIError):
            cognite_client.geospatial.search_features(
                feature_type_external_id=test_feature_type.external_id,
                filter={"stWithin": {"property": "location", "value": {"wkt": "", "srid": 3857}}},
                limit=10,
            )

    def test_get_coordinate_reference_system(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.geospatial.get_coordinate_reference_systems(srids=4326)
        assert res[0].srid == 4326

    def test_get_multiple_coordinate_reference_systems(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        assert set(map(lambda x: x.srid, res)) == {4326, 4327}

    def test_list_coordinate_reference_systems(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.geospatial.list_coordinate_reference_systems()
        all = res
        assert len(all) > 8000
        res = cognite_client.geospatial.list_coordinate_reference_systems(only_custom=True)
        assert len(res) < len(all)

    def test_list_custom_coordinate_reference_systems(
        self, cognite_client: CogniteClient, test_crs: CoordinateReferenceSystem
    ) -> None:
        res = cognite_client.geospatial.list_coordinate_reference_systems(only_custom=True)
        assert test_crs.srid in set(map(lambda x: x.srid, res))

    def test_recursive_delete_feature_types(self, cognite_client: CogniteClient) -> None:
        external_id = f"FT_{uuid.uuid4().hex[:10]}"
        feature_type = cognite_client.geospatial.create_feature_types(
            FeatureTypeWrite(external_id=external_id, properties={"temperature": {"type": "DOUBLE"}})
        )
        cognite_client.geospatial.create_features(
            feature_type.external_id, FeatureWrite(external_id=f"F_{uuid.uuid4().hex[:10]}", temperature=12.4)
        )
        cognite_client.geospatial.delete_feature_types(external_id=external_id, recursive=True)

    def test_get_features_by_ids_with_output_selection(
        self,
        cognite_client: CogniteClient,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.retrieve_features(
            feature_type_external_id=test_feature_type.external_id,
            external_id=[get_or_raise(test_feature.external_id), get_or_raise(another_test_feature.external_id)],
            properties={"temperature": {}, "volume": {}},
        )
        assert len(res) == 2
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[1], "pressure")

    def test_search_with_output_selection(
        self,
        cognite_client: CogniteClient,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            properties={"temperature": {}, "volume": {}},
        )
        assert len(res) == 2
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[1], "pressure")

    def test_search_with_output_srid_selection(
        self,
        cognite_client: CogniteClient,
        allow_crs_transformation: bool,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            properties={"position": {"srid": "3857"}},
            allow_crs_transformation=allow_crs_transformation,
        )
        assert len(res) == 2
        assert hasattr(res[0], "position")
        assert re.compile(r"^POINT\(253457.61[0-9]+ 6250962.06[0-9]+\)$").match(res[0].position["wkt"])
        assert not hasattr(res[0], "pressure")
        assert not hasattr(res[0], "volume")

    def test_search_with_order_by(
        self,
        cognite_client: CogniteClient,
        test_feature_type: FeatureType,
        test_feature: Feature,
        another_test_feature: Feature,
    ) -> None:
        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            order_by=[OrderSpec(property="temperature", direction="ASC")],
        )
        assert res[0].temperature == -10.8  # type: ignore[attr-defined]
        assert res[1].temperature == 12.4  # type: ignore[attr-defined]

        res = cognite_client.geospatial.search_features(
            feature_type_external_id=test_feature_type.external_id,
            order_by=[OrderSpec(property="temperature", direction="DESC")],
        )
        assert res[0].temperature == 12.4  # type: ignore[attr-defined]
        assert res[1].temperature == -10.8  # type: ignore[attr-defined]

    def test_patch_feature_types(
        self, cognite_client: CogniteClient, test_feature_type_test_scoped: FeatureType
    ) -> None:
        res = cognite_client.geospatial.patch_feature_types(
            patch=FeatureTypePatch(
                external_id=test_feature_type_test_scoped.external_id,
                property_patches=Patches(add={"altitude": {"type": "DOUBLE", "optional": True}}, remove=["volume"]),
                search_spec_patches=Patches(
                    add={
                        "altitude_idx": {"properties": ["altitude"]},
                        "pos_alt_idx": {"properties": ["position", "altitude"]},
                    },
                    remove=["vol_press_idx"],
                ),
            )
        )
        assert len(res) == 1
        assert len(res[0].properties or {}) == len(test_feature_type_test_scoped.properties or {})
        assert len(res[0].search_spec or {}) == len(test_feature_type_test_scoped.search_spec or {}) + 1

    def test_stream_features(
        self, cognite_client: CogniteClient, large_feature_type: FeatureType, many_features: FeatureList
    ) -> None:
        features = cognite_client.geospatial.stream_features(
            feature_type_external_id=large_feature_type.external_id,
        )
        feature_list = FeatureList(list(features))
        assert len(feature_list) == len(many_features)

    def test_stream_features_dimensionality_mismatch(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        polygon_z = "POLYGONZ((2.276 48.858 3,2.278 48.859 3,2.2759 48.859 3,2.276 48.858 3))"
        polygon = "POLYGON((2.276 48.858,2.278 48.859,2.275 48.859,2.276 48.858))"
        stream_res = cognite_client.geospatial.stream_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"stWithin": {"property": "position", "value": {"wkt": polygon}}},
        )
        res = [x for x in stream_res]
        assert res[0].external_id == test_feature.external_id

        with pytest.raises(CogniteAPIError):
            stream_res = cognite_client.geospatial.stream_features(
                feature_type_external_id=test_feature_type.external_id,
                filter={"stWithin": {"property": "position", "value": {"wkt": polygon_z}}},
            )
            next(stream_res)

    def test_stream_features_dimensionality_mismatch_flag_set(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        polygon_z = "POLYGONZ((2.276 48.858 3,2.278 48.859 3,2.2759 48.859 3,2.276 48.858 3))"
        stream_res = cognite_client.geospatial.stream_features(
            feature_type_external_id=test_feature_type.external_id,
            filter={"stWithin": {"property": "position", "value": {"wkt": polygon_z}}},
            allow_dimensionality_mismatch=True,
        )
        res = [x for x in stream_res]
        assert len(res) == 1
        assert res[0].external_id == test_feature.external_id

    def test_list(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature_list: FeatureList
    ) -> None:
        with set_request_limit(cognite_client.geospatial, 2):
            res = cognite_client.geospatial.list_features(
                feature_type_external_id=test_feature_type.external_id, properties={"externalId": {}}, limit=4
            )

        assert len(res) == 4
        df = res.to_pandas()
        assert list(df) == ["external_id"]

    def test_to_pandas(self, test_feature_type: FeatureType, test_feature_list: FeatureList) -> None:
        df = test_feature_list.to_pandas(camel_case=True)
        assert list(df) == [
            "externalId",
            "position",
            "volume",
            "temperature",
            "pressure",
            "createdTime",
            "lastUpdatedTime",
        ]

    def test_to_geopandas(self, test_feature_type: FeatureType, test_feature_list: FeatureList) -> None:
        gdf = test_feature_list.to_geopandas(geometry="position", camel_case=True)
        assert list(gdf) == [
            "externalId",
            "position",
            "volume",
            "temperature",
            "pressure",
            "createdTime",
            "lastUpdatedTime",
        ]
        geopandas = local_import("geopandas")
        assert type(gdf.dtypes["position"]) is geopandas.array.GeometryDtype

    def test_from_geopandas_basic(self, cognite_client: CogniteClient, test_feature_type: FeatureType) -> None:
        pd = local_import("pandas")
        df = pd.DataFrame(
            {
                "externalId": [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(4)],
                "position": [
                    "POINT(2.2768485 48.8589506)",
                    "POLYGON((10.689 -25.092, 38.814 -35.639, 13.502 -39.155, 10.689 -25.092))",
                    "LINESTRING (30 10, 10 30, 40 40)",
                    "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
                ],
                "temperature": [12.4, 13.4, 13.4, 13.4],
                "volume": [1212.0, 1313.0, 1414.0, 1515.0],
                "pressure": [2121.0, 2121.0, 2121.0, 2121.0],
            }
        )
        local_import("shapely.wkt")
        geopandas = local_import("geopandas")
        df["position"] = geopandas.GeoSeries.from_wkt(df["position"])
        gdf = geopandas.GeoDataFrame(df, geometry="position")
        fl = FeatureList.from_geopandas(test_feature_type, gdf)
        res = cognite_client.geospatial.create_features(test_feature_type.external_id, fl)
        assert len(res) == 4

    def test_from_geopandas_flexible(self, cognite_client: CogniteClient, test_feature_type: FeatureType) -> None:
        pd = local_import("pandas")
        df = pd.DataFrame(
            {
                "some_unique_id": [f"F{i}_{uuid.uuid4().hex[:10]}" for i in range(4)],
                "some_position": [
                    "POINT(2.2768485 48.8589506)",
                    "POLYGON((10.689 -25.02, 38.814 -35.639, 13.502 -39.155, 10.689 -25.02))",
                    None,
                    "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
                ],
                "some_temperature": [12.4, 13.4, 13.4, 13.4],
                "some_volume": [1212.0, 1313.0, 1414.0, 1515.0],
                "some_pressure": [2121.0, 2121.0, 2121.0, 2121.0],
            }
        )
        local_import("shapely.wkt")
        geopandas = local_import("geopandas")
        df["some_position"] = geopandas.GeoSeries.from_wkt(df["some_position"])
        gdf = geopandas.GeoDataFrame(df, geometry="some_position")
        fl = FeatureList.from_geopandas(
            test_feature_type,
            gdf,
            "some_unique_id",
            {
                "position": "some_position",
                "temperature": "some_temperature",
                "volume": "some_volume",
                "pressure": "some_pressure",
            },
        )
        res = cognite_client.geospatial.create_features(test_feature_type.external_id, fl)
        assert len(res) == 4

    def test_aggregate__temperature_property_min_max(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature_list: FeatureList
    ) -> None:
        res = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type.external_id,
            output={
                "min": {"min": {"property": "temperature"}},
                "max": {"max": {"property": "temperature"}},
            },
        )
        assert res[0].min == 3.4  # type:ignore[attr-defined]
        assert res[0].max == 23.4  # type:ignore[attr-defined]

    def test_aggregate__range_gt_lt(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_list_test_scoped: FeatureList,
    ) -> None:
        res = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            filter={"range": {"property": "temperature", "gt": 12.0, "lt": 13.0}},
            output={"count": {"count": {"property": "temperature"}}},
        )
        assert res[0].count == 1  # type:ignore[attr-defined]

    def test_aggregate__temperature_property_min_max_groupby_xid(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_list_test_scoped: FeatureList,
    ) -> None:
        res = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            output={
                "min": {"min": {"property": "temperature"}},
                "max": {"max": {"property": "temperature"}},
            },
            group_by=["externalId"],
        )
        assert len(res) == 4

    def test_aggregate_with_order_by(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature_list: FeatureList
    ) -> None:
        res = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type.external_id,
            output={"count": {"count": {"property": "temperature"}}},
            group_by=["externalId"],
            order_by=[OrderSpec("externalId", "DESC")],
        )
        external_ids = [item.external_id for item in res]  # type:ignore[attr-defined]
        external_ids.reverse()
        res_asc = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type.external_id,
            output={"count": {"count": {"property": "temperature"}}},
            group_by=["externalId"],
            order_by=[OrderSpec("externalId", "ASC")],
        )
        assert external_ids == [item.external_id for item in res_asc]  # type:ignore[attr-defined]

    def test_aggregate_output(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_list_test_scoped: FeatureList,
    ) -> None:
        res = cognite_client.geospatial.aggregate_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            filter={"range": {"property": "temperature", "gt": 12.0, "lt": 13.0}},
            output={"count": {"count": {"property": "temperature"}}},
        )
        assert res[0].count == 1  # type:ignore[attr-defined]

    def test_put_raster(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        res = cognite_client.geospatial.put_raster(
            feature_type_external_id=test_feature_type.external_id,
            feature_external_id=get_or_raise(test_feature.external_id),
            raster_property_name="raster",
            raster_format="XYZ",
            raster_srid=3857,
            file=GEOSPATIAL_TEST_RESOURCES / "raster-grid-example.xyz",
        )
        assert res.width == 4  # type:ignore[attr-defined]
        assert res.height == 5  # type:ignore[attr-defined]
        assert res.num_bands == 1  # type:ignore[attr-defined]
        assert res.scale_x == 1.0  # type:ignore[attr-defined]
        assert res.scale_y == 1.0  # type:ignore[attr-defined]
        assert res.skew_x == 0.0  # type:ignore[attr-defined]
        assert res.skew_y == 0.0  # type:ignore[attr-defined]
        assert res.srid == 3857  # type:ignore[attr-defined]
        assert res.upper_left_x == -0.5  # type:ignore[attr-defined]
        assert res.upper_left_y == -0.5  # type:ignore[attr-defined]

    def test_get_raster(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_with_raster: Feature,
    ) -> None:
        res = cognite_client.geospatial.get_raster(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature_external_id=get_or_raise(test_feature_with_raster.external_id),
            raster_property_name="raster",
            raster_format="XYZ",
        )
        raster_content = (GEOSPATIAL_TEST_RESOURCES / "raster-grid-example.xyz").read_text()
        assert res.decode(encoding="utf-8") == raster_content

        res = cognite_client.geospatial.get_raster(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature_external_id=get_or_raise(test_feature_with_raster.external_id),
            raster_property_name="raster",
            raster_format="XYZ",
            raster_options={"DECIMAL_PRECISION": 5},
        )
        raster_content = (GEOSPATIAL_TEST_RESOURCES / "raster-grid-5-decimal.xyz").read_text()
        assert res.decode(encoding="utf-8") == raster_content

    def test_get_raster_with_transformation(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_with_raster: Feature,
    ) -> None:
        res = cognite_client.geospatial.get_raster(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature_external_id=get_or_raise(test_feature_with_raster.external_id),
            raster_property_name="raster",
            raster_format="XYZ",
            raster_srid=54030,
            allow_crs_transformation=True,
        )
        raster_content = (GEOSPATIAL_TEST_RESOURCES / "raster-grid-54030-example.xyz").read_text()
        assert res.decode(encoding="utf-8") == raster_content

    def test_retrieve_features_with_raster_property(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_with_raster: Feature,
    ) -> None:
        res = cognite_client.geospatial.retrieve_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            external_id=[get_or_raise(test_feature_with_raster.external_id)],
        )
        assert res[0].external_id == test_feature_with_raster.external_id
        raster_metadata = res[0].raster  # type: ignore[attr-defined]
        assert raster_metadata == {
            "width": 4,
            "height": 5,
            "numBands": 1,
            "scaleX": 1.0,
            "scaleY": 1.0,
            "skewX": 0.0,
            "skewY": 0.0,
            "srid": 3857,
            "upperLeftX": -0.5,
            "upperLeftY": -0.5,
        }

    def test_put_raster_custom_crs(
        self, cognite_client: CogniteClient, test_feature_type: FeatureType, test_feature: Feature
    ) -> None:
        res = cognite_client.geospatial.put_raster(
            feature_type_external_id=test_feature_type.external_id,
            feature_external_id=get_or_raise(test_feature.external_id),
            raster_property_name="raster",
            raster_format="XYZ",
            raster_srid=54030,
            file=GEOSPATIAL_TEST_RESOURCES / "raster-grid-example.xyz",
            allow_crs_transformation=True,
        )
        assert res.width == 4  # type:ignore[attr-defined]
        assert res.height == 5  # type:ignore[attr-defined]
        assert res.num_bands == 1  # type:ignore[attr-defined]
        assert res.scale_x == 1.0  # type:ignore[attr-defined]
        assert res.scale_y == 1.0  # type:ignore[attr-defined]
        assert res.skew_x == 0.0  # type:ignore[attr-defined]
        assert res.skew_y == 0.0  # type:ignore[attr-defined]
        assert res.srid == 3857  # type:ignore[attr-defined]
        assert math.isclose(res.upper_left_x, -0.5891363261459447)  # type:ignore[attr-defined]
        assert math.isclose(res.upper_left_y, -0.316234582133065)  # type:ignore[attr-defined]

    def test_delete_raster(
        self,
        cognite_client: CogniteClient,
        test_feature_type_test_scoped: FeatureType,
        test_feature_with_raster: Feature,
    ) -> None:
        res = cognite_client.geospatial.delete_raster(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            feature_external_id=get_or_raise(test_feature_with_raster.external_id),
            raster_property_name="raster",
        )
        assert res is None
        res = cognite_client.geospatial.retrieve_features(
            feature_type_external_id=test_feature_type_test_scoped.external_id,
            external_id=[get_or_raise(test_feature_with_raster.external_id)],
        )
        assert res[0].external_id == test_feature_with_raster.external_id
        assert hasattr(res[0], "raster") is False

    def test_compute_st_transform_geometry_value(
        self, cognite_client: CogniteClient, test_crs: CoordinateReferenceSystem
    ) -> None:
        compute_st_transform = GeospatialGeometryTransformComputeFunction(
            GeospatialGeometryValueComputeFunction("SRID=4326;POLYGON((0 0,10 0,10 10,0 10,0 0))"), srid=test_crs.srid
        )
        res = cognite_client.geospatial.compute(
            output={
                "output": compute_st_transform,
            }
        )
        items = res.items
        assert len(items) == 1
        output = items[0].output  # type: ignore[attr-defined]
        assert output["srid"] == test_crs.srid
        assert output["wkt"] == "POLYGON((0 0,10 0,10 10,0 10,0 0))"
