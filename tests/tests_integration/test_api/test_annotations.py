from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate, FileMetadata
from cognite.client.data_classes.annotations import AnnotationReverseLookupFilter
from cognite.client.exceptions import CogniteAPIError


def delete_with_check(cognite_client: CogniteClient, delete_ids: list[int], check_ids: list[int] | None = None) -> None:
    if check_ids is None:
        check_ids = delete_ids
    cognite_client.annotations.delete(id=delete_ids)
    try:
        cognite_client.annotations.retrieve_multiple(check_ids)
        raise ValueError(f"retrieve_multiple after delete successful for ids {check_ids}")
    except CogniteAPIError as e:
        assert e.code == 404
        missing = [i["id"] for i in e.failed]
        assert sorted(check_ids) == sorted(missing)


def remove_none_from_nested_dict(d: dict[str, Any]) -> dict[str, Any]:
    new_dict = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = remove_none_from_nested_dict(val)
        if val is not None:
            new_dict[key] = val
    return new_dict


@pytest.fixture
def annotation() -> Annotation:
    return Annotation(
        annotation_type="diagrams.FileLink",
        data={
            "fileRef": {"id": 1, "externalId": None},
            "pageNumber": 1,
            "textRegion": {"xMin": 0.0, "xMax": 0.5, "yMin": 0.5, "yMax": 1.0},
        },
        status="approved",
        creating_app="UnitTest",
        creating_app_version="0.0.1",
        creating_user=None,
        annotated_resource_type="file",
        annotated_resource_id=1,
    )


@pytest.fixture
def file_id(cognite_client: CogniteClient) -> int:
    # Create a test file
    random_id = uuid.uuid4()
    name = f"annotation_unit_test_file_{random_id}"
    file = cognite_client.files.create(FileMetadata(external_id=name, name=name), overwrite=True)[0]
    yield file.id
    # Teardown all annotations to the file
    filter = AnnotationFilter(
        annotated_resource_type="file", annotated_resource_ids=[{"id": file.id}], creating_app="UnitTest"
    )
    annotation_ids = [a.id for a in cognite_client.annotations.list(filter=filter)]
    if annotation_ids:
        delete_with_check(cognite_client, annotation_ids)
    # Teardown the file itself
    cognite_client.files.delete(id=file.id)


@pytest.fixture(scope="session")
def permanent_file_id(cognite_client: CogniteClient) -> int:
    # Create a test file
    external_id = "annotation_unit_test_file_permanent"

    name = "annotation_unit_test_file_permanent.txt"

    file = cognite_client.files.retrieve(external_id=external_id)
    if file is None:
        file = cognite_client.files.create(
            FileMetadata(external_id=external_id, name=name),
            overwrite=True,
        )[0]
        cognite_client.files.upload_bytes(
            content="This is a nice test file", name=name, external_id=external_id, overwrite=True
        )
    # Teardown all annotations to the file
    filter = AnnotationFilter(
        annotated_resource_type="file", annotated_resource_ids=[{"id": file.id}], creating_app="UnitTest"
    )
    annotation_ids = [a.id for a in cognite_client.annotations.list(filter=filter)]
    if annotation_ids:
        delete_with_check(cognite_client, annotation_ids)
    return file.id


@pytest.fixture
def base_annotation(annotation: Annotation, file_id: int) -> Annotation:
    annotation.annotated_resource_id = file_id
    return annotation


@pytest.fixture
def base_suggest_annotation(base_annotation: Annotation) -> Annotation:
    ann = deepcopy(base_annotation)
    ann.status = "suggested"
    return ann


@pytest.fixture(scope="session")
def asset_link_annotation(permanent_file_id: int, cognite_client: CogniteClient) -> Annotation:
    annotation = Annotation(
        annotation_type="diagrams.AssetLink",
        data={
            "pageNumber": 1,
            "assetRef": {"id": 1, "externalId": None},
            "textRegion": {"xMin": 0.5, "xMax": 1.0, "yMin": 0.5, "yMax": 1.0, "confidence": None},
            "text": "AB-CX-DE",
            "symbolRegion": {"xMin": 0.0, "xMax": 0.5, "yMin": 0.5, "yMax": 1.0, "confidence": None},
            "symbol": "pump",
        },
        status="approved",
        creating_app="UnitTest",
        creating_app_version="0.2.3",
        annotated_resource_id=permanent_file_id,
        annotated_resource_type="file",
        creating_user=None,
    )
    return cognite_client.annotations.create(annotation)


def assert_payload_dict(local: dict[str, Any], remote: dict[str, Any]) -> None:
    for k, local_v in local.items():
        if local_v is None:
            continue
        assert k in remote
        remote_v = remote[k]

        if isinstance(local_v, dict):
            assert isinstance(remote_v, dict)
            assert_payload_dict(local_v, remote_v)
        else:
            assert local_v == remote_v


def check_created_vs_base(base_annotation: Annotation, created_annotation: Annotation) -> None:
    base_dump = base_annotation.dump(camel_case=False)
    created_dump = created_annotation.dump(camel_case=False)
    special_keys = ["id", "created_time", "last_updated_time", "data"]
    found_special_keys = 0
    for k, v in created_dump.items():
        if k in special_keys:
            found_special_keys += 1
            assert v is not None
        else:
            assert v == base_dump[k]
    assert found_special_keys == len(special_keys)
    # assert data is equal, except None fields
    created_dump_data = remove_none_from_nested_dict(created_dump["data"])
    base_dump_data = remove_none_from_nested_dict(base_dump["data"])
    assert created_dump_data == base_dump_data


def _test_list_on_created_annotations(
    cognite_client: CogniteClient, annotations: AnnotationList, limit: int = DEFAULT_LIMIT_READ
):
    annotation = annotations[0]
    filter = AnnotationFilter(
        annotated_resource_type=annotation.annotated_resource_type,
        annotated_resource_ids=[{"id": annotation.annotated_resource_id}],
        status=annotation.status,
        creating_app=annotation.creating_app,
        creating_app_version=annotation.creating_app_version,
        creating_user=annotation.creating_user,
    )
    annotations_list = cognite_client.annotations.list(filter=filter, limit=limit)
    assert isinstance(annotations_list, AnnotationList)
    if limit == -1 or limit > len(annotations):
        assert len(annotations_list) == len(annotations)
    else:
        assert len(annotations_list) == limit

    for a in annotations_list:
        check_created_vs_base(annotation, a)


class TestAnnotationsIntegration:
    def test_create_single_annotation(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotation = cognite_client.annotations.create(base_annotation)
        assert isinstance(created_annotation, Annotation)
        check_created_vs_base(base_annotation, created_annotation)
        assert created_annotation.creating_user is None

    def test_create_single_annotation2(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        base_annotation.status = "rejected"
        base_annotation.creating_user = "unit.test@cognite.com"
        created_annotation = cognite_client.annotations.create(base_annotation)
        assert isinstance(created_annotation, Annotation)
        check_created_vs_base(base_annotation, created_annotation)
        assert created_annotation.creating_user == "unit.test@cognite.com"

    def test_create_annotations(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotations = cognite_client.annotations.create([base_annotation] * 30)
        assert isinstance(created_annotations, AnnotationList)
        for a in created_annotations:
            check_created_vs_base(base_annotation, a)

    def test_suggest_single_annotation(
        self, cognite_client: CogniteClient, base_suggest_annotation: Annotation
    ) -> None:
        suggested_annotation = cognite_client.annotations.suggest(base_suggest_annotation)
        assert isinstance(suggested_annotation, Annotation)
        check_created_vs_base(base_suggest_annotation, suggested_annotation)
        assert suggested_annotation.creating_user is None

    def test_suggest_annotations(self, cognite_client: CogniteClient, base_suggest_annotation: Annotation) -> None:
        suggested_annotations = cognite_client.annotations.suggest([base_suggest_annotation] * 30)
        assert isinstance(suggested_annotations, AnnotationList)
        for a in suggested_annotations:
            check_created_vs_base(base_suggest_annotation, a)

    def test_invalid_suggest_annotations(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        with pytest.raises(ValueError, match="status field for Annotation suggestions must be set to 'suggested'"):
            _ = cognite_client.annotations.suggest([base_annotation] * 30)

    def test_delete_annotations(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotations = cognite_client.annotations.create([base_annotation] * 30)
        delete_with_check(cognite_client, [a.id for a in created_annotations])

    def test_update_annotation_by_annotation(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        # Create annotation, make some local changes and cache a dump
        annotation = cognite_client.annotations.create(base_annotation)
        local_dump = annotation.dump(camel_case=False)
        # Update the annotation on remote and make a dump
        annotation = cognite_client.annotations.update(annotation)
        assert isinstance(annotation, Annotation)
        # Check that the local dump matches the remove dump
        remote_dump = annotation.dump(camel_case=False)
        for k, v in remote_dump.items():
            if k == "last_updated_time":
                assert v > local_dump[k]
            elif k == "data":
                assert_payload_dict(local_dump["data"], remote_dump["data"])
            else:
                assert v == local_dump[k]

    def test_update_annotation_by_annotation_update(
        self, cognite_client: CogniteClient, base_annotation: Annotation
    ) -> None:
        update = {
            "data": {
                "pageNumber": 1,
                "assetRef": {"id": 1, "externalId": None},
                "textRegion": {"xMin": 0.5, "xMax": 1.0, "yMin": 0.5, "yMax": 1.0, "confidence": None},
                "text": "AB-CX-DE",
                "symbolRegion": {"xMin": 0.0, "xMax": 0.5, "yMin": 0.5, "yMax": 1.0, "confidence": None},
                "symbol": "pump",
            },
            "status": "rejected",
            "annotation_type": "diagrams.AssetLink",
        }
        created_annotation = cognite_client.annotations.create(base_annotation)

        annotation_update = AnnotationUpdate(id=created_annotation.id)
        for k, v in update.items():
            getattr(annotation_update, k).set(v)

        updated = cognite_client.annotations.update([annotation_update])
        assert isinstance(updated, AnnotationList)
        updated = updated[0]
        for k, v in update.items():
            if k == "data":
                assert_payload_dict(update["data"], updated.data)
            else:
                assert getattr(updated, k) == v

    def test_list(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotations_1 = cognite_client.annotations.create([base_annotation] * 30)
        base_annotation.status = "rejected"
        created_annotations_2 = cognite_client.annotations.create([base_annotation] * 30)
        _test_list_on_created_annotations(cognite_client, created_annotations_1, limit=-1)
        _test_list_on_created_annotations(cognite_client, created_annotations_2, limit=-1)

    def test_list_with_data_filter(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        base_annotation.annotation_type = "images.Classification"
        base_annotation.data = {"label": "test_0"}
        cognite_client.annotations.create(base_annotation)
        base_annotation.data = {"label": "test_1"}
        created_annotation_1 = cognite_client.annotations.create(base_annotation)

        filtered_annotations = cognite_client.annotations.list(
            filter=AnnotationFilter(
                annotated_resource_type="file",
                annotated_resource_ids=[{"id": base_annotation.annotated_resource_id}],
                data={"label": "test_1"},
            )
        )
        assert isinstance(filtered_annotations, AnnotationList)
        assert len(filtered_annotations) == 1
        assert created_annotation_1.dump() == filtered_annotations[0].dump()

    def test_list_limit(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotations = cognite_client.annotations.create([base_annotation] * 30)
        _test_list_on_created_annotations(cognite_client, created_annotations, limit=5)
        _test_list_on_created_annotations(cognite_client, created_annotations)
        _test_list_on_created_annotations(cognite_client, created_annotations, limit=30)
        _test_list_on_created_annotations(cognite_client, created_annotations, limit=-1)

    def test_retrieve(self, cognite_client: CogniteClient, base_annotation: Annotation) -> None:
        created_annotation = cognite_client.annotations.create(base_annotation)
        retrieved_annotation = cognite_client.annotations.retrieve(created_annotation.id)
        assert isinstance(retrieved_annotation, Annotation)
        assert created_annotation.dump() == retrieved_annotation.dump()

    def test_retrieve_multiple(self, cognite_client: CogniteClient, base_annotation: AnnotationList) -> None:
        created_annotations = cognite_client.annotations.create([base_annotation] * 30)
        ids = [c.id for c in created_annotations]
        retrieved_annotations = cognite_client.annotations.retrieve_multiple(ids)
        assert isinstance(retrieved_annotations, AnnotationList)

        # TODO assert the order and do without sorting
        # as soon as the API is fixed
        for ret, new in zip(
            sorted(retrieved_annotations, key=lambda a: a.id), sorted(created_annotations, key=lambda a: a.id)
        ):
            assert ret.dump() == new.dump()

    def test_annotations_reverse_lookup(
        self, asset_link_annotation: Annotation, cognite_client: CogniteClient, permanent_file_id: int
    ) -> None:
        result = cognite_client.annotations.reverse_lookup(
            filter=AnnotationReverseLookupFilter(
                data={"assetRef": {"id": asset_link_annotation.data["assetRef"]["id"]}},
                annotated_resource_type="file",
                creating_user=None,
                annotation_type="diagrams.AssetLink",
                status="approved",
            )
        )
        assert result[0].id == permanent_file_id
