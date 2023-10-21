from __future__ import annotations

import json

import pytest

from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationUpdate
from cognite.client.utils._auxiliary import to_snake_case


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
def annotation_filter() -> AnnotationFilter:
    return AnnotationFilter(
        annotated_resource_type="file",
        annotated_resource_ids=[{"id": 1234}, {"id": 4567}],
        annotation_type="diagrams.FileLink",
        status="approved",
        creating_app="UnitTest",
        creating_user="",
        creating_app_version="0.0.1",
    )


class TestAnnotation:
    @pytest.mark.parametrize(
        "creating_user, camel_case",
        [("john.doe@cognite.com", False), ("john.doe@cognite.com", True), (None, False), (None, True)],
        ids=["snake_case", "camel_case", "snake_case_None", "camel_case_None"],
    )
    def test_dump(self, annotation: Annotation, creating_user: str | None, camel_case: bool) -> None:
        annotation.creating_user = creating_user
        super_dump = super(Annotation, annotation).dump(camel_case=camel_case)
        dump = annotation.dump(camel_case=camel_case)
        key = "creatingUser" if camel_case else "creating_user"
        for k, v in dump.items():
            if k == key:
                assert v == creating_user
            else:
                # No key except creating_user can be None
                assert v is not None
                # Must match the super_dump for all other fields
                assert v == super_dump[k]

    def test_load(self, annotation: Annotation) -> None:
        resource = json.dumps(annotation.dump(camel_case=True))
        loaded_annotation = Annotation.load(resource, cognite_client=None)
        assert annotation == loaded_annotation


class TestAnnotationFilter:
    @pytest.mark.parametrize(
        "creating_user, camel_case",
        [
            ("john.doe@cognite.com", False),
            ("john.doe@cognite.com", True),
            (None, False),
            (None, True),
            ("", False),
            ("", True),
        ],
        ids=["snake_case", "camel_case", "snake_case_None", "camel_case_None", "snake_case_empty", "camel_case_empty"],
    )
    def test_dump(self, annotation_filter: AnnotationFilter, creating_user: str | None, camel_case: bool) -> None:
        annotation_filter.creating_user = creating_user
        super_dump = super(AnnotationFilter, annotation_filter).dump(camel_case=camel_case)
        dump = annotation_filter.dump(camel_case=camel_case)
        key = "creatingUser" if camel_case else "creating_user"
        for k, v in dump.items():
            if k == key:
                assert v == creating_user
            else:
                # No key except creating_user can be None
                assert v is not None
                # Must match the super_dump for all other fields
                assert v == super_dump[k]


class TestAnnotationUpdate:
    def test_set_chain(self):
        update = {
            "data": {"assetRef": {"id": 1}, "textRegion": {"xMin": 0.0, "xMax": 0.5, "yMin": 0.5, "yMax": 1.0}},
            "status": "rejected",
            "annotationType": "diagrams.AssetLink",
        }
        annotation_update = AnnotationUpdate(id=1)
        for k, v in update.items():
            snake_case_key = to_snake_case(k)
            getattr(annotation_update, snake_case_key).set(v)
            if v is None:
                update[k] = {"setNull": True}
            else:
                update[k] = {"set": v}

        annotation_update_dump = annotation_update.dump()
        assert annotation_update_dump["id"] == 1
        assert annotation_update_dump["update"] == update
