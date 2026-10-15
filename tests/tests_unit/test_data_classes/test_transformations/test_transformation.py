from __future__ import annotations

import pytest

from cognite.client.data_classes.transformations import (
    Transformation,
    TransformationUpdate,
    TransformationWrite,
)
from tests.utils import FakeCogniteResourceGenerator


@pytest.mark.dsl  # requirement when using FakeCogniteResourceGenerator
@pytest.mark.parametrize("transformation_cls", (Transformation, TransformationWrite))
def test_copy_preserves_all_fields(transformation_cls: type[Transformation] | type[TransformationWrite]) -> None:
    transformation = FakeCogniteResourceGenerator().create_instance(transformation_cls)

    copied = transformation.copy()

    assert copied is not transformation
    assert type(copied) is transformation_cls
    assert copied.dump() == transformation.dump()


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("my-domain", {"set": "my-domain"}, id="set value"),
        pytest.param(None, {"setNull": True}, id="set to None clears the value"),
    ],
)
def test_update_data_domain_external_id(value: str | None, expected: dict[str, object]) -> None:
    update = TransformationUpdate(external_id="my-transformation").data_domain_external_id.set(value)

    assert update.dump()["update"] == {"dataDomainExternalId": expected}
