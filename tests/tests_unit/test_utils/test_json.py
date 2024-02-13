import json
from decimal import Decimal

import pytest

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils import _json
from cognite.client.utils._importing import local_import


class TestJsonDumpDefault:
    def test_json_serializable_Decimal(self):
        with pytest.raises(TypeError):
            json.dumps(Decimal(1))

        assert _json.dumps(Decimal(1))

    def test_json_not_serializable_sets(self):
        with pytest.raises(TypeError):
            json.dumps({1, 2})
        with pytest.raises(TypeError):
            json.dumps({1, 2})

    @pytest.mark.dsl
    def test_json_serializable_numpy(self):
        np = local_import("numpy")
        arr = np.array([1.2, 3.4], dtype=np.float32)
        with pytest.raises(TypeError):
            json.dumps(arr)
        with pytest.raises(TypeError):
            json.dumps(arr[0])
        with pytest.raises(TypeError):  # core sdk makes it hard to serialize np.ndarray
            assert _json.dumps(arr)
        assert _json.dumps(arr[0])

    @pytest.mark.dsl
    def test_json_serialiable_numpy_integer(self):
        import numpy as np

        inputs = [np.int32(1), np.int64(1)]
        for input in inputs:
            assert _json.dumps(input)

    @pytest.mark.dsl
    def test_json_dump_cognite_object(self):
        class Obj(CogniteObject):
            def __init__(self, foo: int) -> None:
                self.foo = foo

        with pytest.raises(TypeError):
            json.dumps(Obj(1))
        assert '{"foo": 1}' == _json.dumps(Obj(1))
