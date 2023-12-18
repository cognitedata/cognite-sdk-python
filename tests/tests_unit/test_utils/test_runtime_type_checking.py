from __future__ import annotations

import re
from typing import Union, overload, List

import pytest

from cognite.client.exceptions import CogniteTypeError
from cognite.client.utils._runtime_type_checking import runtime_type_checked_public_methods


class Foo:
    ...


class TestTypes:
    @runtime_type_checked_public_methods
    class Types:
        def primitive(self, x: int) -> None:
            ...

        def list(self, x: List[str]) -> None:
            ...

        def custom_class(self, x: Foo) -> None:
            ...

    def test_primitive(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.Types.primitive() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            self.Types().primitive("1")

        self.Types().primitive(1)

    def test_list(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.Types.list() parameter x='1' "
                "violates type hint typing.List[str], as str '1' not instance of list."
            ),
        ):
            self.Types().list("1")

        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.Types.list() parameter x=[1] "
                "violates type hint typing.List[str], as list index 0 item int 1 not instance of str."
            ),
        ):
            self.Types().list([1])

        self.Types().list(["ok"])

    def test_custom_type(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.Types.custom_class() "
                "parameter x='1' violates type hint "
                "<class 'tests.tests_unit.test_utils.test_runtime_type_checking.Foo'>, as str '1' not instance "
                'of <class "tests.tests_unit.test_utils.test_runtime_type_checking.Foo">'
            ),
        ):
            self.Types().custom_class("1")

        self.Types().custom_class(Foo())


class TestOverloads:
    @runtime_type_checked_public_methods
    class WithOverload:
        @overload
        def foo(self, x: int, y: int) -> str:
            ...

        @overload
        def foo(self, x: str, y: str) -> str:
            ...

        def foo(self, x: Union[int, str], y: Union[int, str]) -> str:
            return f"{x}{y}"

    def test_overloads(
        self,
    ) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestOverloads.WithOverload.foo() "
                "parameter y=1.0 violates type hint typing.Union[int, str], as float 1.0 not int or str."
            ),
        ):
            self.WithOverload().foo(1, 1.0)

        # Technically should raise a CogniteTypeError, but beartype isn't very good with overloads yet
        self.WithOverload().foo(1, "1")
