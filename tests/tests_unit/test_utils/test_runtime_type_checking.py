from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, overload

import pytest

from cognite.client.exceptions import CogniteTypeError
from cognite.client.utils._runtime_type_checking import Settings, runtime_type_checked

Settings.enable_runtime_type_checking = True


class Foo: ...


class TestTypes:
    @runtime_type_checked
    class Types:
        def primitive(self, x: int) -> None: ...

        def list(self, x: list[str]) -> None: ...

        def custom_class(self, x: Foo) -> None: ...

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
                "violates type hint list[str], as str '1' not instance of list."
            ),
        ):
            self.Types().list("1")

        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.Types.list() parameter x=[1] "
                "violates type hint list[str], as list index 0 item int 1 not instance of str."
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

    @runtime_type_checked
    class ClassWithConstructor:
        def __init__(self, x: int, y: str) -> None:
            self.x = x
            self.y = y

    def test_constructor_for_class(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.ClassWithConstructor.__init__() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            self.ClassWithConstructor("1", "1")

    def test_constructor_for_subclass(self) -> None:
        class SubDataClassWithConstructor(self.ClassWithConstructor):
            pass

        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.ClassWithConstructor.__init__() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            SubDataClassWithConstructor("1", "1")

    @runtime_type_checked
    @dataclass
    class DataClassWithConstructor:
        x: int
        y: int

    def test_constructor_for_dataclass(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.DataClassWithConstructor.__init__() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            self.DataClassWithConstructor("1", "1")

    def test_constructor_for_dataclass_subclass(self) -> None:
        class SubDataClassWithConstructor(self.DataClassWithConstructor):
            pass

        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypes.DataClassWithConstructor.__init__() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            SubDataClassWithConstructor("1", "1")


class TestOverloads:
    @runtime_type_checked
    class WithOverload:
        @overload
        def foo(self, x: int, y: int) -> str: ...

        @overload
        def foo(self, x: str, y: str) -> str: ...

        def foo(self, x: int | str, y: int | str) -> str:
            return f"{x}{y}"

    def test_overloads(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestOverloads.WithOverload.foo() "
                "parameter y=1.0 violates type hint int | str, as float 1.0 not int or str."
            ),
        ):
            self.WithOverload().foo(1, 1.0)

        # Technically should raise a CogniteTypeError, but beartype isn't very good with overloads yet
        self.WithOverload().foo(1, "1")


class TestTypeCheckSubclasses:
    class Base:
        def __init_subclass__(cls, **kwargs: Any) -> None:
            super().__init_subclass__(**kwargs)
            runtime_type_checked(cls)

    class Sub(Base):
        def foo(self, x: int) -> None: ...

    def test_subclass_is_type_checked(self) -> None:
        with pytest.raises(
            CogniteTypeError,
            match=re.escape(
                "Method tests.tests_unit.test_utils.test_runtime_type_checking.TestTypeCheckSubclasses.Sub.foo() "
                "parameter x='1' violates type hint <class 'int'>, as str '1' not instance of int."
            ),
        ):
            self.Sub().foo("1")

        self.Sub().foo(1)
