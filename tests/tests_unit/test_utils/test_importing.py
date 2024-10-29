import pytest
import yaml as normal_yaml

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._importing import import_yaml_with_converters, local_import


class TestLocalImport:
    @pytest.mark.dsl
    def test_local_import_single_ok(self):
        import pandas

        assert pandas == local_import("pandas")

    @pytest.mark.dsl
    def test_local_import_multiple_ok(self):
        import numpy
        import pandas

        assert (pandas, numpy) == local_import("pandas", "numpy")

    def test_local_import_single_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            local_import("not-a-module")

    @pytest.mark.dsl
    def test_local_import_multiple_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            local_import("pandas", "not-a-module")

    @pytest.mark.coredeps
    def test_dsl_deps_not_installed(self):
        for dep in ["geopandas", "pandas", "shapely", "sympy", "numpy"]:
            with pytest.raises(CogniteImportError, match=dep):
                local_import(dep)

    def test_import_yaml_with_converters(self):
        assert normal_yaml.dump(("foo", "bar")) == "!!python/tuple\n- foo\n- bar\n"

        better_yaml = import_yaml_with_converters()
        assert better_yaml.dump(("foo", "bar")) == "- foo\n- bar\n"
