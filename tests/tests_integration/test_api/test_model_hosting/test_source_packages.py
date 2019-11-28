import os
import time
from random import randint
from tempfile import TemporaryDirectory

import pytest

from cognite.client.data_classes.model_hosting.source_packages import SourcePackage, SourcePackageList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.experimental import CogniteClient

SOURCE_PACKAGES_API = CogniteClient().model_hosting.source_packages


@pytest.mark.skip("model hosting development")
class TestSourcePackages:
    @pytest.fixture(scope="class")
    def source_package_file_path(self):
        with TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "sp.tar.gz")
            with open(file_path, "w") as f:
                f.write("content")
            yield file_path

    @pytest.fixture(scope="class")
    def created_source_package(self, source_package_file_path):
        sp_name = "test-sp-{}".format(randint(0, 1e5))
        sp = SOURCE_PACKAGES_API.upload_source_package(
            name=sp_name,
            package_name="whatever",
            available_operations=["TRAIN", "PREDICT"],
            runtime_version="0.1",
            file_path=source_package_file_path,
        )
        assert sp.upload_url is None
        yield sp
        SOURCE_PACKAGES_API.delete_source_package(id=sp.id)

    @pytest.fixture(scope="class")
    def source_package_directory(self):
        yield os.path.join(os.path.dirname(__file__), "source_package_for_tests")

    def test_build_and_create_source_package(self, source_package_directory):
        sp_name = "test-sp-{}".format(randint(0, 1e5))
        sp = SOURCE_PACKAGES_API.build_and_upload_source_package(
            name=sp_name, runtime_version="0.1", package_directory=source_package_directory
        )
        assert sp.upload_url is None

        sp = SOURCE_PACKAGES_API.get_source_package(sp.id)
        assert ["TRAIN", "PREDICT"] == sp.available_operations
        assert "my_model" == sp.package_name
        SOURCE_PACKAGES_API.delete_source_package(id=sp.id)

    def test_list_source_packages(self, created_source_package):
        res = SOURCE_PACKAGES_API.list_source_packages()
        assert len(res) > 0
        assert isinstance(res, SourcePackageList)
        assert isinstance(res[:1], SourcePackageList)
        assert isinstance(res[0], SourcePackage)
        for sp in res:
            assert isinstance(sp, SourcePackage)

    def test_get_source_package(self, created_source_package):
        for i in range(5):
            sp = SOURCE_PACKAGES_API.get_source_package(created_source_package.id)
            if sp.is_uploaded:
                break
            time.sleep(1)
        assert isinstance(sp, SourcePackage)
        assert sp.id == created_source_package.id
        assert sp.is_uploaded is True

    def test_deprecate_source_package(self, created_source_package):
        sp = SOURCE_PACKAGES_API.deprecate_source_package(created_source_package.id)
        assert sp.is_deprecated is True
        sp = SOURCE_PACKAGES_API.get_source_package(created_source_package.id)
        assert sp.is_deprecated is True

    def test_download_code(self, created_source_package):
        with TemporaryDirectory() as tmp_dir:
            SOURCE_PACKAGES_API.download_source_package_code(id=created_source_package.id, directory=tmp_dir)
            sp_name = SOURCE_PACKAGES_API.get_source_package(id=created_source_package.id).name
            file_path = os.path.join(tmp_dir, sp_name + ".tar.gz")
            assert os.path.isfile(file_path)
            with open(file_path) as f:
                assert "content" == f.read()

    @pytest.mark.skip(reason="Bug in model hosting API causes this to return 500")
    def test_delete_code(self, created_source_package):
        SOURCE_PACKAGES_API.delete_source_package_code(id=created_source_package.id)
        with pytest.raises(CogniteAPIError, match="deleted"):
            SOURCE_PACKAGES_API.download_source_package_code(id=created_source_package.id)
