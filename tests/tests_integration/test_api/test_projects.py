from __future__ import annotations

import pytest

from cognite.client.data_classes import ProjectURLName, ProjectURLNameList


@pytest.fixture(scope="module")
def available_projects(cognite_client) -> ProjectURLNameList:
    if projects := cognite_client.iam.projects.list():
        return projects
    pytest.skip("Can't test projects without any projects available", allow_module_level=True)


@pytest.mark.skip(reason="Lack access to projects to perform operations")
class TestProjects:
    def test_list_projects(self, available_projects: ProjectURLNameList) -> None:
        assert len(available_projects) >= 1, "Expected at least one project"
        assert isinstance(available_projects, ProjectURLNameList)
        assert isinstance(available_projects[0], ProjectURLName)
