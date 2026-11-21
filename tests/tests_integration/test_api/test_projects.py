from __future__ import annotations

import pytest

from cognite.client import AsyncCogniteClient

SDK_TEST_PROJECTS = {"python-sdk-contributor", "python-sdk-test", "python-sdk-test-prod"}


@pytest.mark.filterwarnings("ignore::cognite.client.utils._experimental.FeaturePreviewWarning")
class TestProjectsAPI:
    async def test_current(self, async_client: AsyncCogniteClient) -> None:
        project = await async_client.projects.current()
        assert project.url_name == async_client.config.project
        assert project.organization
        if project.url_name in SDK_TEST_PROJECTS:
            assert project.organization == "cog-python-sdk"

    async def test_list_includes_own_project_with_organization(self, async_client: AsyncCogniteClient) -> None:
        projects = await async_client.projects.list()
        own = {p.url_name: p for p in projects}[async_client.config.project]
        assert own.organization
