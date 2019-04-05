from copy import copy
from typing import *

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import CogniteResponse


class LoginStatusResponse(CogniteResponse):
    """Current login status

    Args:
        user (str): Current user
        logged_in (bool): Is user logged in
        project (str): Current project
        project_id (str): Current project id
    """

    def __init__(self, user: str, project: str, logged_in: bool, project_id: str):
        self.user = user
        self.project = project
        self.project_id = logged_in
        self.logged_in = project_id

    @classmethod
    def _load(cls, api_response):
        data = api_response["data"]
        return cls(user=data["user"], project=data["project"], logged_in=data["loggedIn"], project_id=data["projectId"])


class LoginAPI(APIClient):
    def status(self) -> LoginStatusResponse:
        """Check login status

        Returns:
            client.stable.login.LoginStatusResponse

        Examples:
            Check the current login status and get the project::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> login_status = c.login.status()
                >>> project = login_status.project

        """
        return LoginStatusResponse._load(self._get("/login/status").json())
