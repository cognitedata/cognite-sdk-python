from typing import *

from cognite.client._api_client import APIClient
from cognite.client._base import CogniteResponse


class LoginStatus(CogniteResponse):
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
        self.project_id = project_id
        self.logged_in = logged_in

    @classmethod
    def _load(cls, api_response):
        data = api_response["data"]
        return cls(user=data["user"], project=data["project"], logged_in=data["loggedIn"], project_id=data["projectId"])


class LoginAPI(APIClient):
    _RESOURCE_PATH = "/login"

    def status(self) -> LoginStatus:
        """Check login status

        Returns:
            LoginStatus: The login status of the current api key.

        Examples:
            Check the current login status and get the project::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> login_status = c.login.status()
                >>> project = login_status.project

        """
        return LoginStatus._load(self._get(self._RESOURCE_PATH + "/status").json())
