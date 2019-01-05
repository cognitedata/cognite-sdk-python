# -*- coding: utf-8 -*-
from copy import copy

from cognite.client._api_client import APIClient, CogniteResponse


class LoginStatusResponse(CogniteResponse):
    """
    Attributes:
        user (str): Current user
        logged_in (bool): Is user logged in
        project (str): Current project
        project_id (str): Current project id
    """

    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        self.user = internal_representation["data"]["user"]
        self.project = internal_representation["data"]["project"]
        self.project_id = internal_representation["data"]["projectId"]
        self.logged_in = internal_representation["data"]["loggedIn"]

    def to_json(self):
        json_repr = copy(self.__dict__)
        del json_repr["internal_representation"]
        return json_repr


class LoginClient(APIClient):
    def status(self) -> LoginStatusResponse:
        """Check login status

        Returns:
            client.stable.login.LoginStatusResponse

        Examples:
            Check the current login status::

                client = CogniteClient()
                login_status = client.login.status()
                print(login_status)

        """
        return LoginStatusResponse(self._get("/login/status").json())
