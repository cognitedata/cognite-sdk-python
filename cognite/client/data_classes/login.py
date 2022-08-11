from typing import Any, Dict

from cognite.client.data_classes._base import CogniteResponse


class LoginStatus(CogniteResponse):
    """Current login status

    Args:
        user (str): Current user.
        logged_in (bool): Is user logged in.
        project (str): Current project.
        project_id (int): Current project id.
        api_key_id (int): Current api key id.
    """

    def __init__(self, user: str, project: str, logged_in: bool, project_id: int, api_key_id: int):
        self.user = user
        self.project = project
        self.project_id = project_id
        self.logged_in = logged_in
        self.api_key_id = api_key_id

    @classmethod
    def _load(cls, api_response: Dict[str, Any]) -> "LoginStatus":
        data = api_response["data"]
        return cls(
            user=data["user"],
            project=data["project"],
            logged_in=data["loggedIn"],
            project_id=data["projectId"],
            api_key_id=data.get("apiKeyId"),
        )
