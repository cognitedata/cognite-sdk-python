from cognite.client.data_classes._base import CogniteResponse


class LoginStatus(CogniteResponse):
    def __init__(self, user, project, logged_in, project_id, api_key_id):
        self.user = user
        self.project = project
        self.project_id = project_id
        self.logged_in = logged_in
        self.api_key_id = api_key_id

    @classmethod
    def _load(cls, api_response):
        data = api_response["data"]
        return cls(
            user=data["user"],
            project=data["project"],
            logged_in=data["loggedIn"],
            project_id=data["projectId"],
            api_key_id=data.get("apiKeyId"),
        )
