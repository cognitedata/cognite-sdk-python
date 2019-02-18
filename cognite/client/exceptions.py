import json


class APIError(Exception):
    """Cognite API Error

    Raised if a given request fails.

    Args:
        message (str):  The error message produced by the API
        code (int):     The error code produced by the failure
        x_request_id (str): The request-id generated for the failed request.
        extra (Dict):   A dict of any additional information.

    Examples:
        Catching an API-error and handling it based on the error code::

            from cognite.client import CogniteClient, APIError

            client = CogniteClient()

            try:
                client.login.status()
            except APIError as e:
                if e.code == 401:
                    print("You are not authorized")
                elif e.code == 400:
                    print("Something is wrong with your request")
                elif e.code == 500:
                    print("Something went terribly wrong. Here is the request-id: {}".format(e.x_request_id)
                print("The message returned from the API: {}".format(e.message))
    """

    def __init__(self, message, code=None, x_request_id=None, extra=None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.extra = extra

    def __str__(self):
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            return "{} | code: {} | X-Request-ID: {}\n{}".format(
                self.message, self.code, self.x_request_id, pretty_extra
            )
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)
