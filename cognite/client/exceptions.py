class APIError(Exception):
    """Cognite API Error

    Raised if a given request fails

    Args:
        message (str):  The error message produced by the API
        code (int):     The error code produced by the failure
        x_request_id (str): The request-id generated for the failed request.
    """

    def __init__(self, message, code=None, x_request_id=None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id

    def __str__(self):
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)
