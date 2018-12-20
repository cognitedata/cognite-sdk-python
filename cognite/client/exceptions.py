class APIError(Exception):
    def __init__(self, message, code=None, x_request_id=None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id

    def __str__(self):
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)
