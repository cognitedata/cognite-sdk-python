import json
import reprlib


class CogniteException(Exception):
    pass


class CogniteConnectionError(CogniteException):
    pass


class CogniteConnectionRefused(CogniteConnectionError):
    pass


class CogniteReadTimeout(CogniteException):
    pass


class CogniteMultiException(CogniteException):
    def __init__(self, successful=None, failed=None, unknown=None, unwrap_fn=None):
        self.successful = successful or []
        self.failed = failed or []
        self.unknown = unknown or []
        self._unwrap_fn = unwrap_fn or (lambda x: x)

    def _get_multi_exception_summary(self):
        if (len(self.successful) == 0) and (len(self.unknown) == 0) and (len(self.failed) == 0):
            return ""
        return "\n".join(
            (
                "",
                "The API Failed to process some items.",
                f"Successful (2xx): {list(map(self._unwrap_fn, self.successful))}",
                f"Unknown (5xx): {list(map(self._unwrap_fn, self.unknown))}",
                f"Failed (4xx): {list(map(self._unwrap_fn, self.failed))}",
            )
        )


class CogniteAPIError(CogniteMultiException):
    def __init__(
        self,
        message,
        code,
        x_request_id=None,
        missing=None,
        duplicated=None,
        successful=None,
        failed=None,
        unknown=None,
        unwrap_fn=None,
        extra=None,
    ):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.missing = missing
        self.duplicated = duplicated
        self.extra = extra
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self):
        msg = f"{self.message} | code: {self.code} | X-Request-ID: {self.x_request_id}"
        if self.missing:
            msg += f"\nMissing: {self.missing}"
        if self.duplicated:
            msg += f"\nDuplicated: {self.duplicated}"
        msg += self._get_multi_exception_summary()
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            msg += f"\nAdditional error info: {pretty_extra}"
        return msg


class CogniteNotFoundError(CogniteMultiException):
    def __init__(self, not_found, successful=None, failed=None, unknown=None, unwrap_fn=None):
        self.not_found = not_found
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self):
        not_found = self.not_found
        if not_found > 200:
            not_found = reprlib.repr(self.not_found)
        return f"Not found: {not_found}{self._get_multi_exception_summary()}"


class CogniteDuplicatedError(CogniteMultiException):
    def __init__(self, duplicated, successful=None, failed=None, unknown=None, unwrap_fn=None):
        self.duplicated = duplicated
        super().__init__(successful, failed, unknown, unwrap_fn)

    def __str__(self):
        msg = f"Duplicated: {self.duplicated}"
        msg += self._get_multi_exception_summary()
        return msg


class CogniteImportError(CogniteException):
    def __init__(self, module, message=None):
        self.module = module
        self.message = message or f"The functionality you are trying to use requires '{self.module}' to be installed."

    def __str__(self):
        return self.message


class CogniteMissingClientError(CogniteException):
    def __str__(self):
        return "A CogniteClient has not been set on this object. Pass it in the constructor to use it."


class CogniteAuthError(CogniteException):
    ...


class CogniteAPIKeyError(CogniteAuthError):
    ...


class ModelFailedException(Exception):
    def __init__(self, typename, id, error_message):
        self.typename = typename
        self.id = id
        self.error_message = error_message

    def __str__(self):
        return f"{self.typename} {self.id} failed with error '{self.error_message}'"
