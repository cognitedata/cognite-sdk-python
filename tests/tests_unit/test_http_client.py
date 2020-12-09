import socket
from unittest.mock import MagicMock

import pytest
import requests.exceptions
import urllib3.exceptions

from cognite.client._http_client import HTTPClient, HTTPClientConfig, _RetryTracker
from cognite.client.exceptions import CogniteConnectionError, CogniteConnectionRefused, CogniteReadTimeout

DEFAULT_CONFIG = HTTPClientConfig(
    status_codes_to_retry={429},
    backoff_factor=0.5,
    max_backoff_seconds=30,
    max_retries_total=10,
    max_retries_read=5,
    max_retries_connect=5,
    max_retries_status=10,
)


class TestRetryTracker:
    def test_total_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_total = 10
        rt.status = 4
        rt.connect = 4
        rt.read = 4

        assert rt.total == 12
        assert rt.should_retry(200) is False

    def test_status_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_status = 1
        assert rt.should_retry(None) is True
        rt.status = 1
        assert rt.should_retry(None) is False

    def test_read_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_read = 1
        assert rt.should_retry(None) is True
        rt.read = 1
        assert rt.should_retry(None) is False

    def test_connect_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_connect = 1
        assert rt.should_retry(None) is True
        rt.connect = 1
        assert rt.should_retry(None) is False

    def test_correct_retry_of_status(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        assert rt.should_retry(500) is False
        rt.config.status_codes_to_retry = {500, 429}
        assert rt.should_retry(500) is True

    def test_get_backoff_time(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        for i in range(1000):
            rt.read += 1
            assert 0 <= rt.get_backoff_time() <= DEFAULT_CONFIG.max_backoff_seconds


def raise_exception_wrapped_as_in_requests_lib(exc: Exception):
    try:
        raise exc
    except type(exc):
        try:
            raise urllib3.exceptions.RequestError(pool=None, url=None, message=None)
        except urllib3.exceptions.RequestError:
            raise requests.exceptions.RequestException


class TestHTTPClient:
    def test_read_timeout_errors(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        c = HTTPClient(
            config=cnf,
            retry_tracker_factory=lambda _: retry_tracker,
            session=MagicMock(
                request=MagicMock(
                    side_effect=lambda *args, **kwargs: raise_exception_wrapped_as_in_requests_lib(socket.timeout())
                )
            ),
        )

        with pytest.raises(CogniteReadTimeout):
            c.request("GET", "bla")

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.read == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.connect == 0
        assert retry_tracker.status == 0

    @pytest.mark.parametrize("exc_type", [ConnectionAbortedError, ConnectionResetError, BrokenPipeError])
    def test_connect_errors(self, exc_type):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        c = HTTPClient(
            config=cnf,
            retry_tracker_factory=lambda _: retry_tracker,
            session=MagicMock(
                request=MagicMock(
                    side_effect=lambda *args, **kwargs: raise_exception_wrapped_as_in_requests_lib(exc_type())
                )
            ),
        )

        with pytest.raises(CogniteConnectionError):
            c.request("GET", "bla")

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.read == 0
        assert retry_tracker.connect == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.status == 0

    def test_connection_refused_not_retried(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        c = HTTPClient(
            config=cnf,
            retry_tracker_factory=lambda _: retry_tracker,
            session=MagicMock(
                request=MagicMock(
                    side_effect=lambda *args, **kwargs: raise_exception_wrapped_as_in_requests_lib(
                        ConnectionRefusedError()
                    )
                )
            ),
        )

        with pytest.raises(CogniteConnectionRefused):
            c.request("GET", "bla")

        assert retry_tracker.total == 1
        assert retry_tracker.read == 0
        assert retry_tracker.connect == 1
        assert retry_tracker.status == 0

    def test_status_errors(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        c = HTTPClient(
            config=cnf,
            retry_tracker_factory=lambda _: retry_tracker,
            session=MagicMock(request=MagicMock(return_value=MagicMock(status_code=429))),
        )

        res = c.request("GET", "bla")
        assert res.status_code == 429

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_status
        assert retry_tracker.read == 0
        assert retry_tracker.connect == 0
        assert retry_tracker.status == DEFAULT_CONFIG.max_retries_status


class UnderlyingException(Exception):
    pass


class TestGetUnderlyingException:
    def test_get_underlying_exception_does_not_exist_in_context(self):
        try:
            raise Exception
        except Exception as e:
            assert not HTTPClient._any_exception_in_context_isinstance(e, UnderlyingException)

    def test_get_underlying_exception_no_context(self):
        try:
            raise UnderlyingException
        except Exception as e:
            assert HTTPClient._any_exception_in_context_isinstance(e, (UnderlyingException, Exception))

    def test_get_underlying_exception_nested_1_layer(self):
        def testcase():
            try:
                raise UnderlyingException
            except Exception:
                raise Exception()

        try:
            testcase()
        except Exception as e:
            assert HTTPClient._any_exception_in_context_isinstance(e, UnderlyingException)

    def test_get_underlying_exception_nested_2_layers(self):
        def testcase():
            try:
                raise UnderlyingException
            except Exception:
                try:
                    raise Exception()
                except Exception:
                    raise Exception()

        try:
            testcase()
        except Exception as e:
            assert HTTPClient._any_exception_in_context_isinstance(e, UnderlyingException)
