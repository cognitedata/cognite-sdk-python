import httpx
import pytest

from cognite.client._http_client import HTTPClientWithRetryConfig, RetryTracker


@pytest.fixture
def default_config():
    return HTTPClientWithRetryConfig(
        status_codes_to_retry={429},
        backoff_factor=0.5,
        max_backoff_seconds=30,
        max_retries_total=10,
        max_retries_read=5,
        max_retries_connect=4,
        max_retries_status=10,
    )


@pytest.fixture
def example_error():
    return httpx.RequestError("nice error")


class TestRetryTracker:
    def test_total_retries_exceeded(self, default_config):
        default_config._max_retries_total = 10
        rt = RetryTracker(default_config)
        rt.status = 4
        rt.connect = 4
        rt.read = 4

        assert rt.total == 12
        assert rt.should_retry_total is False
        assert rt.should_retry_status_code(429) is False

    def test_status_retries_exceeded(self, default_config):
        default_config._max_retries_status = 1
        rt = RetryTracker(default_config)
        assert rt.should_retry_total is True
        assert rt.should_retry_status_code(429) is True
        assert rt.should_retry_status_code(429) is False
        assert rt.last_failed_reason == "status_code=429"

    def test_read_retries_exceeded(self, default_config, example_error):
        default_config._max_retries_read = 1
        rt = RetryTracker(default_config)
        assert rt.should_retry_timeout(example_error) is True
        assert rt.should_retry_timeout(example_error) is False
        assert rt.last_failed_reason == "RequestError"

    def test_connect_retries_exceeded(self, default_config, example_error):
        default_config._max_retries_connect = 1
        rt = RetryTracker(default_config)
        assert rt.should_retry_connect_error(example_error) is True
        assert rt.should_retry_connect_error(example_error) is False

    def test_correct_retry_of_status(self, default_config):
        rt = RetryTracker(default_config)
        assert rt.should_retry_status_code(500) is False
        rt.config.status_codes_to_retry.add(500)
        assert rt.should_retry_status_code(500) is True

    def test_get_backoff_time(self, default_config):
        rt = RetryTracker(default_config)
        for i in range(10):
            rt.read += 1
            assert 0 <= rt.get_backoff_time() <= default_config.max_backoff_seconds

    def test_is_auto_retryable(self, default_config):
        default_config._max_retries_status = 1
        rt = RetryTracker(default_config)

        # 409 is not in the list of status codes to retry, but we set is_auto_retryable=True, which should override it
        assert rt.should_retry_status_code(409, is_auto_retryable=True) is True
        assert rt.should_retry_status_code(409, is_auto_retryable=False) is False
