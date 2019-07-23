import pytest


@pytest.fixture
def mock_data_spec():
    class FakeScheduleDataSpec:
        def dump(self):
            return {"spec": "spec"}

    return FakeScheduleDataSpec()
