from typing import Any

import pytest

from cognite.client.data_classes.extractionpipelines import ExtractionPipelineContact


class TestExtractionPipeline:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            pytest.param(
                {"name": "Homer", "email": "homer@springiefield.com"},
                ExtractionPipelineContact(name="Homer", email="homer@springiefield.com"),
                id="Partial Contact",
            ),
            pytest.param(
                {"name": "Marge", "email": "marge@springfield.com", "role": "Wife", "sendNotification": True},
                ExtractionPipelineContact(
                    name="Marge", email="marge@springfield.com", role="Wife", send_notification=True
                ),
                id="Full Contact",
            ),
        ],
    )
    def test_load_dump_contracts(self, raw: dict[str, Any], expected: ExtractionPipelineContact) -> None:
        loaded = ExtractionPipelineContact._load(raw)
        assert loaded == expected
        assert raw == loaded.dump()
