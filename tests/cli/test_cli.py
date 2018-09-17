import os
import sys
from unittest import mock

import pytest

from cognite.cli.cli import CogniteCLI
from cognite.cli.cli_models import CogniteModelsCLI


class TestCogniteCLI:
    @mock.patch("cognite.cli.cli.getattr")
    def test_use_service(self, getattr_mock):
        sys.argv = ["cognite", "models"]
        CogniteCLI()
        assert 1 == getattr_mock.call_count

    @pytest.mark.noautofixt
    @mock.patch("cognite.cli.cli.getattr")
    def test_no_env_vars_set(self, getattr_mock):
        sys.argv = ["cognite", "models"]
        CogniteCLI()
        assert 0 == getattr_mock.call_count

    @mock.patch("cognite.cli.cli.getattr")
    def test_unrecognized_service(self, getattr_mock):
        sys.argv = ["cognite", "blopi"]
        CogniteCLI()
        assert 0 == getattr_mock.call_count

    @mock.patch("cognite.cli.cli.CogniteModelsCLI")
    def test_models_service(self, ml_cli_mock):
        sys.argv = ["cognite", "models", "get"]
        ml_cli_instance_mock = mock.MagicMock(spec=CogniteModelsCLI)
        ml_cli_mock.return_value = ml_cli_instance_mock
        CogniteCLI()
        assert 1 == ml_cli_instance_mock.get.call_count
