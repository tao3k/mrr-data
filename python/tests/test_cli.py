import sys
from unittest.mock import Mock

from mrr_data_testing import cli


def test_dispatch_preserves_suite_arguments(monkeypatch):
    suite = Mock()
    loader = Mock(return_value=suite)
    monkeypatch.setattr(cli.importlib, "import_module", loader)
    monkeypatch.setattr(sys, "argv", ["mrr-data-test", "duckgql-build", "--jobs", "2"])
    cli.main()
    loader.assert_called_once_with("mrr_data_testing.duckgql.build")
    suite.main.assert_called_once_with()
    assert sys.argv == ["mrr-data-test duckgql-build", "--jobs", "2"]


def test_suite_help_is_forwarded(monkeypatch):
    suite = Mock()
    monkeypatch.setattr(cli.importlib, "import_module", Mock(return_value=suite))
    monkeypatch.setattr(sys, "argv", ["mrr-data-test", "duckgql-qualify", "--help"])
    cli.main()
    assert sys.argv[-1] == "--help"
