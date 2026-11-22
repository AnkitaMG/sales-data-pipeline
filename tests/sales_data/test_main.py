"""Tests for the sales data pipeline entry point."""

import runpy

from sales_data.main import main


def test_main_returns_none() -> None:
    """The unimplemented entry point currently completes without a result."""
    assert main() is None


def test_module_can_run_as_script() -> None:
    """Running the module as a script should complete without raising."""
    runpy.run_module("sales_data.main", run_name="__main__")
