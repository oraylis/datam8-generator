"""
Simple conftest for unit tests that don't require external dependencies.
"""
import pytest


def pytest_configure(config):
    """Configure pytest for unit tests without external solution files."""
    # Simple configuration that doesn't require solution files
    pass