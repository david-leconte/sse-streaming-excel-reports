"""
Pytest configuration and shared fixtures for the test suite.

This module defines reusable pytest fixtures that provide common test dependencies
such as temporary directories, mock objects, and sample configurations. These
fixtures are automatically available to all test modules in this directory.
"""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def temp_project_path(tmp_path):
    """Create a temporary project directory for testing."""
    project_path = tmp_path / "test_project"
    project_path.mkdir(exist_ok=True)
    (project_path / "logs").mkdir()
    return project_path


@pytest.fixture
def sample_api_base_url():
    """Provide sample API base URL."""
    return "https://test.local"


@pytest.fixture
def sample_one_sse_topic(temp_project_path):  # pylint: disable=redefined-outer-name
    """Provide a sample SSE topic name, metadata configuration and basepath."""
    return {
        "name": "t1",
        "metadata": {"t1": {"path": "/stream", "primary_key": ["id"]}},
        "basepaths": {"t1": temp_project_path / "data/queues/test_topic"},
    }


@pytest.fixture
def dbt_result():
    """Create a mock dbt result."""
    result = MagicMock()
    result.success = True
    result.exception = None

    return result
