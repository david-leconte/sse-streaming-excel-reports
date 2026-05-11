"""
Tests for the Run class which manages the event processing lifecycle.

This module tests the Run class responsible for coordinating the SSE streaming,
queue writing, and warehouse transformation processes. Tests cover initialization,
process launching, and termination behavior.
"""

import pytest
from unittest.mock import patch, MagicMock
from src.run import Run


class TestRun:
    """Test suite for the Run class functionality."""

    @patch("src.run.Path.exists", return_value=True)
    @patch("builtins.open")
    @patch("tomllib.load")
    @patch.object(Run, "_propagate_dbt_files_to_user_path")
    @patch.object(Run, "_create_local_files_dirs")
    @patch.object(Run, "_create_local_queues_paths")
    def test_initializes_with_valid_project_directory(
        self,
        _mock_create_local_queues_paths,
        _mock_create_local_files_dirs,
        _mock_propagate_dbt_files_to_user_path,
        mock_toml_load,
        _mock_open,
        _mock_path_exists,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
        Test that Run initializes correctly with a valid project directory.

        The topics.toml file must exist, and after initialization the run
        should be inactive (not processing).
        """

        mock_toml_load.return_value = {
            "api": {"base_url": sample_api_base_url},
            "topics": sample_one_sse_topic["metadata"],
        }

        run = Run(str(temp_project_path))
        assert run.is_active is False

    @patch("src.run.Path.exists", return_value=False)
    def test_exits_for_missing_topics_toml(self, _mock_path_exists):
        """
        Test that Run exits when topics.toml is missing.

        If the project directory does not contain a topics.toml file,
        the Run constructor should call sys.exit(1).
        """
        with pytest.raises(SystemExit):
            Run("invalid_path")

    @patch("src.run.Process")
    @patch.object(
        Run, "_get_user_config_and_project_path", return_value=("url", {}, MagicMock())
    )
    @patch.object(Run, "_propagate_dbt_files_to_user_path")
    @patch.object(Run, "_create_local_files_dirs", return_value=MagicMock())
    @patch.object(Run, "_create_local_queues_paths", return_value={})
    def test_launches_processes(
        self,
        _mock_create_local_queues_paths,
        _mock_create_local_files_dirs,
        _mock_propagate_dbt_files_to_user_path,
        _mock_get_user_config_and_project_path,
        mock_process,
        temp_project_path,
    ):
        """
        Test that launch() starts the required worker processes.

        When launched, two subprocesses should be created: one for writing
        topic queues (write_topics) and one for transforming to warehouse
        (write_warehouse). Both processes should be started and is_active set to True.
        """
        run = Run(str(temp_project_path), is_gui_run=True)
        run.launch()

        assert mock_process.call_count == 2

        first_process = mock_process.return_value
        second_process = mock_process.return_value
        first_process.start.assert_called()
        second_process.start.assert_called()

        assert run.is_active is True

    @patch("src.run.Process")
    @patch.object(
        Run, "_get_user_config_and_project_path", return_value=("url", {}, MagicMock())
    )
    @patch.object(Run, "_propagate_dbt_files_to_user_path")
    @patch.object(Run, "_create_local_files_dirs", return_value=MagicMock())
    @patch.object(Run, "_create_local_queues_paths", return_value={})
    def test_terminate_marks_run_inactive(
        self,
        _mock_create_local_queues_paths,
        _mock_create_local_files_dirs,
        _mock_propagate_dbt_files_to_user_path,
        _mock_get_user_config_and_project_path,
        mock_process,
        temp_project_path,
    ):
        """
        Test that terminate() stops all processes and marks run as inactive.

        When terminate() is called, both worker processes should receive
        the terminate() call and is_active should be set to False.
        """
        run = Run(str(temp_project_path), is_gui_run=True)
        run.launch()

        run.terminate()

        assert mock_process.return_value.terminate.call_count == 2
        assert run.is_active is False
