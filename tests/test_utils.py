"""
Tests for utility functions in src.utils.

This module tests helper functions for CLI argument parsing, logger creation,
and sample project directory copying.
"""

import logging
from unittest.mock import patch
from src.utils import (
    get_user_project_dir_cli_input,
    get_process_logger,
    copy_sample_project_into_user_path,
)


class TestUtils:
    """Test suite for utility functions."""

    @patch("argparse.ArgumentParser.parse_args")
    def test_returns_provided_directory_path(self, mock_parse_args, temp_project_path):
        """
        Test that get_user_project_dir_cli_input returns the user-provided path.

        Parses command line arguments and returns the value of the
        --user-project-dir option if provided.
        """
        temp_project_path_str = str(temp_project_path)

        mock_parse_args.return_value.user_project_dir = temp_project_path_str

        assert get_user_project_dir_cli_input() == temp_project_path_str

    def test_creates_logger_with_stream_handler(self):
        """
        Test that get_process_logger creates a logger with a StreamHandler.

        The logger should have at least one handler of type StreamHandler
        for outputting log messages to stdout/stderr.
        """
        logger = get_process_logger("test_logger")

        assert logger.name == "test_logger"
        assert len(logger.handlers) >= 1
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

    @patch("shutil.copytree")
    def test_copies_sample_project_successfully(self, mock_copytree, temp_project_path):
        """
        Test that copy_sample_project_into_user_path copies the sample project.

        The sample_project directory should be copied into the parent directory
        with the name 'wikimedia_eventstreams_live_excel_reports' and the
        destination path should be returned as a string.
        """
        sample_project_result_dir = copy_sample_project_into_user_path(
            temp_project_path
        )

        expected_dest_dir = (
            temp_project_path / "wikimedia_eventstreams_live_excel_reports"
        )

        mock_copytree.assert_called_once_with(
            "sample_project", expected_dest_dir, dirs_exist_ok=False
        )

        assert sample_project_result_dir == str(expected_dest_dir)
