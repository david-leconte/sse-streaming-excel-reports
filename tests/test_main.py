"""
Tests for the main entry point module.

This module contains tests for the main() function which serves as the application's
entry point. The tests verify that the correct components are instantiated based on
whether a project directory is provided via CLI arguments or the GUI should be launched.
"""

from unittest.mock import patch
from src.__main__ import main


class TestMainFunction:
    """Test suite for the main() function behavior."""

    @patch("src.__main__.Run")
    @patch("src.__main__.get_user_project_dir_cli_input")
    def test_creates_run_instance_when_cli_arg_provided(
        self, mock_user_dir_input, mock_run_class, temp_project_path
    ):
        """
        Test that main() creates a Run instance when a CLI argument is provided.

        When get_user_project_dir_cli_input returns a valid path, main() should
        instantiate the Run class with that path and call its launch() method.
        """
        temp_project_path_str = str(temp_project_path)

        mock_user_dir_input.return_value = temp_project_path_str
        main()

        mock_run_class.assert_called_once_with(temp_project_path_str)
        mock_run_class.return_value.launch.assert_called_once()

    @patch("src.__main__.AppWindow")
    @patch("src.__main__.get_user_project_dir_cli_input", return_value=None)
    def test_creates_gui_app_when_no_cli_arg(
        self, _mock_user_dir_input, mock_app_window_class
    ):
        """
        Test that main() launches GUI when no CLI argument is provided.

        When get_user_project_dir_cli_input returns None, main() should instantiate
        the AppWindow class and start its mainloop.
        """
        main()
        mock_app_window_class.assert_called_once()
        mock_app_window_class.return_value.mainloop.assert_called_once()
