class TestGetUserProjectDirCliInput:
    def test_returns_provided_directory_path(self):
        """Should return the user project directory when provided as CLI argument."""
        # TODO

    def test_returns_none_when_no_argument_provided(self):
        """Should return None when no CLI argument is provided."""
        # TODO

    def test_handles_relative_paths(self):
        """Should accept and return relative paths."""
        # TODO

    def test_handles_absolute_paths(self):
        """Should accept and return absolute paths."""
        # TODO

    def test_handles_paths_with_spaces(self):
        """Should handle paths containing spaces."""
        # TODO


class TestGetProcessLogger:
    def test_creates_logger_with_stream_handler(self):
        """Should create a logger with stream handler by default."""
        # TODO

    def test_creates_logger_with_file_handler_when_path_provided(self):
        """Should add file handler when user_project_path is provided."""
        # TODO

    def test_creates_logger_with_queue_handler_when_queue_provided(self):
        """Should add queue handler when gui_global_log_queue is provided."""
        # TODO

    def test_logger_has_correct_formatter(self):
        """Should apply the correct formatter with timestamp and log level."""
        # TODO

    def test_logger_level_set_to_info(self):
        """Should set logger level to INFO."""
        # TODO

    def test_logger_propagation_disabled(self):
        """Should disable propagation for the logger."""
        # TODO

    def test_creates_log_file_in_correct_directory(self):
        """Should create log file in the user_project_path/logs directory."""
        # TODO

    def test_log_filename_sanitizes_logger_name(self):
        """Should sanitize logger name for use as filename."""
        # TODO

    def test_handles_multiple_handlers_simultaneously(self):
        """Should handle multiple handlers (stream, file, queue) together."""
        # TODO

    def test_logs_correctly_to_stream(self):
        """Should successfully log messages to stream handler."""
        # TODO

    def test_logs_correctly_to_file(self):
        """Should successfully log messages to file handler."""
        # TODO

    def test_logs_correctly_to_queue(self):
        """Should successfully log messages to queue handler."""
        # TODO


class TestCopySampleProjectIntoUserPath:
    def test_copies_sample_project_successfully(self):
        """Should copy sample_project directory to the specified user path."""
        # TODO

    def test_returns_full_path_to_copied_project(self):
        """Should return the full path to the newly created project directory."""
        # TODO

    def test_creates_correct_directory_name(self):
        """Should create directory named 'wikimedia_eventstreams_live_excel_reports'."""
        # TODO

    def test_raises_file_exists_error_when_directory_exists(self):
        """Should raise FileExistsError if target directory already exists."""
        # TODO

    def test_raises_os_error_on_copy_failure(self):
        """Should raise OSError if copy operation fails."""
        # TODO

    def test_preserves_directory_structure(self):
        """Should preserve the complete directory structure from sample_project."""
        # TODO

    def test_preserves_file_contents(self):
        """Should preserve file contents during copy."""
        # TODO

    def test_handles_user_path_with_spaces(self):
        """Should handle user paths containing spaces."""
        # TODO

    def test_handles_non_existent_parent_directory(self):
        """Should handle case when parent directory doesn't exist gracefully."""
        # TODO


class TestAppConfigLoading:
    def test_app_config_is_dict(self):
        """Should load app_config as a dictionary."""
        # TODO

    def test_app_config_has_user_agent_key(self):
        """Should contain 'user_agent' key."""
        # TODO

    def test_app_config_has_topic_new_queue_file_seconds_threshold_key(self):
        """Should contain 'topic_new_queue_file_seconds_threshold' key."""
        # TODO

    def test_app_config_has_load_and_transform_every_seconds_key(self):
        """Should contain 'load_and_transform_every_seconds' key."""
        # TODO

    def test_app_config_has_output_gold_csv_every_seconds_key(self):
        """Should contain 'output_gold_csv_every_seconds' key."""
        # TODO

    def test_app_config_values_are_correct_types(self):
        """Should have values of expected types (strings, ints)."""
        # TODO

    def test_app_config_loads_from_correct_file(self):
        """Should load configuration from app_config.toml in project root."""
        # TODO
