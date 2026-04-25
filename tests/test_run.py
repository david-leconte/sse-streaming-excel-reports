class TestRunInit:
    def test_initializes_with_valid_project_directory(self):
        """Should initialize successfully with valid user project directory."""
        # TODO

    def test_sets_gui_run_flag_correctly(self):
        """Should store is_gui_run flag."""
        # TODO

    def test_loads_config_from_topics_toml(self):
        """Should load configuration from topics.toml file."""
        # TODO

    def test_raises_file_not_found_for_missing_project_directory(self):
        """Should raise FileNotFoundError if user_project_dir_input doesn't exist."""
        # TODO

    def test_raises_file_not_found_for_missing_topics_toml(self):
        """Should raise FileNotFoundError if topics.toml is missing."""
        # TODO

    def test_initializes_is_active_to_false(self):
        """Should initialize is_active to False."""
        # TODO

    def test_creates_data_paths(self):
        """Should create local data directory structure."""
        # TODO

    def test_propagates_dbt_files_to_user_path(self):
        """Should copy dbt files to the user project path."""
        # TODO

    def test_handles_gui_log_queue_when_provided(self):
        """Should accept and use gui_global_log_queue parameter."""
        # TODO

    def test_sets_logger_level_to_error_by_default(self):
        """Should set logger level to ERROR by default."""
        # TODO


class TestGetUserConfigAndProjectPath:
    def test_returns_api_base_url_from_topics_toml(self):
        """Should extract api.base_url from topics.toml."""
        # TODO

    def test_returns_topics_metadata_from_topics_toml(self):
        """Should extract topics section from topics.toml."""
        # TODO

    def test_returns_user_project_path(self):
        """Should return Path object to user project directory."""
        # TODO

    def test_raises_file_not_found_when_topics_file_missing(self):
        """Should raise FileNotFoundError if topics.toml doesn't exist."""
        # TODO

    def test_raises_key_error_when_api_base_url_missing(self):
        """Should raise KeyError if 'api.base_url' is missing from config."""
        # TODO

    def test_raises_key_error_when_topics_missing(self):
        """Should raise KeyError if 'topics' section is missing from config."""
        # TODO

    def test_raises_runtime_error_on_toml_decode_error(self):
        """Should raise RuntimeError if TOML file is malformed."""
        # TODO

    def test_raises_runtime_error_on_file_read_error(self):
        """Should raise RuntimeError if file cannot be read."""
        # TODO

    def test_handles_multiple_topics_in_config(self):
        """Should correctly parse multiple topics from topics.toml."""
        # TODO

    def test_returns_tuple_in_correct_order(self):
        """Should return tuple as (api_url, topics_metadata, project_path)."""
        # TODO


class TestPropagateDbtFilesToUserPath:
    def test_copies_dbt_files_to_user_path(self):
        """Should copy dbt files from project to user path."""
        # TODO

    def test_creates_dbt_directory_if_not_exists(self):
        """Should create dbt directory in user project path if missing."""
        # TODO

    def test_preserves_dbt_file_structure(self):
        """Should preserve directory structure of dbt files."""
        # TODO

    def test_handles_missing_dbt_directory_gracefully(self):
        """Should handle case when dbt directory doesn't exist in source."""
        # TODO


class TestCreateLocalFilesDirs:
    def test_creates_data_directory(self):
        """Should create data directory in user project path."""
        # TODO

    def test_creates_warehouse_subdirectory(self):
        """Should create data/warehouse subdirectory."""
        # TODO

    def test_creates_logs_directory(self):
        """Should create logs directory in user project path."""
        # TODO

    def test_returns_path_to_data_directory(self):
        """Should return Path object to created data directory."""
        # TODO

    def test_handles_existing_directories_gracefully(self):
        """Should handle case when directories already exist."""
        # TODO


class TestCreateLocalQueuesPaths:
    def test_creates_queue_directories_for_each_topic(self):
        """Should create a queue directory for each topic."""
        # TODO

    def test_returns_dict_with_topic_keys(self):
        """Should return dictionary with topic names as keys."""
        # TODO

    def test_returns_dict_with_path_values(self):
        """Should return dictionary with Path objects as values."""
        # TODO

    def test_queue_paths_are_under_data_directory(self):
        """Should create queue directories under data directory."""
        # TODO


class TestRunLaunch:
    def test_launches_topics_writer_process(self):
        """Should start the TopicQueuesAsyncWriter process."""
        # TODO

    def test_launches_warehouse_transformer_process(self):
        """Should start the WarehouseTransformer process."""
        # TODO

    def test_sets_is_active_to_true(self):
        """Should set is_active flag to True."""
        # TODO

    def test_processes_are_started_in_correct_order(self):
        """Should start topics writer before warehouse transformer."""
        # TODO

    def test_passes_correct_arguments_to_topics_writer_process(self):
        """Should pass api_url, topics_metadata, and paths to topics writer."""
        # TODO

    def test_passes_correct_arguments_to_warehouse_transformer_process(self):
        """Should pass config and paths to warehouse transformer."""
        # TODO

    def test_processes_run_in_background(self):
        """Should spawn processes that run independently."""
        # TODO


class TestRunStop:
    def test_terminates_topics_writer_process(self):
        """Should terminate the running topics writer process."""
        # TODO

    def test_terminates_warehouse_transformer_process(self):
        """Should terminate the running warehouse transformer process."""
        # TODO

    def test_sets_is_active_to_false(self):
        """Should set is_active flag to False."""
        # TODO

    def test_handles_already_terminated_processes(self):
        """Should handle case when processes are already terminated."""
        # TODO

    def test_joins_processes_before_returning(self):
        """Should wait for processes to fully terminate."""
        # TODO


class TestRunExceptionHandling:
    def test_catches_value_error_during_init(self):
        """Should catch ValueError and log it appropriately."""
        # TODO

    def test_catches_file_not_found_error_during_init(self):
        """Should catch FileNotFoundError and log it appropriately."""
        # TODO

    def test_catches_key_error_during_init(self):
        """Should catch KeyError and log it appropriately."""
        # TODO

    def test_catches_runtime_error_during_init(self):
        """Should catch RuntimeError and log it appropriately."""
        # TODO

    def test_catches_os_error_during_init(self):
        """Should catch OSError and log it appropriately."""
        # TODO

    def test_exits_on_exception_when_not_gui_run(self):
        """Should call exit() when exception occurs and is_gui_run is False."""
        # TODO

    def test_logs_exception_details(self):
        """Should log full exception details including stack trace."""
        # TODO

    def test_does_not_exit_on_exception_when_gui_run(self):
        """Should not call exit() when exception occurs and is_gui_run is True."""
        # TODO
