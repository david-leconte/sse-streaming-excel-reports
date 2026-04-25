import pytest


# Fixtures for temporary directories and files
@pytest.fixture
def temp_project_dir():
    """Create a temporary project directory for testing."""
    # TODO


@pytest.fixture
def temp_user_project_path():
    """Create a temporary user project path with required structure."""
    # TODO


@pytest.fixture
def temp_topics_toml_file(temp_user_project_path):
    """Create a temporary topics.toml file with sample configuration."""
    # TODO


@pytest.fixture
def temp_dbt_directory(temp_user_project_path):
    """Create a temporary dbt directory structure."""
    # TODO


@pytest.fixture
def temp_data_directory(temp_user_project_path):
    """Create a temporary data directory with required subdirectories."""
    # TODO


# Fixtures for configuration
@pytest.fixture
def sample_sse_topics_metadata():
    """Provide sample SSE topics metadata configuration."""
    # TODO


@pytest.fixture
def sample_api_base_url():
    """Provide sample API base URL."""
    # TODO


@pytest.fixture
def sample_app_config():
    """Provide sample app configuration."""
    # TODO


# Fixtures for multiprocessing
@pytest.fixture
def mock_queue():
    """Create a mock Queue for testing."""
    # TODO


@pytest.fixture
def gui_log_queue():
    """Create a real Queue for GUI logging tests."""
    # TODO


# Fixtures for logging
@pytest.fixture
def mock_logger():
    """Create a mock logger for testing."""
    # TODO


@pytest.fixture
def capture_logger_output():
    """Fixture to capture logger output."""
    # TODO


# Fixtures for mocking external dependencies
@pytest.fixture
def mock_duckdb_connection():
    """Create a mock DuckDB connection."""
    # TODO


@pytest.fixture
def mock_dbt_runner():
    """Create a mock dbtRunner instance."""
    # TODO


@pytest.fixture
def mock_run_instance(temp_user_project_path):
    """Create a mock Run instance."""
    # TODO


@pytest.fixture
def mock_topic_queues_async_writer():
    """Create a mock TopicQueuesAsyncWriter instance."""
    # TODO


@pytest.fixture
def mock_warehouse_transformer():
    """Create a mock WarehouseTransformer instance."""
    # TODO


# Fixtures for HTTP/aiohttp
@pytest.fixture
def mock_aiohttp_session():
    """Create a mock aiohttp ClientSession."""
    # TODO


@pytest.fixture
def mock_sse_response():
    """Create a mock SSE response."""
    # TODO


# Fixtures for file system
@pytest.fixture
def sample_bin_file(temp_user_project_path):
    """Create a sample .bin queue file."""
    # TODO


@pytest.fixture
def sample_json_events():
    """Provide sample JSON events for testing."""
    # TODO


# Fixtures for GUI testing
@pytest.fixture
def mock_tkinter_window():
    """Create a mock Tkinter window."""
    # TODO


@pytest.fixture
def mock_filedialog():
    """Create a mock filedialog module."""
    # TODO


# Markers for test categorization
def pytest_configure(config):
    """Register custom markers."""
    # TODO


# Custom hooks
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup resources after each test."""
    # TODO


@pytest.fixture
def mock_asyncio_run():
    """Mock asyncio.run() for testing."""
    # TODO


@pytest.fixture
def mock_process():
    """Create a mock Process for testing."""
    # TODO


@pytest.fixture
def sample_log_record():
    """Create a sample LogRecord for GUI testing."""
    # TODO


@pytest.fixture
def temporary_working_directory():
    """Create and manage temporary working directory."""
    # TODO


@pytest.fixture
def mock_sseclient():
    """Create a mock sseclient for testing."""
    # TODO


@pytest.fixture
def mock_pyarrow_table():
    """Create a mock PyArrow Table."""
    # TODO
