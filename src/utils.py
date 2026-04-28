"""
Shared utilities for the SSE Streaming Excel Reports application.

This module provides common functionality used across the codebase:
- Command-line argument parsing for user project directory input
- Process-specific logger setup with optional file and queue handlers
- Sample project copying into user-specified parent directory
- Application configuration loading from app_config.toml
"""

import logging
from logging.handlers import QueueHandler
from pathlib import Path
import argparse
import shutil
import tomllib
from typing import Any
from multiprocessing.queues import Queue


def get_user_project_dir_cli_input() -> str | None:
    """
    Parse the command-line argument for the user project directory.

    The application expects an optional positional argument that points to a
    folder containing a topics.toml configuration file and (optionally) a
    models/ directory with dbt models.

    Returns:
        The provided project directory path as a string, or None if no
        argument was supplied.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "user_project_dir",
        nargs="?",
        help="Path to the user project folder",
    )
    args = parser.parse_args()

    if args.user_project_dir:
        return args.user_project_dir

    return None


def get_process_logger(
    name: str,
    user_project_path: Path | None = None,
    gui_global_log_queue: Queue | None = None,
) -> logging.Logger:
    """
    Create and configure a logger for a multiprocessing worker.

    The logger writes to stdout by default. If a user_project_path is provided,
    it also writes to a timestamped log file under <project>/logs/. If a
    gui_global_log_queue is provided, log records are sent to that queue for
    display in the GUI.

    Args:
        name: Logger name (typically the module's __name__).
        user_project_path: Root path of the user project for file logging.
        gui_global_log_queue: Multiprocessing Queue to forward log records
            to the GUI (optional).

    Returns:
        A configured logging.Logger instance.
    """
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    if user_project_path:
        filename = f"{name.translate(str.maketrans('.- ', '___')).lower()}.log"
        file_handler = logging.FileHandler(
            filename=str(user_project_path / "logs" / filename), encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

    if gui_global_log_queue:
        queue_handler = QueueHandler(gui_global_log_queue)
        queue_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    logger.addHandler(stream_handler)
    if user_project_path:
        logger.addHandler(file_handler)
    if gui_global_log_queue:
        logger.addHandler(queue_handler)

    logger.propagate = False

    return logger


def copy_sample_project_into_user_path(user_selected_parent_path: Path) -> str:
    """
    Copy the bundled sample_project directory into the user-selected parent path.

    The sample project is a ready-to-run Wikimedia EventStreams configuration
    that demonstrates the full pipeline without manual setup.

    Args:
        user_selected_parent_path: Parent directory where the sample project
            folder should be created (as wikimedia_eventstreams_live_excel_reports).

    Returns:
        Absolute path to the newly created sample project directory.

    Raises:
        FileExistsError: If the target directory already exists.
        OSError: If the copy operation fails due to filesystem errors.
    """
    sample_project_path = (
        user_selected_parent_path / "wikimedia_eventstreams_live_excel_reports"
    )

    try:
        shutil.copytree(
            "sample_project",
            sample_project_path,
            dirs_exist_ok=False,
        )
    except FileExistsError as error:
        raise FileExistsError(
            f"The folder {sample_project_path} already exists. Please remove it or select another parent folder."
        ) from error
    except OSError as error:
        raise OSError(
            f"Error copying sample project into user given path {user_selected_parent_path}:\n{error}"
        ) from error

    return str(sample_project_path)


with open("app_config.toml", "rb") as app_config_file:
    app_config: dict[str, Any] = tomllib.load(app_config_file)

