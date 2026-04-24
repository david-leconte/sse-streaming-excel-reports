import logging
from logging.handlers import QueueHandler
from pathlib import Path
import argparse
import shutil
import tomllib
from typing import Any
from multiprocessing.queues import Queue


def get_user_project_dir_cli_input() -> str | None:
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
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    if user_project_path:
        filename = f"{name.translate(str.maketrans(".- ", "___")).lower()}.log"
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
