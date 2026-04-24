import logging
from pathlib import Path
import argparse
import tomllib
from typing import Any


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


def get_process_logger(user_project_path: Path, name: str):
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")
    filename = f"{name.translate(str.maketrans(".- ", "___")).lower()}.log"

    file_handler = logging.FileHandler(
        filename=str(user_project_path / "logs" / filename), encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.propagate = False

    return logger


with open("app_config.toml", "rb") as app_config_file:
    app_config: dict[str, Any] = tomllib.load(app_config_file)
