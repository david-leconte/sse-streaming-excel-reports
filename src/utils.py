import logging
from typing import Tuple, Iterable
from pathlib import Path
import tomllib
from tomllib import TOMLDecodeError
import os
import shutil
import argparse


def get_user_config_and_project_path() -> Tuple[str, dict[str, dict[str, str]], Path]:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.ERROR)

    parser = argparse.ArgumentParser()

    default_user_project_file_path = (
        Path(run_config["default_user_project_dir"]) / "topics.toml"
    )

    parser.add_argument(
        "user_project_file",
        nargs="?",
        default=str(default_user_project_file_path),
        help="Path to the user project file",
    )
    args = parser.parse_args()

    if not Path(args.user_project_file).exists():
        raise FileNotFoundError(
            f"Provided user project file path does not exist: {args.user_project_file}"
        )

    try:
        with open(args.user_project_file, "rb") as f:
            topics_config = tomllib.load(f)
    except (OSError, TOMLDecodeError) as error:
        raise RuntimeError(f"Loading user project file failed:\n{error}") from error
    
    user_project_path = Path(args.user_project_file).parent

    try:
        sse_api_base_url: str = topics_config["api"]["base_url"]
        sse_topics_metadata: dict[str, dict[str, str]] = topics_config["topics"]
    except KeyError as error:
        raise KeyError(
            "Provided user project file is missing required keys: 'api.base_url' and/or 'topics'"
        ) from error

    return sse_api_base_url, sse_topics_metadata, user_project_path


def propagate_dbt_files_to_user_path(user_project_path: Path):
    dbt_files_path = user_project_path / "dbt"

    try:
        dbt_files_path.mkdir(exist_ok=True)
    except OSError as error:
        raise OSError(
            f"Error creating dbt files directory in user project path {user_project_path}:\n{error}"
        ) from error

    try:
        shutil.copyfile("dbt/dbt_project.yml", str(dbt_files_path / "dbt_project.yml"))
        shutil.copyfile("dbt/profiles.yml", str(dbt_files_path / "profiles.yml"))
        shutil.copytree(
            "dbt/macros", str(user_project_path / "macros"), dirs_exist_ok=True
        )
    except OSError as error:
        raise OSError(
            f"Error copying dbt files to user project path {user_project_path}:\n{error}"
        ) from error


def create_local_files_dirs(user_project_path: Path) -> Path:
    try:
        os.makedirs(str(user_project_path / "logs"), exist_ok=True)
    except OSError as error:
        raise OSError(
            f"Error creating logs directory in user project path {user_project_path}:\n{error}"
        ) from error

    data_path = user_project_path / "data"

    try:
        data_path.mkdir(exist_ok=True)
    except OSError as error:
        raise OSError(
            f"Error creating data directory in user project path {user_project_path}:\n{error}"
        ) from error

    for layer in ["queues", "warehouse", "csv"]:
        layer_path = data_path / layer

        try:
            layer_path.mkdir(exist_ok=True)
        except OSError as error:
            raise OSError(
                f"Error creating {layer} directory in user project path {user_project_path}:\n{error}"
            ) from error

    return data_path


def create_local_queues_paths(
    queues_base_path: Path, topics: Iterable[str]
) -> dict[str, Path]:
    all_queues_basepaths = dict()

    for topic in topics:
        all_queues_basepaths[topic] = queues_base_path / f"{topic}"
        all_queues_basepaths[topic].mkdir(exist_ok=True)

    return all_queues_basepaths


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


with open("app_config.toml", "rb") as run_config_file:
    run_config = tomllib.load(run_config_file)
