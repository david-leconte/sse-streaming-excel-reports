import logging
from typing import Iterable
from pathlib import Path
import tomllib
import os
import shutil


def get_process_logger(user_project_path: Path, name: str):
    os.makedirs(str(user_project_path / "logs"), exist_ok=True)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s :\n%(message)s")
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


def propagate_dbt_files_to_user_path(user_project_path: Path):
    dbt_files_path = user_project_path / "dbt"
    dbt_files_path.mkdir(exist_ok=True)

    shutil.copyfile(
        "dbt/dbt_project.yml", str(dbt_files_path / "dbt_project.yml")
    )

    shutil.copyfile(
        "dbt/profiles.yml", str(dbt_files_path / "profiles.yml")
    )

    shutil.copytree(
        "dbt/macros", str(user_project_path / "macros"), dirs_exist_ok=True
    )


def create_local_files_dirs(user_project_path: Path) -> Path:
    data_path = user_project_path / "data"
    data_path.mkdir(exist_ok=True)

    for layer in ["queues", "warehouse", "csv"]:
        layer_path = data_path / layer
        layer_path.mkdir(exist_ok=True)

    return data_path


def create_local_queues_paths(
    queues_base_path: Path, topics: Iterable[str]
) -> dict[str, Path]:
    all_queues_basepaths = dict()

    for topic in topics:
        all_queues_basepaths[topic] = queues_base_path / f"{topic}"
        all_queues_basepaths[topic].mkdir(exist_ok=True)

    return all_queues_basepaths


with open("app_config.toml", "rb") as f:
    run_config = tomllib.load(f)

with open("run/topics.toml", "rb") as f:
    topics_config = tomllib.load(f)
