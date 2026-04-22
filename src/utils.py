import logging
from typing import Iterable
from pathlib import Path
import tomllib


def get_process_logger(name: str):
    formatter = logging.Formatter("%(asctime)s - %(levelname)s :\n%(message)s")
    filename = f"{name.translate(str.maketrans(".- ", "___")).lower()}.log"

    file_handler = logging.FileHandler(f"logs/{filename}")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.propagate = False

    return logger


def create_local_files_dirs() -> Path:
    data_path = Path("run/data")
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


with open("config/app.toml", "rb") as f:
    run_config = tomllib.load(f)

with open("run/topics.toml", "rb") as f:
    topics_config = tomllib.load(f)
