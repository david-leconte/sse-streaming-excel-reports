from typing import Iterable
from pathlib import Path
import tomllib

def create_local_layers_dirs() -> Path:
    data_path = Path("data")
    data_path.mkdir(exist_ok=True)

    for layer in ["queues", "csv"]:
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


with open("run/config.toml", "rb") as f:
    run_config = tomllib.load(f)

with open("analytics/topics.toml", "rb") as f:
    topics_config = tomllib.load(f)