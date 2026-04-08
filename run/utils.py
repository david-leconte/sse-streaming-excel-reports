from typing import Iterable, Callable
from pathlib import Path
import tomllib
import pyarrow as pa


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


def get_pyarrow_type_from_str(type_str: str) -> pa.DataType:
    pyarrow_type_module_attr = getattr(pa, type_str, None)

    if isinstance(pyarrow_type_module_attr, Callable) and isinstance(
        pyarrow_type_module_attr(), pa.DataType
    ):
        return pyarrow_type_module_attr()

    else:
        raise ValueError()


def get_pyarrow_schema_from_dict(schema_dict: dict[str, str]) -> pa.Schema:
    return pa.schema(
        [(key, get_pyarrow_type_from_str(value)) for key, value in schema_dict.items()]
    )


with open("run/config.toml", "rb") as f:
    run_config = tomllib.load(f)

with open("analytics/topics.toml", "rb") as f:
    topics_config = tomllib.load(f)
