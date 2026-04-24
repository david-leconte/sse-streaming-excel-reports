import logging
from multiprocessing import Process
from typing import Tuple
from pathlib import Path
import tomllib
from tomllib import TOMLDecodeError
import os
import shutil

from src.write_topics import TopicQueuesAsyncWriter
from src.write_warehouse import WarehouseTransformer


class Run:
    def __init__(self, user_project_dir_input: str):
        self._logger = logging.getLogger("app")
        self._logger.setLevel(logging.ERROR)

        self._user_project_dir_input: str = user_project_dir_input

        try:
            self._sse_api_base_url: str
            self._sse_topics_metadata: dict[str, dict[str, str]]
            self._user_project_path: Path

            (
                self._sse_api_base_url,
                self._sse_topics_metadata,
                self._user_project_path,
            ) = self.get_user_config_and_project_path()
        except (ValueError, FileNotFoundError, KeyError, RuntimeError):
            self._logger.exception(
                "Error getting user config and project path. Exiting...",
            )
            exit()

        try:
            self.propagate_dbt_files_to_user_path()
        except OSError:
            self._logger.exception(
                "Error propagating dbt files to user path. Exiting...",
            )
            exit()

        try:
            self._data_path: Path = self.create_local_files_dirs()
        except OSError:
            self._logger.exception(
                "Error creating local files directories. Exiting...",
            )
            exit()

        self._all_queues_basepaths: dict[str, Path] = self.create_local_queues_paths()

    def get_user_config_and_project_path(
        self,
    ) -> Tuple[str, dict[str, dict[str, str]], Path]:
        if not Path(self._user_project_dir_input).exists():
            raise FileNotFoundError(
                f"Provided user project directory path does not exist: {self._user_project_dir_input}"
            )

        user_project_file = Path(self._user_project_dir_input) / "topics.toml"

        if not Path(user_project_file).exists():
            raise FileNotFoundError(
                "No topics.toml file found in provided user project directory path"
            )

        try:
            with open(user_project_file, "rb") as f:
                topics_config = tomllib.load(f)
        except (OSError, TOMLDecodeError) as error:
            raise RuntimeError(f"Loading user project file failed:\n{error}") from error

        user_project_path = Path(user_project_file).parent

        try:
            sse_api_base_url = topics_config["api"]["base_url"]
            sse_topics_metadata = topics_config["topics"]
        except KeyError as error:
            raise KeyError(
                "Provided user project file is missing required keys: 'api.base_url' and/or 'topics'"
            ) from error

        return sse_api_base_url, sse_topics_metadata, user_project_path

    def propagate_dbt_files_to_user_path(self):
        dbt_files_path = self._user_project_path / "dbt"

        try:
            dbt_files_path.mkdir(exist_ok=True)
        except OSError as error:
            raise OSError(
                f"Error creating dbt files directory in user project path {self._user_project_path}:\n{error}"
            ) from error

        try:
            shutil.copyfile(
                "dbt/dbt_project.yml", str(dbt_files_path / "dbt_project.yml")
            )
            shutil.copyfile("dbt/profiles.yml", str(dbt_files_path / "profiles.yml"))
            shutil.copytree(
                "dbt/macros",
                str(self._user_project_path / "macros"),
                dirs_exist_ok=True,
            )
        except OSError as error:
            raise OSError(
                f"Error copying dbt files to user project path {self._user_project_path}:\n{error}"
            ) from error

    def create_local_files_dirs(self) -> Path:
        try:
            os.makedirs(str(self._user_project_path / "logs"), exist_ok=True)
        except OSError as error:
            raise OSError(
                f"Error creating logs directory in user project path {self._user_project_path}:\n{error}"
            ) from error

        data_path = self._user_project_path / "data"

        try:
            data_path.mkdir(exist_ok=True)
        except OSError as error:
            raise OSError(
                f"Error creating data directory in user project path {self._user_project_path}:\n{error}"
            ) from error

        for layer in ["queues", "warehouse", "csv"]:
            layer_path = data_path / layer

            try:
                layer_path.mkdir(exist_ok=True)
            except OSError as error:
                raise OSError(
                    f"Error creating {layer} directory in user project path {self._user_project_path}:\n{error}"
                ) from error

        return data_path

    def create_local_queues_paths(self) -> dict[str, Path]:
        all_queues_basepaths = dict()

        for topic in self._sse_topics_metadata:
            all_queues_basepaths[topic] = self._data_path / "queues" / f"{topic}"
            all_queues_basepaths[topic].mkdir(exist_ok=True)

        return all_queues_basepaths

    def launch(self):
        topics_writer_process = Process(
            target=TopicQueuesAsyncWriter.build_and_run_topic_queues_writer,
            args=(
                self._sse_api_base_url,
                self._sse_topics_metadata,
                self._user_project_path,
                self._all_queues_basepaths,
            ),
            daemon=True,
        )

        warehouse_transformer_process = Process(
            target=WarehouseTransformer.build_and_run_continuously_warehouse_transformer,
            args=(
                self._sse_topics_metadata,
                self._user_project_path,
                self._all_queues_basepaths,
            ),
            daemon=True,
        )

        topics_writer_process.start()
        warehouse_transformer_process.start()

        try:
            while (
                topics_writer_process.is_alive()
                and warehouse_transformer_process.is_alive()
            ):
                pass
        except KeyboardInterrupt:
            self._logger.setLevel(logging.INFO)
            self._logger.info("Keyboard interrupt received. Terminating processes...")

            topics_writer_process.terminate()
            warehouse_transformer_process.terminate()

            exit()
