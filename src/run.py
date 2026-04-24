import logging
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import Tuple
from pathlib import Path
import tomllib
from tomllib import TOMLDecodeError
import os
import shutil

from src.utils import get_process_logger
from src.write_topics import TopicQueuesAsyncWriter
from src.write_warehouse import WarehouseTransformer


class Run:
    def __init__(
        self,
        user_project_dir_input: str,
        is_gui_run: bool = False,
        gui_global_log_queue: Queue | None = None,
    ):
        self._gui_global_log_queue = gui_global_log_queue
        self._logger = get_process_logger(
            __name__, gui_global_log_queue=self._gui_global_log_queue
        )
        self._logger.setLevel(logging.ERROR)

        self._user_project_dir_input: str = user_project_dir_input
        self._is_gui_run = is_gui_run

        try:
            self._sse_api_base_url: str
            self._sse_topics_metadata: dict[str, dict[str, str]]
            self._user_project_path: Path

            (
                self._sse_api_base_url,
                self._sse_topics_metadata,
                self._user_project_path,
            ) = self._get_user_config_and_project_path()

            self._propagate_dbt_files_to_user_path()

            self._data_path: Path = self._create_local_files_dirs()

        except (ValueError, FileNotFoundError, KeyError, RuntimeError, OSError):
            self._logger.exception("Error encountered during project setup :")

            if not self._is_gui_run:
                self._logger.setLevel(logging.INFO)
                self._logger.info(
                    "Exiting...",
                )
                exit()

        self._all_queues_basepaths: dict[str, Path] = self._create_local_queues_paths()

        self._topics_writer_process: Process | None = None
        self._warehouse_transformer_process: Process | None = None

        self.is_active = False

    def _get_user_config_and_project_path(
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

    def _propagate_dbt_files_to_user_path(self):
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

    def _create_local_files_dirs(self) -> Path:
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

    def _create_local_queues_paths(self) -> dict[str, Path]:
        all_queues_basepaths = dict()

        for topic in self._sse_topics_metadata:
            all_queues_basepaths[topic] = self._data_path / "queues" / f"{topic}"
            all_queues_basepaths[topic].mkdir(exist_ok=True)

        return all_queues_basepaths

    def terminate(self):
        if not self._topics_writer_process or not self._warehouse_transformer_process:
            self._logger.error("Processes have not been started yet")

        if self._topics_writer_process:
            self._topics_writer_process.terminate()

        if self._warehouse_transformer_process:
            self._warehouse_transformer_process.terminate()

        self.is_active = False

    def launch(self):
        self._topics_writer_process = Process(
            target=TopicQueuesAsyncWriter.build_and_run_topic_queues_writer,
            args=(
                self._sse_api_base_url,
                self._sse_topics_metadata,
                self._user_project_path,
                self._all_queues_basepaths,
                self._gui_global_log_queue,
            ),
            daemon=True,
        )

        self._warehouse_transformer_process = Process(
            target=WarehouseTransformer.build_and_run_continuously_warehouse_transformer,
            args=(
                self._sse_topics_metadata,
                self._user_project_path,
                self._all_queues_basepaths,
                self._gui_global_log_queue,
            ),
            daemon=True,
        )

        self._topics_writer_process.start()
        self._warehouse_transformer_process.start()

        self.is_active = True

        if not self._is_gui_run:
            try:
                while (
                    self._topics_writer_process.is_alive()
                    and self._warehouse_transformer_process.is_alive()
                ):
                    pass

            except KeyboardInterrupt:
                self._logger.setLevel(logging.INFO)
                self._logger.info(
                    "Keyboard interrupt received. Exiting..."
                )

                self.terminate()
                exit()
