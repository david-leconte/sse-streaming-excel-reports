from pathlib import Path
import json
from json.decoder import JSONDecodeError
import time
import os

import sseclient
import pyarrow as pa
import duckdb
from duckdb import DuckDBPyConnection
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.artifacts.schemas.run import RunExecutionResult

from src.utils import get_process_logger, run_config


class WarehouseTransformer:
    def __init__(
        self,
        sse_topics_metadata: dict[str, dict[str, str | dict[str, str]]],
        user_project_path: Path,
        local_queues_base_paths: dict[str, Path],
    ):
        self._sse_topics_metadata = sse_topics_metadata
        self._user_project_path = user_project_path
        self._local_queues_base_paths = local_queues_base_paths

        self._app_cwd_path = Path.cwd()
        self._duckdb_conn: DuckDBPyConnection = self._attach_warehouse()
        self._dbt = dbtRunner()

        self._topics_bronze_table_exist: dict[str, bool] = (
            self._check_topics_bronze_table_exist()
        )

        self._logger = get_process_logger(user_project_path, __name__)

    def _attach_warehouse(self) -> DuckDBPyConnection:
        duckdb_conn = duckdb.connect()

        warehouse_metadata_abs_path = (
            self._user_project_path / "data/warehouse/warehouse.sqlite"
        ).resolve()
        warehouse_data_abs_path = (
            self._user_project_path / "data/warehouse/data_files"
        ).resolve()

        duckdb_conn.execute(
            f"""
            INSTALL ducklake;
            INSTALL sqlite;
            ATTACH 
                'ducklake:sqlite:{warehouse_metadata_abs_path}' 
                AS warehouse 
                (
                    DATA_PATH '{warehouse_data_abs_path}',
                    AUTOMATIC_MIGRATION true
                );
            USE warehouse;
            """
        )

        return duckdb_conn

    def _check_topics_bronze_table_exist(self) -> dict[str, bool]:

        topics_bronze_table_exist = dict()

        for topic, _ in self._sse_topics_metadata.items():
            query = f"""
                SELECT 
                    COUNT(*) 
                FROM 
                    information_schema.tables 
                WHERE 
                    table_schema = 'bronze' AND 
                    table_name = '{topic}_raw'
                """

            information_schema_table_record = self._duckdb_conn.sql(query).fetchone()

            topics_bronze_table_exist[topic] = (
                information_schema_table_record is not None
                and information_schema_table_record[0] > 0
            )

        return topics_bronze_table_exist

    def _get_complete_files_per_topic(self) -> dict[str, list[Path]]:
        complete_files_list_per_topic = dict()

        for topic, _ in self._sse_topics_metadata.items():
            topic_path = self._local_queues_base_paths[topic]
            complete_files_list: list[Path] = sorted(
                list(topic_path.glob("*.bin")), key=lambda path: path.name
            )
            complete_files_list_per_topic[topic] = complete_files_list

        return complete_files_list_per_topic

    @property
    def _topics_new_files_lists(self) -> dict[str, list[Path]]:
        topics_complete_files_lists = self._get_complete_files_per_topic()

        new_files_list_per_topic = dict()

        for topic, _ in self._sse_topics_metadata.items():
            log_newest_file_loaded_path = (
                self._local_queues_base_paths[topic].parent
                / f"{topic}_newest_file_loaded_log.txt"
            )

            if not log_newest_file_loaded_path.exists():
                new_files_list_per_topic[topic] = topics_complete_files_lists[topic]
                continue

            if not topic in new_files_list_per_topic:
                new_files_list_per_topic[topic] = []

            with open(
                str(log_newest_file_loaded_path), "r", encoding="utf-8"
            ) as log_newest_file_loaded_file:
                newest_file_loaded_path = (
                    self._local_queues_base_paths[topic]
                    / log_newest_file_loaded_file.read()
                )

            for current_file_path in topics_complete_files_lists[topic]:
                if current_file_path > newest_file_loaded_path:
                    new_files_list_per_topic[topic].append(current_file_path)

        return new_files_list_per_topic

    def _load_topic(
        self,
        topic: str,
    ) -> int:
        primary_key = self._sse_topics_metadata[topic]["primary_key"]

        events_data_str_list: list[str] = []

        seen_dicts = 0
        seen_missing_primary_key_dicts = 0
        seen_incomplete_dicts = 0

        if not self._topics_new_files_lists[topic]:
            return 0

        for event_file_path in self._topics_new_files_lists[topic]:
            with open(event_file_path, "rb") as event_file:
                events = sseclient.SSEClient(event_file).events()

                while True:
                    try:
                        event = next(events)
                    except UnicodeDecodeError as error:
                        self._logger.error(
                            "Decoding UTF-8 failed on topic %s with file %s",
                            topic,
                            event_file_path.name,
                        )
                        self._logger.exception(error)
                        continue
                    except StopIteration:
                        break

                    seen_dicts += 1

                    try:
                        event_data_dict = json.loads(event.data)
                        event_primary_key_missing = False

                        for primary_key_part in primary_key:
                            if not primary_key_part in event_data_dict:
                                event_primary_key_missing = True
                                seen_missing_primary_key_dicts += 1

                                continue

                        if not event_primary_key_missing:
                            events_data_str_list.append(event.data)

                    except JSONDecodeError:
                        seen_incomplete_dicts += 1

            log_newest_file_loaded_path = (
                self._local_queues_base_paths[topic].parent
                / f"{topic}_newest_file_loaded_log.txt"
            )

            with open(
                str(log_newest_file_loaded_path),
                "w",
                encoding="utf-8",
            ) as log_newest_file_loaded_file:
                log_newest_file_loaded_file.write(event_file_path.name)

        log_func = (
            self._logger.warning
            if seen_missing_primary_key_dicts > 0 or seen_incomplete_dicts > 0
            else self._logger.info
        )
        log_func(
            "Out of %s total records, \n\t%s marked as missing primary key,\n\t%s marked as incomplete.",
            seen_dicts,
            seen_missing_primary_key_dicts,
            seen_incomplete_dicts,
        )

        events_arrow_array = pa.array(events_data_str_list, type=pa.json_(pa.utf8()))
        events_arrow_table = pa.table({"event": events_arrow_array})
        self._duckdb_conn.register("events_arrow", events_arrow_table)

        if not self._topics_bronze_table_exist[topic]:
            self._duckdb_conn.sql(
                f"""
                CREATE SCHEMA IF NOT EXISTS bronze;
                CREATE TABLE IF NOT EXISTS bronze.{topic}_raw (event JSON);
                """
            )

            self._topics_bronze_table_exist[topic] = True

        self._duckdb_conn.sql(
            f"INSERT INTO bronze.{topic}_raw SELECT * FROM events_arrow;"
        )

        return seen_dicts

    def load_and_transform_once(self):
        total_records_loaded = 0

        for topic in self._sse_topics_metadata.keys():
            seen_records = self._load_topic(topic)
            total_records_loaded += seen_records

        if seen_records < 1:
            return

        os.chdir(self._user_project_path)
        dbt_result: dbtRunnerResult = self._dbt.invoke(
            [
                "run",
                "--project-dir",
                "dbt",
                "--profiles-dir",
                "dbt",
                "--target-path",
                "target",
                "--log-path",
                "logs",
                "--log-level",
                "none",
                "--fail-fast",
            ]
        )
        os.chdir(self._app_cwd_path)

        self._logger.info(
            "%s records loaded and transformed with dbt models.", total_records_loaded
        )

        if dbt_result.exception:
            raise dbt_result.exception

        elif not dbt_result.success:
            if isinstance(dbt_result.result, RunExecutionResult):
                messages = "".join(
                    [f"{result.message}\n" for result in dbt_result.result.results]
                )

                raise RuntimeError(f"dbt run failed without exception:\n{messages}")

            else:
                raise RuntimeError(
                    "dbt run failed without exception nor detailed result."
                )

    def load_and_transform_continuously(self):
        while True:
            self.load_and_transform_once()

            time.sleep(run_config["load_and_transform_every_seconds"])

    @staticmethod
    def build_and_run_continuously_warehouse_transformer(
        sse_topics_metadata: dict[str, dict[str, str | dict[str, str]]],
        user_project_path: Path,
        local_queues_base_paths: dict[str, Path],
    ):
        try:
            warehouse_transformer = WarehouseTransformer(
                sse_topics_metadata,
                user_project_path,
                local_queues_base_paths,
            )
            warehouse_transformer.load_and_transform_continuously()
        except Exception as error:  # pylint: disable=broad-except
            logger = get_process_logger(user_project_path, __name__)
            logger.exception(error)
            exit()
