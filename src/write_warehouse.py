"""
WarehouseTransformer: Loads SSE events from local queues, transforms with dbt, outputs CSV.

This module defines the WarehouseTransformer class that implements the ETL (Extract-Transform-Load)
pipeline of the application. It:

1. Attaches to a DuckDB database configured as a DuckLake warehouse
   (metadata in SQLite, data files in a separate directory).
2. Discovers new .bin queue files per topic (based on a "newest file loaded" log).
3. Parses SSE events from those files, filters records missing the configured
   primary key, and loads raw JSON into a bronze.{topic}_raw table.
4. Invokes dbt to run the medallion transformation (bronze → silver → gold).
5. Periodically exports gold layer tables to CSV files in <project>/data/csv/.

The transformer is designed to run continuously in a loop, with configurable
intervals for ETL execution and CSV export.

Dependencies:
    duckdb, dbt-core, dbt-duckdb, sseclient-py
"""

from pathlib import Path
import json
from json.decoder import JSONDecodeError
import time
import os
from datetime import datetime
from multiprocessing.queues import Queue

import sseclient
import duckdb
from duckdb import DuckDBPyConnection
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.artifacts.schemas.run import RunExecutionResult

from src.utils import app_config, get_process_logger


class WarehouseTransformer:
    """
    Manages the ETL pipeline: ingest local SSE queue files → DuckDB bronze → dbt → CSV.

    The transformer runs in two modes:
        - load_and_transform_once(): Single ETL cycle (used in tests / one-shot runs)
        - load_and_transform_continuously(): Infinite loop with configurable sleep

    Architecture:
        Bronze layer: Raw JSON events loaded into bronze.{topic}_raw tables.
        Silver layer: Parsed and normalized by dbt models (defined in sample_project/).
        Gold layer: Aggregated/report-ready tables exported to CSV.

    Multiprocessing:
        Instantiated and run in a separate Process by the Run orchestrator;
        logging can be forwarded to the parent via a multiprocessing.Queue.
    """

    def __init__(
        self,
        sse_topics_metadata: dict[str, dict[str, str | dict[str, str] | list[str]]],
        user_project_path: Path,
        local_queues_base_paths: dict[str, Path],
        gui_global_log_queue: Queue | None = None,
    ):
        """
        Initialize the WarehouseTransformer.

        Args:
            sse_topics_metadata: Mapping of topic names to their configuration
                (at minimum: "path" and "primary_key" entries).
            user_project_path: Absolute path to the user's project directory.
            local_queues_base_paths: Mapping of topic name → directory path where
                .bin queue files for that topic are stored.
            gui_global_log_queue: Optional multiprocessing.Queue for forwarding
                log records to a GUI (unused if running headless).
        """
        self._sse_topics_metadata = sse_topics_metadata
        self._user_project_path = user_project_path
        self._local_queues_base_paths = local_queues_base_paths

        self._app_cwd_path = Path.cwd()
        self._duckdb_conn: DuckDBPyConnection = self._attach_warehouse()
        self._dbt = dbtRunner()

        self._topics_bronze_table_exist: dict[str, bool] = (
            self._check_topics_bronze_table_exist()
        )

        self._logger = get_process_logger(
            __name__, user_project_path, gui_global_log_queue
        )

    def _attach_warehouse(self) -> DuckDBPyConnection:
        """
        Attach to the DuckDB DuckLake warehouse (SQLite metadata + data files).

        Returns:
            A connected DuckDBPyConnection instance with the warehouse attached
            and the bronze schema created.

        Raises:
            duckdb.Error: If the ATTACH statement fails.
        """
        duckdb_conn = duckdb.connect()

        warehouse_metadata_abs_path = (
            self._user_project_path / "data/warehouse/warehouse.sqlite"
        ).resolve()
        warehouse_data_abs_path = (
            self._user_project_path / "data/warehouse/data_files"
        ).resolve()

        duckdb_conn.sql(f"""
            INSTALL ducklake;
            INSTALL sqlite;
            ATTACH 
                'ducklake:sqlite:{warehouse_metadata_abs_path}' 
                AS warehouse 
                (
                    DATA_PATH '{warehouse_data_abs_path}',
                    OVERRIDE_DATA_PATH true,
                    AUTOMATIC_MIGRATION true
                );
            USE warehouse;
            CREATE SCHEMA IF NOT EXISTS bronze;
            """)

        return duckdb_conn

    def _check_topics_bronze_table_exist(self) -> dict[str, bool]:
        """
        Scan existing bronze tables and mark which topics already have a raw table.

        Returns:
            Dictionary mapping topic names → True if bronze.{topic}_raw exists.
        """
        topics_bronze_table_exist = dict()

        bronze_tables_records = self._duckdb_conn.sql(
            "SHOW TABLES FROM bronze;"
        ).fetchall()

        for bronze_table_record in bronze_tables_records:
            bronze_table_name: str = bronze_table_record[0]

            if bronze_table_name.endswith("_raw"):
                topic_name = bronze_table_name.removesuffix("_raw")

                if topic_name in self._sse_topics_metadata:
                    topics_bronze_table_exist[topic_name] = True

        for topic in self._sse_topics_metadata.keys():
            if not topic in topics_bronze_table_exist:
                topics_bronze_table_exist[topic] = False

        return topics_bronze_table_exist

    def _get_complete_files_per_topic(self) -> dict[str, list[Path]]:
        """
        List all .bin queue files for each topic, sorted by filename.

        Returns:
            Mapping of topic name → sorted list of .bin file paths.
        """
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
        """
        Determine which queue files are new (unprocessed) for each topic.

        Uses a "{topic}_newest_file_loaded_log.txt" file in the queue's parent
        directory to track the last processed file. All newer files are returned.

        Returns:
            Mapping of topic name → list of new .bin file Paths.
        """
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
        """
        Parse SSE events from new queue files and insert into bronze.{topic}_raw.

        For each event in each new file:
            - Attempt to parse event.data as JSON
            - Skip records missing any primary key field (counted in logger)
            - Append valid JSON strings to a bulk INSERT batch
        After processing each file, the "newest file loaded" log is updated.
        The bronze table is created if it does not already exist.

        Args:
            topic: Topic name as defined in topics.toml.

        Returns:
            Total number of SSE events seen across all new files for this topic.

        Raises:
            No exceptions are raised; records causing JSONDecodeError or
            missing primary keys are counted and skipped.
        """
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
                    except UnicodeDecodeError:
                        self._logger.exception(
                            "Decoding UTF-8 failed on topic %s with file %s",
                            topic,
                            event_file_path.name,
                        )
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
            "Out of %s total records, %s marked as missing primary key, %s marked as incomplete.",
            seen_dicts,
            seen_missing_primary_key_dicts,
            seen_incomplete_dicts,
        )

        events_data_str = "[" + ",".join(events_data_str_list) + "]"

        if not self._topics_bronze_table_exist[topic]:
            self._duckdb_conn.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze.{topic}_raw (event JSON);
                """)

            self._topics_bronze_table_exist[topic] = True

        self._duckdb_conn.execute(
            f"INSERT INTO bronze.{topic}_raw SELECT UNNEST(CAST(? AS JSON[])) AS event;",
            [events_data_str],
        )

        return seen_dicts

    def load_and_transform_once(self):
        """
        Run a single ETL cycle: load all topics' new events and run dbt.

        Does not output CSV. See output_gold_layer_csv() and
        load_and_transform_continuously() for periodic CSV export.
        """
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

    def output_gold_layer_csv(self):
        """Export all gold layer tables to CSV in <project>/data/csv/."""
        gold_tables_records = self._duckdb_conn.sql(
            "SHOW TABLES FROM warehouse.gold;"
        ).fetchall()

        for gold_table_record in gold_tables_records:
            gold_table_name = gold_table_record[0]

            gold_table_csv_path = (
                self._user_project_path / f"data/csv/{gold_table_name}.csv"
            )

            self._duckdb_conn.sql(
                f"COPY warehouse.gold.{gold_table_name} TO '{gold_table_csv_path}' (FORMAT CSV, HEADER, DELIM ';')"
            )

    def load_and_transform_continuously(self):
        """
        Run ETL cycles and CSV exports in an infinite loop.

        Sleeps between cycles according to app_config["load_and_transform_every_seconds"].
        Exports gold-layer CSV every app_config["output_gold_csv_every_seconds"].
        Intended to run as a daemon process; terminates only on exception or process kill.
        """
        last_csv_output_datetime = datetime.now()

        while True:
            self.load_and_transform_once()

            if (datetime.now() - last_csv_output_datetime).seconds > (
                app_config["output_gold_csv_every_seconds"]
            ):
                last_csv_output_datetime = datetime.now()
                self.output_gold_layer_csv()

            time.sleep(app_config["load_and_transform_every_seconds"])

    @staticmethod
    def build_and_run_continuously_warehouse_transformer(
        sse_topics_metadata: dict[str, dict[str, str | dict[str, str] | list[str]]],
        user_project_path: Path,
        local_queues_base_paths: dict[str, Path],
        gui_global_log_queue: Queue | None = None,
    ):
        """
        Factory method to instantiate and run the transformer in a new process.

        Designed as a multiprocessing entry point. Catches all exceptions,
        logs them, and exits with status 1 on failure.

        Args:
            sse_topics_metadata: Topics configuration dictionary.
            user_project_path: User project root path.
            local_queues_base_paths: Per-topic queue directory mapping.
            gui_global_log_queue: Optional log-forwarding queue for the GUI.
        """
        try:
            warehouse_transformer = WarehouseTransformer(
                sse_topics_metadata,
                user_project_path,
                local_queues_base_paths,
                gui_global_log_queue,
            )
            warehouse_transformer.load_and_transform_continuously()
        except Exception:  # pylint: disable=broad-except
            logger = get_process_logger(
                __name__, user_project_path, gui_global_log_queue
            )
            logger.exception("Error in warehouse transformer, shutting down process...")
            exit()
