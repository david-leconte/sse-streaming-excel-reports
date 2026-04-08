from pathlib import Path
from typing import Any
import json
from json.decoder import JSONDecodeError
import time

import sseclient
import pandas as pd
import duckdb
from dbt.cli.main import dbtRunner

from run.utils import run_config


class WarehouseTransformer:
    def __init__(
        self,
        sse_topics_metadata: dict[str, dict[str, str]],
        local_queues_base_paths: dict[str, Path],
        warehouse_path: Path,
    ):
        self._sse_topics_metadata = sse_topics_metadata
        self._local_queues_base_paths = local_queues_base_paths
        self._warehouse_path = warehouse_path

        self._duckdb_conn = duckdb.connect(str(self._warehouse_path))
        self._dbt = dbtRunner()

        self._topics_bronze_table_exist: dict[str, bool] = (
            self._check_topics_bronze_table_exist()
        )

        self._topics_complete_files_lists: dict[str, list[Path]] = (
            self._get_complete_files_per_topic()
        )

        self._topics_new_files_lists: dict[str, list[Path]] = (
            self._get_new_files_per_topic()
        )

    def _check_topics_bronze_table_exist(self) -> dict[str, bool]:
        topics_bronze_table_exist = dict()

        for topic, _ in self._sse_topics_metadata.items():
            information_schema_table_record = self._duckdb_conn.sql(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{topic}'"
            ).fetchone()

            topics_bronze_table_exist[topic] = (
                information_schema_table_record is not None
                and information_schema_table_record[0] > 0
            )

        return topics_bronze_table_exist

    def _get_complete_files_per_topic(self) -> dict[str, list[Path]]:
        complete_files_list_per_topic = dict()

        for topic, _ in self._sse_topics_metadata.items():
            topic_path = Path("data/queues") / topic
            complete_files_list: list[Path] = sorted(
                list(topic_path.glob("*.bin")), key=lambda path: path.name
            )
            complete_files_list_per_topic[topic] = complete_files_list

        return complete_files_list_per_topic

    def _get_new_files_per_topic(self):
        new_files_list_per_topic = dict()

        for topic, _ in self._sse_topics_metadata.items():
            queues_path = Path("data/queues")
            newest_file_loaded_log_path = (
                queues_path / f"{topic}_newest_file_loaded_log.txt"
            )

            if not newest_file_loaded_log_path.exists():
                new_files_list_per_topic[topic] = self._topics_complete_files_lists[
                    topic
                ]
                continue

            if not topic in new_files_list_per_topic:
                new_files_list_per_topic[topic] = []

            with open(
                str(newest_file_loaded_log_path), "r", encoding="utf-8"
            ) as newest_file_loaded_log_file:
                newest_file_loaded_path = (
                    Path("data/queues")
                    / topic
                    / newest_file_loaded_log_file.read()
                )

            for current_file_path in self._topics_complete_files_lists[topic]:
                if current_file_path > newest_file_loaded_path:
                    new_files_list_per_topic[topic].append(current_file_path)

        return new_files_list_per_topic

    def _load_topic(
        self,
        topic: str,
    ):
        primary_key = self._sse_topics_metadata[topic]["primary_key"]

        events_dicts: list[dict[str, Any]] = []

        seen_dicts = 0
        seen_missing_primary_key_dicts = 0
        seen_incomplete_dicts = 0

        if not self._topics_new_files_lists[topic]:
            return

        for event_file_path in self._topics_new_files_lists[topic]:
            with open(event_file_path, "rb") as event_file:
                events = sseclient.SSEClient(event_file).events()

                while True:
                    try:
                        event = next(events)
                    except UnicodeDecodeError as error:
                        print(
                            f"ERROR: Decoding UTF-8 failed on topic {topic } with file {event_file_path.name}"
                        )
                        print(error)
                        continue
                    except StopIteration:
                        break

                    seen_dicts += 1

                    try:
                        event_data = json.loads(event.data)
                        event_primary_key_missing = False

                        for primary_key_part in primary_key:
                            if not primary_key_part in event_data:
                                event_primary_key_missing = True
                                seen_missing_primary_key_dicts += 1

                                continue

                        if not event_primary_key_missing:
                            events_dicts.append(event_data)

                    except JSONDecodeError:
                        seen_incomplete_dicts += 1

            queues_path = Path("data/queues")
            newest_file_loaded_log_path = (
                queues_path / f"{topic}_newest_file_loaded_log.txt"
            )

            with open(
                str(newest_file_loaded_log_path),
                "w",
                encoding="utf-8",
            ) as newest_file_loaded_log_file:
                newest_file_loaded_log_file.write(event_file_path.name)

        # print(
        #     f"WARNING: Out of {seen_dicts} total records, \n\t{seen_missing_primary_key_dicts} marked as missing primary key,\n\t{seen_incomplete_dicts} marked as incomplete."
        # )

        events_df = pd.DataFrame(events_dicts)  # pylint : ignore=unused-variable

        if not self._topics_bronze_table_exist[topic]:
            self._duckdb_conn.sql(
                f"""CREATE SCHEMA IF NOT EXISTS bronze;
                CREATE TABLE IF NOT EXISTS bronze.{topic} AS SELECT * FROM events_df;"""
            )

        else:
            self._duckdb_conn.sql(
                f"INSERT INTO bronze.{topic} SELECT * FROM events_df;"
            )

    def load_and_transform_once(self):
        for topic in self._sse_topics_metadata.keys():
            self._load_topic(topic)

        self._dbt.invoke(
            ["run", "--profiles-dir", run_config["dbt_profile_dir"], "--quiet"]
        )

    def load_and_transform_continuously(self):
        while True:
            self.load_and_transform_once()

            time.sleep(run_config["load_and_transform_every_seconds"])

    @staticmethod
    def build_and_run_continuously_warehouse_transformer(
        sse_topics_metadata: dict[str, dict[str, str]],
        local_queues_base_paths: dict[str, Path],
        warehouse_path: Path,
    ):
        warehouse_transformer = WarehouseTransformer(
            sse_topics_metadata, local_queues_base_paths, warehouse_path
        )
        warehouse_transformer.load_and_transform_continuously()
