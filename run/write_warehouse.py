from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Iterable
import json
from json.decoder import JSONDecodeError
from threading import Event

import sseclient
import dlt
from dlt.sources.filesystem import filesystem
from dlt.pipeline.pipeline import Pipeline
from dlt.extract.resource import DltResource
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dbt.cli.main import dbtRunner

from run.utils import config


@dataclass
class WarehouseTransformer:
    sse_topics_metadata: dict[str, dict[str, str]]
    local_queues_base_paths: dict[str, Path]
    warehouse_path: Path

    @staticmethod
    @dlt.transformer(write_disposition="append")
    def read_binary_topics(
        event_files: list[FileItemDict],
        topic: str,  # pylint : disable=unused-argument
        primary_key: Iterable[str],
    ) -> list[dict]:
        records_list: list[dict] = []

        seen_records = 0
        seen_missing_primary_key_records = 0
        seen_incomplete_records = 0

        for event_file_dict in event_files:
            with event_file_dict.open("rb") as event_file:
                events = sseclient.SSEClient(event_file).events()

                while True:
                    try:
                        event = next(events)
                    except UnicodeDecodeError as error:
                        print(
                            f"ERROR: Decoding UTF-8 failed with file {event_file_dict["relative_path"]}"
                        )
                        print(error)
                        continue
                    except StopIteration:
                        break

                    seen_records += 1

                    try:
                        event_data = json.loads(event.data)
                        event_primary_key_missing = False

                        for primary_key_part in primary_key:
                            if not primary_key_part in event_data:
                                event_primary_key_missing = True
                                seen_missing_primary_key_records += 1

                                continue

                        if not event_primary_key_missing:
                            records_list.append(event_data)

                    except JSONDecodeError:
                        seen_incomplete_records += 1

        print(
            f"WARNING: Out of {seen_records} total records, \n\t{seen_missing_primary_key_records} marked as missing primary key,\n\t{seen_incomplete_records} marked as incomplete."
        )

        return records_list

    @cached_property
    def filesystem_resources(self) -> dict[str, DltResource]:
        filesystem_resources: dict[str, DltResource] = dict()

        for topic, _ in self.local_queues_base_paths.items():
            filesystem_resources[topic] = (
                filesystem(
                    bucket_url=str(self.local_queues_base_paths[topic]),
                    incremental=dlt.sources.incremental("modification_date"),
                    file_glob="*.bin",
                )
                | WarehouseTransformer.read_binary_topics(
                    topic, self.sse_topics_metadata[topic]["primary_key"]
                )  # pylint: disable=unsupported-binary-operation
            ).apply_hints(merge_key=self.sse_topics_metadata[topic]["primary_key"])

        return filesystem_resources

    @cached_property
    def load_pipeline(self) -> Pipeline:
        pipeline = dlt.pipeline(
            pipeline_name="bronze_stream",
            pipelines_dir=".dlt",
            destination=dlt.destinations.duckdb(str(self.warehouse_path)),
            dataset_name="bronze",
        )

        return pipeline

    @cached_property
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    def load_and_transform_once(self):
        for topic, filesystem_resource in self.filesystem_resources.items():
            self.load_pipeline.run(filesystem_resource, table_name=topic)

        self.dbt.invoke([])

    def load_and_transform_continuously(self, thread_stop_event: Event):
        while not thread_stop_event.is_set():
            self.load_and_transform_once()
            thread_stop_event.wait(config["app"]["load_and_transform_every_seconds"])
