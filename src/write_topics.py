from typing import Tuple
from pathlib import Path
from datetime import datetime
import asyncio
from asyncio import IncompleteReadError

from aiofiles.threadpool.binary import AsyncBufferedIOBase
import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientPayloadError

from src.utils import get_process_logger, run_config


class TopicQueuesAsyncWriter:
    def __init__(self, user_project_path: Path):
        self._logger = get_process_logger(user_project_path, __name__)

    @staticmethod
    async def _get_new_topic_queue_file(
        topic: str, local_queues_basepaths: dict[str, Path]
    ) -> Tuple[datetime, AsyncBufferedIOBase]:
        topic_last_queue_file_datetime = datetime.now()
        topic_last_queue_full_path = (
            local_queues_basepaths[topic]
            / f"{str(topic_last_queue_file_datetime.timestamp())}.bin"
        )
        topic_last_queue_fp = await aiofiles.open(topic_last_queue_full_path, "wb")

        return topic_last_queue_file_datetime, topic_last_queue_fp

    async def write_topic_local_queue(
        self,
        topic: str,
        base_url: str,
        sse_topics_metadata: dict[str, dict[str, str]],
        queues_base_paths: dict[str, Path],
    ):
        last_queue_file_datetime: datetime | None = None
        last_queue_file: AsyncBufferedIOBase | None = None

        timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

        while True:
            try:
                async with (
                    aiohttp.ClientSession(timeout=timeout) as session,
                    session.get(
                        base_url + "/" + sse_topics_metadata[topic]["path"],
                        headers={"User-Agent": run_config["user_agent"]},
                    ) as resp,
                ):
                    while True:
                        try:
                            current_contents = await resp.content.readuntil(
                                separator=b"\n\n"
                            )
                        except IncompleteReadError:
                            print("ERROR: streaming request error, incomplete read")
                            break

                        if not last_queue_file or not last_queue_file_datetime:
                            last_queue_file_datetime, last_queue_file = (
                                await TopicQueuesAsyncWriter._get_new_topic_queue_file(
                                    topic, queues_base_paths
                                )
                            )

                        if (
                            datetime.now() - last_queue_file_datetime
                        ).seconds > run_config[
                            "topic_new_queue_file_seconds_threshold"
                        ]:
                            await last_queue_file.close()
                            last_queue_file_datetime, last_queue_file = (
                                await TopicQueuesAsyncWriter._get_new_topic_queue_file(
                                    topic, queues_base_paths
                                )
                            )

                        await last_queue_file.write(current_contents)

            except ClientPayloadError:
                self._logger.exception(
                    "Connection dropped randomly on topic %s, trying again...", topic
                )

    @staticmethod
    async def write_all_topics_local_queues_async(
        base_url: str,
        sse_topics_metadata: dict[str, dict[str, str]],
        user_project_path: Path,
        queues_base_paths: dict[str, Path],
    ):
        topics_writer = TopicQueuesAsyncWriter(user_project_path)

        async with asyncio.TaskGroup() as topics_task_group:
            for topic, _ in queues_base_paths.items():
                topics_task_group.create_task(
                    topics_writer.write_topic_local_queue(
                        topic,
                        base_url,
                        sse_topics_metadata,
                        queues_base_paths,
                    )
                )

    @staticmethod
    def build_and_run_topic_queues_writer(
        base_url: str,
        sse_topics_metadata: dict[str, dict[str, str]],
        user_project_path: Path,
        queues_base_paths: dict[str, Path],
    ):
        try:
            asyncio.run(
                TopicQueuesAsyncWriter.write_all_topics_local_queues_async(
                    base_url, sse_topics_metadata, user_project_path, queues_base_paths
                )
            )
        except Exception as error:  # pylint: disable=broad-except
            logger = get_process_logger(user_project_path, __name__)
            logger.exception(error)
            exit()
