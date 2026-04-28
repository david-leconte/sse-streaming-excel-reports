"""
TopicQueuesAsyncWriter: SSE stream ingestion → local queue files.

This module defines TopicQueuesAsyncWriter, an asynchronous writer that consumes
Server-Sent Events streams from a configured API endpoint and persists them
into timestamped .bin files on disk. One writer task runs per topic.

The .bin files are later consumed by WarehouseTransformer to load data into
the DuckDB warehouse.

Multiprocessing:
    This module spawns its own asyncio event loop inside a multiprocessing.Process
    via the build_and_run_topic_queues_writer() static factory method.

Dependencies:
    aiohttp, aiofiles, sseclient-py
"""

from typing import Tuple
from pathlib import Path
from datetime import datetime
import asyncio
from asyncio import IncompleteReadError
from multiprocessing.queues import Queue

from aiofiles.threadpool.binary import AsyncBufferedIOBase
import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientPayloadError

from src.utils import app_config, get_process_logger


class TopicQueuesAsyncWriter:
    """
    Asynchronous SSE stream consumer that writes events to local queue files.

    For a given topic, this class opens an HTTP connection to the SSE endpoint
    and accumulates SSE event data into .bin files. A new file is started when:
        - No file is currently open, OR
        - The elapsed time since the current file was created exceeds
          app_config["topic_new_queue_file_seconds_threshold"]

    All .bin files are written to <project>/data/queues/{topic}/.

    Attributes:
        _logger: Process logger (to user_project_path/logs/ and/or GUI queue).
    """

    def __init__(
        self, user_project_path: Path, gui_global_log_queue: Queue | None = None
    ):
        """
        Create a new TopicQueuesAsyncWriter.

        Args:
            user_project_path: Root of the user project for log file location.
            gui_global_log_queue: Optional queue to forward log records to GUI.
        """
        self._logger = get_process_logger(
            __name__, user_project_path, gui_global_log_queue
        )

    @staticmethod
    async def _get_new_topic_queue_file(
        topic: str, local_queues_basepaths: dict[str, Path]
    ) -> Tuple[datetime, AsyncBufferedIOBase]:
        """
        Create and open a new timestamped .bin file for a topic.

        Args:
            topic: Topic name (used in filename).
            local_queues_basepaths: Mapping of topic → directory path.

        Returns:
            Tuple of (file creation datetime, open AsyncBufferedIOBase handle).
        """
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
        """
        Continuously consume one SSE topic and write events to disk.

        This is the per-topic coroutine. It loops forever:
            1. Open an aiohttp client session to base_url + topic path
            2. Read SSE events from the response stream (separated by b"\\n\\n")
            3. Write raw SSE event bytes into the current .bin queue file
            4. Rotate to a new file after the configured time threshold
            5. Reconnect on ClientPayloadError (connection drop)

        Args:
            topic: Topic key as found in sse_topics_metadata.
            base_url: SSE API base URL (e.g. "https://stream.wikimedia.org/").
            sse_topics_metadata: Topics configuration dictionary (unused locally
                but required for signature compatibility with build_and_run...).
            queues_base_paths: Mapping of topic name → local queue directory.

        Note:
            This coroutine never returns under normal operation. It handles
            ClientPayloadError by logging and restarting the connection.
        """
        last_queue_file_datetime: datetime | None = None
        last_queue_file: AsyncBufferedIOBase | None = None

        timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

        while True:
            try:
                async with (
                    aiohttp.ClientSession(timeout=timeout) as session,
                    session.get(
                        base_url + "/" + sse_topics_metadata[topic]["path"],
                        headers={"User-Agent": app_config["user_agent"]},
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
                        ).seconds > app_config[
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
        gui_global_log_queue: Queue | None = None,
    ):
        """
        Launch one writer coroutine per topic concurrently using asyncio.TaskGroup.

        This method creates a TopicQueuesAsyncWriter instance and spawns a
        task for every configured topic. All tasks run concurrently; the
        method returns only when all topics terminate (which they never do
        unless an error occurs).

        Args:
            base_url: SSE API base URL.
            sse_topics_metadata: Topics configuration dictionary.
            user_project_path: Root path for log files.
            queues_base_paths: Per-topic queue directory mapping.
            gui_global_log_queue: Optional queue for GUI log forwarding.
        """
        topics_writer = TopicQueuesAsyncWriter(user_project_path, gui_global_log_queue)

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
        gui_global_log_queue: Queue | None = None,
    ):
        """
        Multiprocessing entry point: create writer and run its asyncio loop.

        Designed to be passed as the target of a multiprocessing.Process.
        Catches all exceptions, logs them, and exits with status 1 on failure.

        Args:
            base_url: SSE API base URL.
            sse_topics_metadata: Topics configuration dictionary.
            user_project_path: User project root path.
            queues_base_paths: Per-topic queue directory mapping.
            gui_global_log_queue: Optional queue for GUI log forwarding.
        """
        try:
            asyncio.run(
                TopicQueuesAsyncWriter.write_all_topics_local_queues_async(
                    base_url,
                    sse_topics_metadata,
                    user_project_path,
                    queues_base_paths,
                    gui_global_log_queue,
                )
            )
        except Exception:  # pylint: disable=broad-except
            logger = get_process_logger(
                __name__, user_project_path, gui_global_log_queue
            )
            logger.exception("Error in topic queues writer, shutting down process...")
            exit()
