from typing import Tuple, Coroutine
from pathlib import Path
from datetime import datetime
from threading import Event

import asyncio
from asyncio import IncompleteReadError
from aiofiles.threadpool.binary import AsyncBufferedIOBase
import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientPayloadError

from run.utils import config


async def get_new_topic_queue_file(
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
    topic: str,
    base_url: str,
    sse_topics_metadata: dict[str, dict[str, str]],
    queues_base_paths: dict[str, Path],
    thread_stop_event: Event,
):
    last_queue_file_datetime: datetime | None = None
    last_queue_file: AsyncBufferedIOBase | None = None

    timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

    while not thread_stop_event.is_set():
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session, session.get(
                base_url + "/" + sse_topics_metadata[topic]["path"],
                headers={"User-Agent": config["app"]["user_agent"]},
            ) as resp:
                while not thread_stop_event.is_set():
                    try:
                        current_contents = await resp.content.readuntil(
                            separator=b"\n\n"
                        )
                    except IncompleteReadError:
                        print("ERROR: streaming request error, incomplete read")
                        break

                    if not last_queue_file or not last_queue_file_datetime:
                        last_queue_file_datetime, last_queue_file = (
                            await get_new_topic_queue_file(topic, queues_base_paths)
                        )

                    if (datetime.now() - last_queue_file_datetime).seconds > config[
                        "app"
                    ]["topic_new_queue_file_seconds_threshold"]:
                        await last_queue_file.close()
                        last_queue_file_datetime, last_queue_file = (
                            await get_new_topic_queue_file(topic, queues_base_paths)
                        )

                    await last_queue_file.write(current_contents)

        except ClientPayloadError:
            print(
                f"ERROR: Connection dropped randomly on topic {topic}, trying again..."
            )


async def write_all_topics_local_queues(
    base_url: str,
    sse_topics_metadata: dict[str, dict[str, str]],
    queues_base_paths: dict[str, Path],
    thread_stop_event: Event,
):
    queues_awaitables_list: list[Coroutine] = [
        write_topic_local_queue(
            topic, base_url, sse_topics_metadata, queues_base_paths, thread_stop_event
        )
        for topic in queues_base_paths.keys()
    ]

    await asyncio.gather(*queues_awaitables_list)
