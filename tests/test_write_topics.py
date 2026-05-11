"""
Tests for the TopicQueuesAsyncWriter class.

This module tests the asynchronous writer that consumes SSE events from multiple
topics and writes them to binary queue files. Tests cover file creation,
event streaming, time-based file rotation, error handling, and concurrent
topic processing.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime
from aiohttp.client_exceptions import ClientPayloadError
from asyncio import IncompleteReadError
from src.write_topics import TopicQueuesAsyncWriter


class TestWriteTopics:
    """Test suite for TopicQueuesAsyncWriter functionality."""

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    @patch("aiofiles.open", new_callable=AsyncMock)
    async def test_creates_new_binary_file(
        self,
        mock_aiofiles_open,
        mock_session_get,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
         Test that write_topic_local_queue creates a new binary queue file.

         When processing a topic, a new file should be created with a timestamp
        -based name (Unix timestamp with decimal precision) and opened in
         binary write mode.
        """
        mock_file = AsyncMock()
        mock_aiofiles_open.return_value = mock_file
        mock_resp = AsyncMock()
        mock_resp.content.readuntil.side_effect = [
            b"event_data\n\n",
            Exception("Stop reading new events"),
        ]
        mock_session_get.return_value.__aenter__.return_value = mock_resp

        writer = TopicQueuesAsyncWriter(temp_project_path)

        with pytest.raises(Exception, match="Stop reading new events"):
            await writer.write_topic_local_queue(
                sample_one_sse_topic["name"],
                sample_api_base_url,
                sample_one_sse_topic["metadata"],
                sample_one_sse_topic["basepaths"],
            )

        mock_aiofiles_open.assert_called_once()
        assert mock_aiofiles_open.call_args.args[0].name.endswith(".bin")
        assert mock_aiofiles_open.call_args.args[1] == "wb"

    @pytest.mark.asyncio
    @patch("src.write_topics.TopicQueuesAsyncWriter._get_new_topic_queue_file")
    @patch("aiohttp.ClientSession.get")
    async def test_writes_events_to_queue_file(
        self,
        mock_session_get,
        mock_get_new_file,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
        Test that SSE events are correctly written to the queue file.

        Each SSE event data chunk should be written to the binary file.
        The test simulates multiple readuntil calls followed by an
        IncompleteReadError to signal stream termination.
        """
        mock_file = AsyncMock()
        mock_get_new_file.return_value = (datetime.now(), mock_file)
        mock_resp = AsyncMock()
        mock_resp.content.readuntil.side_effect = [
            b"event_data\n\n",
            Exception("Stop reading new events"),
        ]
        mock_session_get.return_value.__aenter__.return_value = mock_resp

        writer = TopicQueuesAsyncWriter(temp_project_path)

        with pytest.raises(Exception, match="Stop reading new events"):
            await writer.write_topic_local_queue(
                sample_one_sse_topic["name"],
                sample_api_base_url,
                sample_one_sse_topic["metadata"],
                sample_one_sse_topic["basepaths"],
            )

        mock_file.write.assert_called_once_with(b"event_data\n\n")

    @pytest.mark.asyncio
    @patch("src.write_topics.TopicQueuesAsyncWriter._get_new_topic_queue_file")
    @patch("aiohttp.ClientSession.get")
    @patch(
        "src.write_topics.app_config",
        {"topic_new_queue_file_seconds_threshold": -1, "user_agent": "test"},
    )
    async def test_creates_new_file_after_time_threshold(
        self,
        mock_session_get,
        mock_get_new_file,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
        Test that a new queue file is created when the time threshold is exceeded.

        With a negative threshold (-1 seconds), each event triggers a new file.
        The previous file should be closed when switching to a new one.
        """
        mock_file1 = AsyncMock()
        mock_file2 = AsyncMock()
        mock_file3 = AsyncMock()

        mock_get_new_file.side_effect = [
            (datetime.now(), mock_file1),
            (datetime.now(), mock_file2),
            (datetime.now(), mock_file3),
        ]

        mock_resp = AsyncMock()
        mock_resp.content.readuntil.side_effect = [
            b"data1\n\n",
            b"data2\n\n",
            Exception("Stop reading new events"),
        ]
        mock_session_get.return_value.__aenter__.return_value = mock_resp

        writer = TopicQueuesAsyncWriter(temp_project_path)

        with pytest.raises(Exception, match="Stop reading new events"):
            await writer.write_topic_local_queue(
                sample_one_sse_topic["name"],
                sample_api_base_url,
                sample_one_sse_topic["metadata"],
                sample_one_sse_topic["basepaths"],
            )

        assert mock_get_new_file.call_count == 3
        mock_file1.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    async def test_handles_incomplete_read_error(
        self,
        mock_session_get,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
        Test that IncompleteReadError during streaming is properly handled.

        When the SSE stream ends with an IncompleteReadError, the writer
        should exit gracefully.
        """
        mock_resp = AsyncMock()
        mock_resp.content.readuntil.side_effect = IncompleteReadError(b"partial", None)
        mock_session_get.side_effect = [
            mock_session_get.return_value,
            Exception("Stop reading new events"),
        ]
        mock_session_get.return_value.__aenter__.return_value = mock_resp

        writer = TopicQueuesAsyncWriter(temp_project_path)

        with pytest.raises(Exception, match="Stop reading new events"):
            await writer.write_topic_local_queue(
                sample_one_sse_topic["name"],
                sample_api_base_url,
                sample_one_sse_topic["metadata"],
                sample_one_sse_topic["basepaths"],
            )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    async def test_handles_client_payload_error_with_retry(
        self,
        mock_session_get,
        temp_project_path,
        sample_api_base_url,
        sample_one_sse_topic,
    ):
        """
        Test that ClientPayloadError triggers a retry and eventually stops.

        The first attempt should raise ClientPayloadError, which should be
        retried once before the test stops with a stop exception.
        """
        mock_session_get.side_effect = [
            ClientPayloadError(),
            Exception("Stop retrying API server connection"),
        ]
        writer = TopicQueuesAsyncWriter(temp_project_path)
        with pytest.raises(Exception, match="Stop retrying API server connection"):
            await writer.write_topic_local_queue(
                sample_one_sse_topic["name"],
                sample_api_base_url,
                sample_one_sse_topic["metadata"],
                sample_one_sse_topic["basepaths"],
            )

    @pytest.mark.asyncio
    @patch("asyncio.TaskGroup")
    async def test_executes_multiple_topics_concurrently(
        self, mock_asyncio_tg_class, temp_project_path, sample_api_base_url
    ):
        """
        Test that write_all_topics_local_queues_async creates concurrent tasks.

        When multiple topics are configured, a separate async task should be
        created for each topic using an asyncio TaskGroup.
        """

        # When making the mock instance of asyncio.TaskGroup
        # part of an asynchronous context using AsyncMock,
        # all its methods become asynchronous as well,
        # but technically the "create_task" method by itself is synchronous,
        # thus awaiting create_task gives a warning
        # (i.e. the context must be awaited, not "create_task" inside it)
        mock_asyncio_tg_async_context = AsyncMock()
        mock_asyncio_tg_async_context.create_task = MagicMock()
        mock_asyncio_tg_class.return_value.__aenter__.return_value = (
            mock_asyncio_tg_async_context
        )

        metadata = {
            "t1": {"path": "/stream1", "primary_key": ["id"]},
            "t2": {"path": "/stream2", "primary_key": ["id"]},
        }
        base_paths = {
            "t1": temp_project_path / "data/queues/t1",
            "t2": temp_project_path / "data/queues/t2",
        }

        await TopicQueuesAsyncWriter.write_all_topics_local_queues_async(
            sample_api_base_url, metadata, temp_project_path, base_paths
        )

        # The "create_task" method called is the one from
        # the made up async context, not the original class one
        # which is now detached
        assert mock_asyncio_tg_async_context.create_task.call_count == 2
