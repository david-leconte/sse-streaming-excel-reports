"""
Tests for the WarehouseTransformer class.

This module tests the WarehouseTransformer class responsible for loading binary
queue files, parsing SSE events, inserting them into DuckDB bronze tables, and
running dbt transformations to produce gold layer exports as CSV.
"""

import pytest
from unittest.mock import patch, MagicMock, mock_open, PropertyMock
from pathlib import Path
from src.write_warehouse import WarehouseTransformer


class TestWriteWarehouse:
    """Test suite for WarehouseTransformer functionality."""

    @patch("duckdb.connect")
    def test_connects_to_duckdb(self, mock_duckdb_connect, temp_project_path):
        """
        Test that WarehouseTransformer establishes a DuckDB connection on init.

        The constructor should call duckdb.connect() and execute at least
        one SQL statement (likely schema creation).
        """
        WarehouseTransformer({}, temp_project_path, {})

        mock_duckdb_connect.assert_called_once()
        assert mock_duckdb_connect.return_value.sql.call_count >= 1

    @patch("dbt.cli.main.dbtRunner.invoke")
    @patch("duckdb.connect")
    @patch("builtins.open", new_callable=mock_open, read_data="1.bin")
    @patch("pathlib.Path.exists", return_value=True)
    def test_filters_files_by_timestamp_after_newest_loaded(
        self,
        _mock_path_exists,
        mock_file_open,
        _mock_duckdb_connect,
        mock_dbt_invoke,
        temp_project_path,
        sample_one_sse_topic,
        dbt_result,
    ):
        """
        Test that only files newer than the last loaded file are processed.

        The transformer reads the newest_file_loaded_log.txt to determine
        the last processed file timestamp and filters the queue files
        accordingly using Path.glob().
        """
        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"],
            temp_project_path,
            sample_one_sse_topic["basepaths"],
        )

        mock_dbt_invoke.return_value = dbt_result

        with patch.object(
            Path,
            "glob",
            return_value=[
                sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                / "0.bin",
                sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                / "1.bin",
            ],
        ):
            transformer.load_and_transform_once()

        newest_file_loaded_log_path = (
            temp_project_path
            / f"data/queues/{sample_one_sse_topic["name"]}_newest_file_loaded_log.txt"
        )

        mock_file_open.assert_any_call(
            str(newest_file_loaded_log_path),
            "r",
            encoding="utf-8",
        )

    @patch("dbt.cli.main.dbtRunner.invoke")
    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer,
        "_check_topics_bronze_table_exist",
        return_value={"t1": True},
    )
    @patch("builtins.open", mock_open())
    @patch("sseclient.SSEClient")
    def test_skips_events_with_missing_primary_key(
        self,
        mock_sseclient,
        _mock_open,
        mock_duckdb_connect,
        mock_dbt_invoke,
        temp_project_path,
        sample_one_sse_topic,
        dbt_result,
    ):
        """
        Test that events missing required primary key fields are skipped.

        When an SSE event JSON does not contain all primary key fields
        defined in the topic config, the event is not inserted into
        the bronze table and an empty JSON array is inserted instead.
        """
        duckdb_conn = mock_duckdb_connect.return_value

        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"],
            temp_project_path,
            sample_one_sse_topic["basepaths"],
        )

        mock_dbt_invoke.return_value = dbt_result

        with patch.object(
            WarehouseTransformer,
            "_topics_new_files_lists",
            new_callable=PropertyMock,
            return_value={
                sample_one_sse_topic["name"]: [
                    sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                    / "0.bin"
                ],
            },
        ):
            mock_event = MagicMock()
            mock_event.data = '{"other_key": "value"}'
            mock_sseclient.return_value.events.return_value = iter([mock_event])

            transformer.load_and_transform_once()

        duckdb_conn.execute.assert_called_once_with(
            "INSERT INTO bronze.t1_raw SELECT UNNEST(CAST(? AS JSON[])) AS event;",
            ["[]"],
        )

    @patch("dbt.cli.main.dbtRunner.invoke")
    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer,
        "_check_topics_bronze_table_exist",
        return_value={"t1": True},
    )
    @patch("builtins.open", mock_open())
    @patch("sseclient.SSEClient")
    def test_skips_invalid_json_events(
        self,
        mock_sseclient,
        _mock_open,
        mock_duckdb_connect,
        mock_dbt_invoke,
        temp_project_path,
        sample_one_sse_topic,
        dbt_result,
    ):
        """
        Test that malformed JSON events are skipped.

        SSE events with invalid JSON data (e.g., parse errors) are not
        inserted into the bronze table; instead an empty array is inserted.
        """
        duckdb_conn = mock_duckdb_connect.return_value

        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"],
            temp_project_path,
            sample_one_sse_topic["basepaths"],
        )

        mock_dbt_invoke.return_value = dbt_result

        with patch.object(
            type(transformer),
            "_topics_new_files_lists",
            new_callable=PropertyMock,
            return_value={
                sample_one_sse_topic["name"]: [
                    sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                    / "0.bin"
                ]
            },
        ), patch.object(
            WarehouseTransformer,
            "load_and_transform_once",
            wraps=transformer.load_and_transform_once,
        ):
            mock_event = MagicMock()
            mock_event.data = "{invalid_json"
            mock_sseclient.return_value.events.return_value = iter([mock_event])

            transformer.load_and_transform_once()

        duckdb_conn.execute.assert_called_once_with(
            "INSERT INTO bronze.t1_raw SELECT UNNEST(CAST(? AS JSON[])) AS event;",
            ["[]"],
        )

    @patch("dbt.cli.main.dbtRunner.invoke")
    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer,
        "_check_topics_bronze_table_exist",
        return_value={"t1": True},
    )
    @patch("builtins.open", mock_open())
    @patch("sseclient.SSEClient")
    def test_handles_utf8_decode_errors(
        self,
        mock_sseclient,
        _mock_open,
        mock_duckdb_connect,
        mock_dbt_invoke,
        temp_project_path,
        sample_one_sse_topic,
        dbt_result,
    ):
        """
        Test that UTF-8 decode errors during event reading are handled gracefully.

        If a UnicodeDecodeError occurs while reading SSE event data, the
        transformer should continue processing remaining events.
        """
        duckdb_conn = mock_duckdb_connect.return_value

        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"],
            temp_project_path,
            sample_one_sse_topic["basepaths"],
        )

        mock_dbt_invoke.return_value = dbt_result

        def mock_events_generator():
            yield MagicMock(data='{"id": 1}')
            raise UnicodeDecodeError("utf-8", b"", 1, 2, "mock error")

        with patch.object(
            WarehouseTransformer,
            "_topics_new_files_lists",
            new_callable=PropertyMock,
            return_value={
                sample_one_sse_topic["name"]: [
                    sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                    / "0.bin"
                ]
            },
        ):
            mock_sseclient.return_value.events.return_value = mock_events_generator()
            transformer.load_and_transform_once()

        assert duckdb_conn.execute.call_count == 1

    @patch("dbt.cli.main.dbtRunner.invoke")
    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer,
        "_check_topics_bronze_table_exist",
        return_value={"t1": True},
    )
    @patch("builtins.open", mock_open())
    @patch("sseclient.SSEClient")
    def test_inserts_valid_events_into_bronze_table(
        self,
        mock_sseclient,
        _mock_open,
        mock_duckdb_connect,
        mock_dbt_invoke,
        temp_project_path,
        sample_one_sse_topic,
        dbt_result,
    ):
        """
        Test that valid SSE events are correctly inserted into the bronze table.

        Events with valid JSON containing all primary key fields should be
        inserted as a JSON array into the bronze.t1_raw table.
        """
        duckdb_conn = mock_duckdb_connect.return_value

        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"],
            temp_project_path,
            sample_one_sse_topic["basepaths"],
        )

        mock_dbt_invoke.return_value = dbt_result

        with patch.object(
            WarehouseTransformer,
            "_topics_new_files_lists",
            new_callable=PropertyMock,
            return_value={
                sample_one_sse_topic["name"]: [
                    sample_one_sse_topic["basepaths"][sample_one_sse_topic["name"]]
                    / "0.bin"
                ]
            },
        ):
            mock_event = MagicMock()
            mock_event.data = '{"id": "123", "val": 1}'
            mock_sseclient.return_value.events.return_value = iter([mock_event])

            transformer.load_and_transform_once()

        duckdb_conn.execute.assert_called_once_with(
            f"INSERT INTO bronze.{sample_one_sse_topic["name"]}_raw SELECT UNNEST(CAST(? AS JSON[])) AS event;",
            ['[{"id": "123", "val": 1}]'],
        )

    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer, "_check_topics_bronze_table_exist", return_value={}
    )
    @patch.object(WarehouseTransformer, "_load_topic", return_value=1)
    @patch("dbt.cli.main.dbtRunner.invoke")
    def test_raises_runtime_error_when_dbt_not_success(
        self,
        mock_dbt_invoke,
        _mock_load_topic,
        _mock_check_bronze_tables_exist,
        _mock_duckdb_connect,
        temp_project_path,
        sample_one_sse_topic,
    ):
        """
        Test that a RuntimeError is raised when dbt run fails.

        If dbtRunner indicates success=False without an exception, a
        RuntimeError with a descriptive message should be raised.
        """
        transformer = WarehouseTransformer(
            sample_one_sse_topic["metadata"], temp_project_path, sample_one_sse_topic
        )

        mock_result = MagicMock()
        mock_result.exception = None
        mock_result.success = False
        mock_result.result = None
        mock_dbt_invoke.return_value = mock_result

        with pytest.raises(
            RuntimeError, match="dbt run failed without exception nor detailed result"
        ):
            transformer.load_and_transform_once()

    @patch("duckdb.connect")
    @patch.object(
        WarehouseTransformer, "_check_topics_bronze_table_exist", return_value={}
    )
    def test_exports_each_table_to_csv(
        self, _mock_check_bronze_tables_exist, mock_duckdb_connect, temp_project_path
    ):
        """
        Test that output_gold_layer_csv exports all gold tables to CSV.

        After dbt completes, each gold layer table should be exported
        using DuckDB's COPY statement to the output directory.
        """
        duckdb_conn = mock_duckdb_connect.return_value

        transformer = WarehouseTransformer({}, temp_project_path, {})

        duckdb_conn.sql.return_value.fetchall.return_value = [
            ("gold_table_1",),
            ("gold_table_2",),
        ]

        transformer.output_gold_layer_csv()

        sql_calls = [call.args[0] for call in duckdb_conn.sql.call_args_list]
        assert any("COPY warehouse.gold.gold_table_1 TO" in call for call in sql_calls)
        assert any("COPY warehouse.gold.gold_table_2 TO" in call for call in sql_calls)
