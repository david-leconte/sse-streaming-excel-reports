# SSE Streaming Excel Reports

A Python application that streams Server-Sent Events (SSE) from configured sources, stores them locally, transforms them using DuckDB and dbt, and outputs Excel-ready CSV data in near real-time.

## Overview

This application consumes SSE streams from a configurable API endpoint, writes events to local queue files, processes them through a medallion architecture (bronze → silver → gold layers) using dbt with DuckDB as the data warehouse, and outputs CSV files for Excel reporting.

## Architecture

```plaintext
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  SSE API        │────>│  Local Queue    │────>│  DuckDB (dbt)   │────>│  Human-readable │────>│  Excel reports  │
│  (e.g.,         │     │  Files (.bin)   │     │  Warehouse      │     │  CSV (.csv)     │     │  (.xlsx)        │
│  Wikimedia)     │     │                 │     │  (medallion)    │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Components:**

| Module                | Responsibility                                              |
|-----------------------|-------------------------------------------------------------|
| `src.__main__`        | Entry point: CLI mode or GUI mode                           |
| `src.gui`             | Tkinter-based GUI for configuration and monitoring          |
| `src.run`             | Orchestrates multiprocessing: spawns writer and transformer |
| `src.write_topics`    | Consumes SSE streams → local queue files                    |
| `src.write_warehouse` | Loads queue files → DuckDB with dbt → CSV                   |
| `src.utils`           | Shared utilities: logging, config loading, project setup    |

**Data pipeline layers:**

- **Bronze** — Raw SSE events ingested as JSON
- **Silver** — Parsed and normalized events
- **Gold** — Aggregated/report-ready tables (exported to CSV)

## Dependencies
  
**Python:** 3.13
**Packages:** aiofiles, aiohttp, dbt-core, dbt-duckdb, duckdb, sseclient-py
**Build (optional):** nuitka

Install with:

```bash
python -m pip install -r requirements.txt
```

## Configuration

**`app_config.toml`** (global app settings):

- `user_agent` — HTTP User-Agent header for SSE requests
- `topic_new_queue_file_seconds_threshold` — max age (seconds) before rotating queue file per topic
- `load_and_transform_every_seconds` — interval between ETL cycles
- `output_gold_csv_every_seconds` — interval between CSV exports

**`topics.toml`** (per-project, created by user or sample project):

- `api.base_url` — SSE provider base URL
- `topics` — mapping of topic names to SSE path and primary key(s)

Example (Wikimedia EventStreams):

```toml
[api]
base_url = "https://stream.wikimedia.org/"

[topics.recentchange]
path = "/v2/stream/recentchange"
primary_key = ["id"]
```

## Usage

**CLI mode** (run against an existing project folder):

```bash
python -m src /path/to/your/project
```

**GUI mode** (interactive):

```bash
python -m src
```

Use the GUI to select a project folder or create a new one from the sample Wikimedia EventStreams project.

## Build (Windows)

Create a standalone executable with Nuitka:

```bash
python build.py
```

The executable is produced at `target/src.dist/sse-streaming-excel-reports.exe`.

## Project Structure

```plaintext
├── src/
│   ├── __init__.py
│   ├── __main__.py          # Entry point
│   ├── gui.py               # Tkinter GUI
│   ├── run.py               # Multiprocessing orchestrator
│   ├── utils.py             # Shared utilities
│   ├── write_topics.py      # SSE ingestion
│   └── write_warehouse.py   # dbt transformation
├── dbt/                     # dbt bound configurations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── macros/
├── sample_project/          # Wikimedia EventStreams sample project
│   ├── topics.toml
│   └── models/
├── tests/                   # Pytest suite
├── app_config.toml
├── requirements.txt
├── build.py
└── README.md
```

## Sample Project

The included `sample_project/` demonstrates using Wikimedia's EventStreams API. Topics produce streamed edits and changes from Wikipedia. dbt models transform raw JSON into gold-layer aggregated tables (e.g., per-domain activity, bot vs human actions...).

## TODO

- Integration of better gold layer statistics
- Actual Excel report to support
