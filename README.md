# SSE Streaming Excel Reports

Live Excel reports coming from Server-sent events (SSE) sources

## How to build the executable (Windows)

`python -m nuitka --output-dir=build --output-filename="sse_streaming_excel_reports.exe" --enable-plugins=tk-inter --mode=standalone --include-data-dir=dbt=dbt --include-data-dir=sample_project=sample_project src/main.py`
