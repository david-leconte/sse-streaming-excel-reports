"""
Build script: compile the application into a standalone Windows executable.

Uses Nuitka to compile the src/ package into a single executable with
all dependencies bundled. The build is intended for Windows and includes
the Tkinter GUI, dbt project files, and the sample_project template.

Usage:
    python build.py

Output:
    target/sse-streaming-excel-reports.exe
"""

import subprocess
import sys


if __name__ == "__main__":
    build_args = [
        sys.executable,
        "-m",
        "nuitka",
        "--main=src",
        "--mode=standalone",
        "--output-dir=target",
        "--output-filename=sse-streaming-excel-reports.exe",
        "--nofollow-import-to=pytest",
        "--assume-yes-for-downloads",
        "--disable-console",
        "--enable-plugins=tk-inter",
        "--include-package=parsedatetime",
        "--include-data-dir=dbt=dbt",
        "--include-data-dir=sample_project=sample_project",
        "--include-data-files=app_config.toml=app_config.toml",
    ]

    subprocess.run(build_args, check=True)

