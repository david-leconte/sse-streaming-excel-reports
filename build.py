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
        "--output-filename=sse_streaming_excel_reports.exe",
        "--nofollow-import-to=unittest",
        "--nofollow-import-to=pytest",
        "--assume-yes-for-downloads",
        "--enable-plugins=tk-inter",
        "--include-data-dir=dbt=dbt",
        "--include-data-dir=sample_project=sample_project",
    ]

    subprocess.run(build_args, check=True)
