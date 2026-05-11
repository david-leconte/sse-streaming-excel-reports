"""
Entry point for the SSE Streaming Excel Reports application.

This module provides the main() function that serves as the application entry point.
It determines whether to run in CLI mode (with a project path argument) or GUI mode
(interactive window for project selection and monitoring).

Run with:
    python -m src                    # GUI mode
    python -m src /path/to/project   # CLI mode
"""

from src.utils import get_user_project_dir_cli_input
from src.run import Run
from src.gui import AppWindow


def main():
    """Parse CLI arguments and launch in CLI or GUI mode."""
    user_project_dir_cli_arg = get_user_project_dir_cli_input()

    if user_project_dir_cli_arg:
        run = Run(user_project_dir_cli_arg)
        run.launch()

    else:
        app_window = AppWindow()
        app_window.mainloop()


if __name__ == "__main__":
    main()
