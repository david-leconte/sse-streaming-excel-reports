"""
Tkinter-based graphical user interface for the SSE Streaming Excel Reports application.

This module defines the AppWindow class, a Tkinter window that allows users to:
    - Configure global application parameters (User-Agent, timing thresholds)
    - Select an existing project folder (containing topics.toml)
    - Create a new project from the bundled Wikimedia EventStreams sample
    - Start/stop the streaming pipeline
    - View live log output from the child processes

The GUI runs in the main thread while spawning the pipeline in separate
multiprocessing.Process workers.
"""

from pathlib import Path
from tkinter import Tk, Menu, Text, StringVar
from tkinter.ttk import Frame, LabelFrame, Label, Entry, Button
from tkinter import filedialog
from multiprocessing import Queue
import logging
from logging import LogRecord

from src.run import Run
from src.utils import app_config, get_process_logger, copy_sample_project_into_user_path


class AppWindow(Tk):
    """
    Main application window for the GUI mode.

    Layout:
        ┌────────────────────────────────────────────────────────┐
        │  File [Exit]                                            │
        │                                                         │
        │  App configuration                                      │
        │  ┌─────────────────────────────────────────────────┐   │
        │  │ User-Agent:          [Text field]               │   │
        │  │ New file after (s):  [Entry]                    │   │
        │  │ Load interval (s):   [Entry]                    │   │
        │  │ CSV output every (s):[Entry]                    │   │
        │  └─────────────────────────────────────────────────┘   │
        │                                                         │
        │  Project configuration                                  │
        │  ┌─────────────────────────────────────────────────┐   │
        │  │  No project folder selected                      │   │
        │  │  [Try sample project]                            │   │
        │  └─────────────────────────────────────────────────┘   │
        │                                                         │
        │  [Start processing events]                              │
        │                                                         │
        │  Events (log output)                                    │
        │  ┌─────────────────────────────────────────────────┐   │
        │  │  ... log lines ...                               │   │
        │  └─────────────────────────────────────────────────┘   │
        └────────────────────────────────────────────────────────┘

    Attributes:
        _gui_global_log_queue (Queue): Multiprocessing queue for log records.
        _gui_logger (logging.Logger): Logger for the GUI process.
        _tk_app_config (dict[str, StringVar]): Tkinter variable wrappers for
            each app_config key (user_agent, topic_new_queue_file_seconds_threshold,
            load_and_transform_every_seconds, output_gold_csv_every_seconds).
        _run (Run | None): The active Run instance (if a project is loaded).
        _start_stop_button_text (StringVar): Button label text.
        _log_output (Text): Text widget for displaying live logs.
        _is_project_opened_text (StringVar): Status line above the Try button.
    """

    def __init__(self):
        """Initialize the main window and all UI components."""
        super().__init__()
        self.title("SSE Streaming Excel Reports")

        self._gui_global_log_queue = Queue()
        self._gui_logger = get_process_logger(
            __name__, gui_global_log_queue=self._gui_global_log_queue
        )
        self._gui_logger.setLevel(logging.ERROR)
        self._watch_log_queue()

        self._tk_app_config: dict[str, StringVar] = self._store_tk_app_config()
        self._run: Run | None = None

        self._global_frame = Frame(self)
        self._global_frame.grid(padx=10, pady=10, sticky="nsew")

        self._create_menu_bar()
        self._create_app_config_form()
        self._create_user_config_form()
        self._start_stop_button_text = self._create_start_stop_button()
        self._log_output = self._create_log_output()

    def _store_tk_app_config(self) -> dict[str, StringVar]:
        """
        Wrap each app_config value into a Tkinter StringVar for two-way binding.

        Returns:
            Dictionary mapping config keys to StringVar instances initialized
            with current values from app_config.toml.
        """
        tk_app_config = {}

        for key, value in app_config.items():
            tk_app_config[key] = StringVar(value=value)

        return tk_app_config

    def _create_menu_bar(self):
        """Create the top menu bar with File → Open project folder and Exit."""
        menu_bar = Menu(self)
        self.config(menu=menu_bar)

        file_menu = Menu(menu_bar, tearoff=0)
        menu_bar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(
            label="Open project folder", command=self._open_project_folder
        )
        menu_bar.add_command(label="Exit")

    def _create_app_config_form(self):
        """Create the 'App configuration' labeled frame with four fields."""
        app_config_frame = LabelFrame(self._global_frame, text="App configuration")
        app_config_frame.grid(column=0, row=0, padx=10, pady=10)

        Label(app_config_frame, text="User-Agent:").grid(
            row=0,
            column=0,
            padx=(0, 8),
            pady=(0, 8),
        )
        user_agent = Text(
            app_config_frame,
            width=40,
            height=4,
        )
        user_agent.grid(
            row=0,
            column=1,
            padx=(0, 8),
        )
        user_agent.insert("1.0", self._tk_app_config["user_agent"].get())

        Label(app_config_frame, text="Create new streamed topic file after (s):").grid(
            row=1, column=0, padx=(0, 8), pady=(0, 8)
        )
        Entry(
            app_config_frame,
            textvariable=self._tk_app_config["topic_new_queue_file_seconds_threshold"],
        ).grid(row=1, column=1)

        Label(
            app_config_frame, text="Load and transform into warehouse every (s):"
        ).grid(row=2, column=0, padx=(0, 8), pady=(0, 8))
        Entry(
            app_config_frame,
            textvariable=self._tk_app_config["load_and_transform_every_seconds"],
        ).grid(row=2, column=1)

        Label(app_config_frame, text="Output gold layer CSV files every (s):").grid(
            row=3, column=0, padx=(0, 8), pady=(0, 8)
        )
        Entry(
            app_config_frame,
            textvariable=self._tk_app_config["output_gold_csv_every_seconds"],
        ).grid(row=3, column=1)

    def _create_user_config_form(self):
        """Create the 'Project configuration' labeled frame."""
        user_config_frame = LabelFrame(self._global_frame, text="Project configuration")
        user_config_frame.grid(
            column=0,
            row=1,
            padx=10,
            pady=10,
        )

        self._is_project_opened_text = StringVar(value="No project folder selected")
        Label(
            user_config_frame,
            textvariable=self._is_project_opened_text,
            anchor="center",
        ).grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="ew",
            padx=(8, 8),
            pady=(15, 15),
        )

        Label(
            user_config_frame,
            text="Try with the Wikimedia EventStreams\nsample project:",
            anchor="center",
        ).grid(
            row=1,
            column=0,
            padx=(8, 8),
            pady=(10, 10),
        )
        Button(user_config_frame, text="Try", command=self._try_sample_project).grid(
            row=1,
            column=1,
            padx=(8, 8),
            pady=(10, 10),
        )

    def _create_start_stop_button(self) -> StringVar:
        """Create the Start/Stop processing button and return its label StringVar."""
        start_stop_frame = Frame(self._global_frame)
        start_stop_frame.grid(column=0, row=2, padx=10, pady=10)

        start_stop_button_text = StringVar(value="Start processing events")
        Button(
            start_stop_frame,
            textvariable=start_stop_button_text,
            command=self._start_stop_button_action,
        ).grid(row=0, column=0, pady=(10, 0))

        return start_stop_button_text

    def _create_log_output(self) -> Text:
        """Create the scrollable text area for live log output and return it."""
        log_output_frame = LabelFrame(self._global_frame, text="Events")
        log_output_frame.grid(column=0, row=3, padx=10, pady=10)

        log_output_text = Text(log_output_frame, width=80, height=7)
        log_output_text.grid(row=0, column=0, padx=(8, 8), pady=(8, 8))

        return log_output_text

    def _open_project_folder(self):
        """Handle 'Open project folder' menu action: prompt and create Run instance."""
        user_selected_project_dir = filedialog.askdirectory(
            title="Select project folder (with a topics.toml file and a models directory)"
        )

        if not Path(user_selected_project_dir).exists:
            self._gui_logger.error(
                "Provided user project directory path does not exist: %s",
                user_selected_project_dir,
            )

        self._run = Run(
            user_selected_project_dir,
            is_gui_run=True,
            gui_global_log_queue=self._gui_global_log_queue,
        )

        folder_name = Path(user_selected_project_dir).name
        self._is_project_opened_text.set(
            f"topics.toml successfully loaded from {folder_name} folder.\nReady to start processing events."
        )

    def _try_sample_project(self):
        """Handle 'Try' button: copy sample project and create Run instance."""
        user_selected_parent_path = Path(
            filedialog.askdirectory(title="Select parent folder for the sample project")
        )

        if not user_selected_parent_path.exists():
            self._gui_logger.error(
                "Provided parent directory path does not exist: %s",
                user_selected_parent_path,
            )

        user_project_dir = copy_sample_project_into_user_path(user_selected_parent_path)
        self._run = Run(
            user_project_dir,
            is_gui_run=True,
            gui_global_log_queue=self._gui_global_log_queue,
        )

        self._is_project_opened_text.set(
            "topics.toml successfully loaded from sample project.\nReady to start processing events."
        )

    def _start_stop_button_action(self):
        """
        Handle Start/Stop button click.

        Starts the pipeline if no run is active; stops it if already running.
        Updates the button label accordingly.
        """
        if not self._run:
            self._gui_logger.error(
                "No project selected. Please select a project folder or try the sample project."
            )

        elif self._run.is_active:
            self._run.terminate()
            self._start_stop_button_text.set("Start processing events")
        else:
            self._run.launch()
            self._start_stop_button_text.set("Stop processing events")

    def _watch_log_queue(self):
        """
        Poll the multiprocessing log queue and append new records to the text widget.

        Scheduled via Tkinter's after() to run every 200 ms during the GUI lifetime.
        """
        while not self._gui_global_log_queue.empty():
            log_record: LogRecord = self._gui_global_log_queue.get()
            self._log_output.insert("end", f"{log_record.msg}\n")
            self._log_output.see("end")

        self.after(200, self._watch_log_queue)
