from src.utils import get_user_project_dir_cli_input
from src.run import Run
from src.gui import AppWindow

def main():
    user_project_dir_cli_arg = get_user_project_dir_cli_input()

    if user_project_dir_cli_arg:
        run = Run(user_project_dir_cli_arg)
        run.launch()

    else:
        app_window = AppWindow()
        app_window.mainloop()

main()
