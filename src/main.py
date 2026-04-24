from src.utils import app_config, get_user_project_dir_cli_input
from src.run import Run

if __name__ == "__main__":
    user_project_dir_cli_arg = get_user_project_dir_cli_input()

    if not user_project_dir_cli_arg:
        user_project_dir_cli_arg = app_config["default_user_project_dir"]

    run = Run(user_project_dir_cli_arg)
    run.launch()
