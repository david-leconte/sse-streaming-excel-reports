import logging
from multiprocessing import Process

from src.utils import (
    get_user_config_and_project_path,
    propagate_dbt_files_to_user_path,
    create_local_files_dirs,
    create_local_queues_paths,
)
from src.write_topics import TopicQueuesAsyncWriter
from src.write_warehouse import WarehouseTransformer

if __name__ == "__main__":
    default_logger = logging.getLogger(__name__)
    default_logger.setLevel(logging.ERROR)

    try:
        sse_api_base_url, sse_topics_metadata, user_project_path = (
            get_user_config_and_project_path()
        )
    except (ValueError, FileNotFoundError, KeyError, RuntimeError):
        default_logger.exception(
            "Error getting user config and project path. Exiting...",
        )
        exit()

    try:
        propagate_dbt_files_to_user_path(user_project_path)
    except OSError:
        default_logger.exception(
            "Error propagating dbt files to user path. Exiting...",
        )
        exit()

    try:
        data_path = create_local_files_dirs(user_project_path)
    except OSError:
        default_logger.exception(
            "Error creating local files directories. Exiting...",
        )
        exit()

    all_queues_basepaths = create_local_queues_paths(
        data_path / "queues", sse_topics_metadata.keys()
    )

    topics_writer_process = Process(
        target=TopicQueuesAsyncWriter.build_and_run_topic_queues_writer,
        args=(
            sse_api_base_url,
            sse_topics_metadata,
            user_project_path,
            all_queues_basepaths,
        ),
        daemon=True,
    )

    warehouse_transformer_process = Process(
        target=WarehouseTransformer.build_and_run_continuously_warehouse_transformer,
        args=(sse_topics_metadata, user_project_path, all_queues_basepaths),
        daemon=True,
    )

    topics_writer_process.start()
    warehouse_transformer_process.start()

    try:
        while (
            topics_writer_process.is_alive()
            and warehouse_transformer_process.is_alive()
        ):
            pass
    except KeyboardInterrupt:
        default_logger.setLevel(logging.INFO)
        default_logger.info("Keyboard interrupt received. Terminating processes...")

        topics_writer_process.terminate()
        warehouse_transformer_process.terminate()
        
        exit()
