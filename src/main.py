from multiprocessing import Process
from pathlib import Path

from src.utils import (
    run_config,
    topics_config,
    propagate_dbt_files_to_user_path,
    create_local_files_dirs,
    create_local_queues_paths,
)
from src.write_topics import TopicQueuesAsyncWriter
from src.write_warehouse import WarehouseTransformer

if __name__ == "__main__":
    sse_api_base_url: str = topics_config["api"]["base_url"]
    sse_topics_metadata: dict[str, dict[str, str]] = topics_config["topics"]

    user_project_path = Path(run_config["default_user_project_dir"])

    propagate_dbt_files_to_user_path(user_project_path)
    data_path = create_local_files_dirs(user_project_path)
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
        exit()
