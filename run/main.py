from multiprocessing import Process

from run.utils import config, create_local_layers_dirs, create_local_queues_paths
from run.write_topics import TopicQueuesAsyncWriter
from run.write_warehouse import WarehouseTransformer

if __name__ == "__main__":
    sse_api_base_url: str = config["sse_api"]["base_url"]
    sse_topics_metadata: dict[str, dict[str, str]] = config["sse_api"]["topics"]

    data_path = create_local_layers_dirs(config["app"]["base_dir"])
    all_queues_basepaths = create_local_queues_paths(
        data_path / "queues", sse_topics_metadata.keys()
    )

    topics_writer_process = Process(
        target=TopicQueuesAsyncWriter.build_and_run_topic_queues_writer,
        args=(
            sse_api_base_url,
            sse_topics_metadata,
            all_queues_basepaths,
        ),
        daemon=True,
    )

    warehouse_transformer_process = Process(
        target=WarehouseTransformer.build_and_run_continuously_warehouse_transformer,
        args=(
            sse_topics_metadata,
            all_queues_basepaths,
            data_path / "warehouse.duckdb",
        ),
        daemon=True,
    )

    topics_writer_process.start()
    warehouse_transformer_process.start()

    while topics_writer_process.is_alive() and warehouse_transformer_process.is_alive():
        pass
