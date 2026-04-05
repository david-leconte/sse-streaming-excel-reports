from threading import Thread, Event
import asyncio

from dotenv import load_dotenv

from run.utils import config, create_local_layers_dirs, create_local_queues_paths
from run.write_topics import write_all_topics_local_queues
from run.write_warehouse import WarehouseTransformer

load_dotenv()

thread_stop_event = Event()

sse_api_base_url: str = config["sse_api"]["base_url"]
sse_topics_metadata: dict[str, dict[str, str]] = config["sse_api"]["topics"]

data_path = create_local_layers_dirs(config["app"]["base_dir"])
all_queues_basepaths = create_local_queues_paths(
    data_path / "queues", sse_topics_metadata.keys()
)

topics_writer_thread = Thread(
    target=asyncio.run,
    args=(
        write_all_topics_local_queues(
            sse_api_base_url,
            sse_topics_metadata,
            all_queues_basepaths,
            thread_stop_event,
        ),
    ),
    daemon=True,
)

warehouse_transformer = WarehouseTransformer(
    sse_topics_metadata, all_queues_basepaths, data_path / "warehouse.duckdb"
)

warehouse_transformer_thread = Thread(
    target=warehouse_transformer.load_and_transform_continuously,
    args=(thread_stop_event,),
    daemon=True,
)

topics_writer_thread.start()
warehouse_transformer_thread.start()

try:
    thread_stop_event.wait()
except KeyboardInterrupt:
    thread_stop_event.set()

