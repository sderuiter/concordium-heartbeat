# ruff: noqa: F403, F405, E402, E501, E722
import asyncio
from heartbeat import Heartbeat
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.GRPCClient.CCD_Types import *
from sharingiscaring.tooter import Tooter
from sharingiscaring.mongodb import (
    MongoDB,
    MongoMotor,
)
from env import *
from rich.console import Console
import urllib3

urllib3.disable_warnings()

console = Console()

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)

motormongo = MongoMotor(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)


def main():
    """
    The Hearbeat repo is an endless async loop of three methods:
    1. `get_finalized_blocks`: this method looks up the last processed block
    in a mongoDB helper collection, and determines how many finalized
    blocks it needs to request from the node. These blocks are then added
    to the queue `finalized_block_infos_to_process`.
    2. `process_blocks` picks up this queue of blocks to process, and
    continues processing until the queue is empty again. For every block,
    we store the block_info (including tx hashes) into the collection `blocks`.
    Furthermore, we inspect all transactions for a block to determine whether we need
    to create any indices for them.
    3. `send_to_mongo`: this method takes all queues and sends them to the respective
    MongoDB collections.
    """
    console.log(f"{RUN_ON_NET=}")
    grpcclient = GRPCClient()

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)

    # these to helper methods are not needed, only things
    # go really wrong...

    # heartbeat.create_mongodb_indices()
    
    # heartbeat.create_block_per_day()

    loop = asyncio.get_event_loop()

    loop.create_task(heartbeat.get_finalized_blocks())
    loop.create_task(heartbeat.process_blocks())
    loop.create_task(heartbeat.send_to_mongo())

    loop.create_task(heartbeat.update_token_accounting())
    loop.create_task(heartbeat.pre_main_tokens_page())
    loop.create_task(heartbeat.pre_addresses_by_contract_count())

    loop.create_task(heartbeat.get_special_purpose_blocks())
    loop.create_task(heartbeat.process_special_purpose_blocks())

    loop.create_task(heartbeat.get_redo_token_addresses())
    loop.create_task(heartbeat.special_purpose_token_accounting())

    loop.create_task(heartbeat.update_nodes_from_dashboard())
    loop.create_task(heartbeat.update_impacted_addresses_all_top_list())

    loop.create_task(heartbeat.update_exchange_rates_for_tokens())
    loop.create_task(heartbeat.update_exchange_rates_historical_for_tokens())

    loop.create_task(heartbeat.update_memos_to_hashes())

    # loop.create_task(heartbeat.web23_domain_name_metadata())
    # loop.create_task(heartbeat.read_token_metadata_if_not_present())

    loop.run_forever()


if __name__ == "__main__":
    try:
        main()
    except Exception as f:
        console.log("main error: ", f)
