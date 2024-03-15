from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoTypeModule,
    MongoTypeInstance,
    MongoTypeInvolvedContract,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.GRPCClient.CCD_Types import *

from ccdefundamentals.cis import (
    CIS,
    StandardIdentifiers,
    mintEvent,
    burnEvent,
    transferEvent,
    updateOperatorEvent,
    tokenMetadataEvent,
    LoggedEvents,
    MongoTypeTokenAddress,
    MongoTypeLoggedEvent,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenForAddress,
)
from ccdefundamentals.enums import NET
from ccdefundamentals.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database
from ccdefundamentals.GRPCClient.CCD_Types import *
from rich import print
from env import *
from rich.console import Console
from rich.progress import track

console = Console()

tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", MONGODB_PASSWORD_LOCAL)
grpcclient = GRPCClient()
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)


def mongo_save_for_token_address(token_address_as_class: MongoTypeTokenAddress):
    repl_dict = token_address_as_class.dict()
    if "id" in repl_dict:
        del repl_dict["id"]

    sorted_holders = list(repl_dict["token_holders"].keys())
    sorted_holders.sort()
    token_holders_sorted = {i: repl_dict["token_holders"][i] for i in sorted_holders}
    token_holders_sorted = {k: v for k, v in token_holders_sorted.items() if int(v) > 0}
    repl_dict["token_holders"] = token_holders_sorted

    queue_item = ReplaceOne(
        {"_id": token_address_as_class.id},
        replacement=repl_dict,
        upsert=True,
    )
    return queue_item


def mongo_save_for_address(address_to_save: MongoTypeTokenHolderAddress):
    repl_dict = address_to_save.dict()
    if "id" in repl_dict:
        del repl_dict["id"]

    sorted_tokens = list(repl_dict["tokens"].keys())
    sorted_tokens.sort()
    tokens_sorted = {i: repl_dict["tokens"][i] for i in sorted_tokens}
    tokens_sorted = {
        k: v for k, v in tokens_sorted.items() if int(v["token_amount"]) > 0
    }
    repl_dict["tokens"] = tokens_sorted

    queue_item = ReplaceOne(
        {"_id": address_to_save.id},
        replacement=repl_dict,
        upsert=True,
    )
    return queue_item


def copy_token_holders_state_to_address_and_save(
    token_address_as_class: MongoTypeTokenAddress,
):
    _queue = []
    for address, token_amount in token_address_as_class.token_holders.items():
        address_to_save = MongoTypeTokenHolderAddress(
            **{
                "_id": address,
                "tokens": {},
            }
        )

        token_to_save = MongoTypeTokenForAddress(
            **{
                "token_address": token_address_as_class.id,
                "contract": token_address_as_class.contract,
                "token_id": token_address_as_class.token_id,
                "token_amount": str(token_amount),
            }
        )

        address_to_save.tokens[token_address_as_class.token_address] = token_to_save

        _queue.append(mongo_save_for_address(address_to_save))

    return _queue


def save_mint(token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent):
    result = mintEvent(**log.result)
    token_holders: dict[CCD_AccountAddress, str] = token_address_as_class.token_holders
    token_holders[result.to_address] = str(
        int(token_holders.get(result.to_address, "0")) + result.token_amount
    )
    token_address_as_class.token_amount = str(
        (int(token_address_as_class.token_amount) + result.token_amount)
    )
    token_address_as_class.token_holders = token_holders
    return token_address_as_class


def save_metadata(
    token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
):
    result = tokenMetadataEvent(**log.result)
    token_address_as_class.metadata_url = result.metadata.url
    return token_address_as_class


def save_operator(
    db_to_use: dict[Collections, Collection],
    instance_address: str,
    result: tokenMetadataEvent,
    height: int,
):
    token_address = f"{instance_address}-{result.token_id}"
    d = db_to_use[Collections.tokens_token_addresses].find_one({"_id": token_address})
    if not d:
        d = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": result.token_id,
                "last_height_processed": height,
            }
        )
    else:
        d = MongoTypeTokenAddress(**d)

    d.metadata_url = result.metadata.url

    # execute_save(db_to_use[Collections.tokens_token_addresses], d, token_address)


def save_transfer(
    token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent
):
    result = transferEvent(**log.result)
    try:
        token_holders: dict[CCD_AccountAddress, str] = (
            token_address_as_class.token_holders
        )
    except:
        console.log(
            f"{result.tag}: {token_address} | {token_address_as_class} has no field token_holders?"
        )
        Exception(
            console.log(
                f"{result.tag}: {token_address} | {token_address_as_class} has no field token_holders?"
            )
        )

    token_holders[result.to_address] = str(
        int(token_holders.get(result.to_address, "0")) + result.token_amount
    )
    try:
        token_holders[result.from_address] = str(
            int(token_holders.get(result.from_address, "0")) - result.token_amount
        )
        if int(token_holders[result.from_address]) == 0:
            del token_holders[result.from_address]

    except:
        if result.token_amount > 0:
            console.log(
                f"{result.tag}: {result.from_address} is not listed as token holder for {token_address}?"
            )

    token_address_as_class.token_holders = token_holders
    return token_address_as_class


def save_burn(token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEvent):
    result = burnEvent(**log.result)
    token_holders: dict[CCD_AccountAddress, str] = token_address_as_class.token_holders
    try:
        token_holders[result.from_address] = str(
            int(token_holders.get(result.from_address, "0")) - result.token_amount
        )
        if int(token_holders[result.from_address]) == 0:
            del token_holders[result.from_address]

        token_address_as_class.token_amount = str(
            (int(token_address_as_class.token_amount) - result.token_amount)
        )
        token_address_as_class.token_holders = token_holders

    except:
        console.log(
            f"{result.tag}: {result.from_address} is not listed as token holder for {token_address}?"
        )
        # exit

    token_address_as_class.token_holders = token_holders
    return token_address_as_class


def create_new_token_address(token_address: str):
    instance_address = token_address.split("-")[0]
    token_id = token_address.split("-")[1]
    token_address = MongoTypeTokenAddress(
        **{
            "_id": token_address,
            "contract": instance_address,
            "token_id": token_id,
            "token_amount": str(int(0)),  # mongo limitation on int size
            "token_holders": {},  # {CCD_AccountAddress, str(token_amount)}
            "last_height_processed": -1,
        }
    )
    return token_address


def execute_logged_event(
    token_address_as_class: MongoTypeTokenAddress, db_to_use, log: MongoTypeLoggedEvent
):
    if log.tag == 255:
        token_address_as_class = save_transfer(token_address_as_class, log)
    elif log.tag == 254:
        token_address_as_class = save_mint(token_address_as_class, log)
    elif log.tag == 253:
        token_address_as_class = save_burn(token_address_as_class, log)
    elif log.tag == 252:
        pass
        # we only save the logged event, but to not process this in
        # token_address or accounts.
        # save_operator(db_to_use, instance_address, result, height)
    elif log.tag == 251:
        token_address_as_class = save_metadata(token_address_as_class, log)

    return token_address_as_class


net = "testnet"
db_to_use: Database = mongodb.testnet if net == "testnet" else mongodb.mainnet

# start_processing_at_height = 2806849
token_accounting_last_processed_block = -1

result: list[MongoTypeLoggedEvent] = [
    MongoTypeLoggedEvent(**x)
    for x in db_to_use[Collections.tokens_logged_events]
    .find({"block_height": {"$gt": token_accounting_last_processed_block}})
    .sort(
        [("block_height", ASCENDING), ("tx_index", ASCENDING), ("ordering", ASCENDING)]
    )
]

token_accounting_last_processed_block_when_done = max([x.block_height for x in result])

events_by_token_address = {}
for log in result:
    events_by_token_address[log.token_address] = events_by_token_address.get(
        log.token_address, []
    )
    events_by_token_address[log.token_address].append(log)


console.log(
    f"Found {len(result):,.0f} logged events on {net} to process from {len(list(events_by_token_address.keys()))} token addresses."
)
for token_address in track(list(events_by_token_address.keys())):
    queue = []
    # if we start at the beginning of the chain for token accounting
    # create an empty token address as class to start
    if token_accounting_last_processed_block == -1:
        token_address_as_class = create_new_token_address(token_address)
    else:
        token_address_as_class = db_to_use[Collections.tokens_token_addresses].find_one(
            {"_id": token_address}
        )

    logs_for_token_address = events_by_token_address[token_address]
    for log in logs_for_token_address:
        token_address_as_class = execute_logged_event(
            token_address_as_class,
            db_to_use,
            log,
        )

    token_address_as_class.last_height_processed = log.block_height
    _ = db_to_use[Collections.tokens_token_addresses].bulk_write(
        [mongo_save_for_token_address(token_address_as_class)]
    )
    # all logs for token_address are processed,
    # now copy state from token holders to _accounts
    queue = copy_token_holders_state_to_address_and_save(token_address_as_class)
    if len(queue) > 0:
        try:
            _ = db_to_use[Collections.tokens_accounts].bulk_write(queue)
        except:
            console.log(token_address_as_class)
    else:
        pass
